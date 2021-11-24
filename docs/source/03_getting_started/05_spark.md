# Spark integration

Kedro allows integration with pyspark as described [in kedro documentation](https://kedro.readthedocs.io/en/latest/11_tools_integration/01_pyspark.html|here).
kedro-airflow-k8s allows running such projects with Airflow, using the external Spark cluster as a
computation environment. Not every pipeline node is executed on spark cluster, but only
the ones which require spark environment.

## Project handling

In order to make this happen, the following applies. Firstly plugin detects if any of the kedro
nodes are pyspark related. All such nodes are logically grouped in a way that dependencies between
all nodes in the pipeline are maintained. Such construction keeps data management between
kedro nodes local within the cluster for performance matter, while enforcing order correctness.
Secondly, plugin creates spark submit nodes inside the DAG to reflect spark related tasks, grouped
in the previous step. Additionally, the cluster create and delete operator is setup so that
the dedicated spark instance is ready for the sake of given job run. As the last step, the artifacts required by spark,
namely cluster initialization shell script, project archive and kedro run python script are prepared.

## Configuration

`spark` configuration is a part of a `run_config`. This plugin supports Google Dataproc, but it's also possible
to provide custom operators via external factory.

### Google Dataproc

To configure Dataproc with the project, set `run_config.spark.type` as `dataproc`.
Use `cluster_config` to provide dictionary that describes the cluster as required
by [Airflow Dataproc operators](https://github.com/apache/airflow/blob/v2-1-stable/airflow/providers/google/cloud/operators/Dataproc.py).
Checking with [Google Dataproc REST API](https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig) is helpful.

### Kubernetes

It's possible to execute kedro spark jobs on K8S. In this case, there's no external cluster that's started for the 
purpose of the task. Instead, user has to provide image, containing both spark and kedro project. Usually such image
is created with `docker-image-tool.sh`, which is a part of spark distribution. Example image building process may
look like this:

```shell
cp -r $SPARK_HOME /tmp
cd $KEDRO_PROJECT_HOME
cp -r . /tmp/spark-3.1.2-bin-hadoop3.2
/tmp/spark-3.1.2-bin-hadoop3.2/bin/docker-image-tool.sh -t $COMMIT_SHA -p Dockerfile -r $REGISTRY_IMAGE -b java_image_tag=14-slim build
```

There's a manual work to be done with the `Dockerfile` first. One approach is to use one of the templates provided by
spark distribution and merge it with the `Dockerfile` of kedro project. It's important that image contain kedro project
with all of it's contents and it's installed as a package on a system level together with all the requirements. Any
supplementary jars required by the project can be included as well, unless should be fetched during the job execution
from the external location.

It's also required to provide runner script inside the image. This script is provided as an entry point to `spark-submit`. 
The script should accept `--env` kedro application argument and `--nodes` as a comma-separated list of kedro node 
names to be executed. It should initialize kedro session and run project with given arguments. 

>> `Dockerfile` should execute spark entrypoint on start. Script is provided as an argument to `spark-submit`

Example script template is provided inside the plugin sources in `src/kedro_airflow_k8s/templates/spark_run.py`

### Custom configuration

In order to provide one's own operators it's sufficient to mark `run_config.spark.type` as `custom`,
and provide `run_config.spark.operator_factory` with the name of the custom class that acts as the operator factory.
The class has to be available on the path when executing `kedro airflow-k8s` commands.

The easiest way to start is to derive from `kero_airflow_k8s.template_helper.SparkOperatorFactoryBase`.
The following methods have to be provided:
* create_cluster_operator - returns string with the create cluster operator
* delete_cluster_operator - returns string with the delete cluster operator
* submit_operator - returns string with the submit job operator
* imports_statement - returns string with the full import statement of all required items from the previous methods

### Custom initialization script

`run_config.spark.user_init_path` allows configuring the way the cluster is initialized. Plugin delivers
initialization script that's aligned with the project artifacts. The script can be prepended with custom
logic, to support the cases like custom package repository setup. `run_config.spark.user_post_init_path` additionally 
allows appending to initialization script part of the shell script.
Scripts can use environment variable `PROJECT_HOME` in order to refer to project location on the cluster.
It's required the paths to be relative to the project `src` path.

## Detection of spark nodes

As the part of the plugin's process is to detect spark based nodes, the following rules apply:
* if the node is tagged with `kedro-airflow-k8s:group:pyspark` it's considered as a spark node - this allows arbitrary user selection of node to be executed by spark
* if any of the node's input or output is of type `pyspark.sql.dataframe.DataFrame` it's considered as a spark node - detection happens based on the type hints
* if any of the node's input or output is present in the data catalog as one of the `SparkDataSet`, `SparkHiveDataSet`, `SparkJDBCDataSet` it's considered as a spark node 
* if none of the above applies, but logical group of spark nodes provide data as input to the node and the node provides the data as the input to the group it's considered as a spark node
* if none of the above applies, the node is considered as the `default` and it's put into DAG as usual 

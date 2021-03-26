# Mlflow support

If you use [MLflow](https://mlflow.org/) and [kedro-mlflow](https://kedro-mlflow.readthedocs.io/) for the Kedro 
pipeline runs monitoring, the plugin will automatically enable support for:

* starting the experiment when the pipeline starts,
* logging all the parameters, tags, metrics and artifacts under unified MLFlow run.

To make sure that the plugin discovery mechanism works, add `kedro-mlflow` as a dependencies to `src/requirements.in`
and run:

```console
$ pip-compile src/requirements.in > src/requirements.txt 
$ kedro install
$ kedro mlflow init
```

Then, adjust the kedro-mlflow configuration and point to the mlflow server by editing `conf/local/mlflow.yml` and adjusting `mlflow_tracking_uri` key. Then, build the image:

```console
$ kedro docker build
```

And re-push the image to the remote registry.

> If `kedro-mlflow` is not installed as dependency and configuration is not in place (missing `kedro mlflow init`), the 
> MLflow experiment will not be initialized and available for pipeline tasks in Apache Airflow DAG.

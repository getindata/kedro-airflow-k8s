# Configuration

Plugin maintains the configuration in the `conf/base/airflow-k8s.yaml` file.

```yaml
# Base url of the Apache Airflow, should include the schema (http/https)
host: https://airflow.example.com

# Directory from where Apache Airflow is reading DAGs definitions
output: gs://airflow-bucket-example-com

# Configuration used to run the pipeline
run_config:

    # Name of the image to run as the pipeline steps
    image: airflow-k8s-plugin-demo

    # Pull policy to be used for the steps. Use Always if you push the images
    # on the same tag, or Never if you use only local images
    image_pull_policy: IfNotPresent
    
    # Pod startup timeout in seconds - if timeout passes the pipeline fails, default to 600 
    startup_time: 600

    # Namespace for Airflow pods to be created
    namespace: airflow

    # Name of the Airflow experiment to be created
    experiment_name: Airflow K8S Plugin Demo

    # Name of the dag as it's presented in Airflow
    run_name: airflow-k8s-plugin-demo

    # Apache Airflow cron expression for scheduled runs
    cron_expression: "@daily"
        
    # Optional start date in format YYYYMMDD, if not provided `days_ago(2)` is used instead
    start_date: "20210721"

    # Optional pipeline description
    description: "Very Important Pipeline"

    # Comma separated list of image pull secret names
    image_pull_secrets: my-registry-credentials
   
    # Service account name to execute nodes with
    service_account_name: airflow

    # List of handlers executed after task failure 
    failure_handlers:
      # type of integration, currently only slack is available
      - type: slack
        # airflow connection id with following parameters:
        # host - webhook url
        # password - webhook password
        # login - username
        connection_id: slack
        # message template that will be send. It can contains following parameters that will be replaced:
        # task
        # dag
        # execution_time
        message_template: |
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {execution_time}
            *Log Url*: {url}
            
    # Optional volume specification
    volume:
        # Storage class - use null (or no value) to use the default storage
        # class deployed on the Kubernetes cluster
        storageclass: # default
        # The size of the volume that is created. Applicable for some storage
        # classes
        size: 1Gi
        # Access mode of the volume used to exchange data. ReadWriteMany is
        # preferred, but it is not supported on some environements (like GKE)
        # Default value: ReadWriteOnce
        #access_modes: [ReadWriteMany]
        # Flag indicating if the data-volume-init step (copying raw data to the
        # fresh volume) should be skipped
        skip_init: False
        # Allows to specify fsGroup executing pipelines within containers
        # Default: root user group (to avoid issues with volumes in GKE)
        owner: 0
        # If set to True, shared persistent volume will not be created at all and all other parameters under
        # `volume` are discarded
        disabled: False

    # List of optional secrets specification
    secrets:
            # deploy_type: (Optional - default: 'env`) The type of secret deploy in Kubernetes, either `env` or `volume`
        -   deploy_type: "env"
            # deploy_target: (Optional) The environment variable when `deploy_type` `env` or file path when `deploy_type` `volume` where expose secret. If `key` is not provided deploy target should be None.
            deploy_target: "SQL_CONN"
            # secret: Name of the secrets object in Kubernetes
            secret: "airflow-secrets"
            # key: (Optional) Key of the secret within the Kubernetes Secret if not provided in `deploy_type` `env` it will mount all secrets in object
            key: "sql_alchemy_conn"


    # Apache Airflow macros to be exposed for the parameters
    # List of macros can be found here:
    # https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    macro_params: [ds, prev_ds]

    # Apache Airflow variables to be exposed for the parameters
    variables_params: [env]
  
    # Optional resources specification
    resources:
        # Default configuration used by all nodes that do not declare the
        # resource configuration. It's optional. If node does not declare the resource
        # configuration, __default__ is assigned by default, otherwise cluster defaults
        # will be used.
        __default__:
            # Optional labels to be put into pod node selector
            node_selectors:
              #Labels are user provided key value pairs
              node_pool_label/k8s.io: example_value
            # Optional labels to apply on pods
            labels:
              running: airflow
            # Optional annotations to apply on pods
            annotations:
              iam.amazonaws.com/role: airflow
            # Optional list of kubernetes tolerations
            tolerations:
                - key: "group"
                  value: "data-processing"
                  effect: "NoExecute"
                - key: "group"
                  operator: "Equal",
                  value: "data-processing",
                  effect: "NoSchedule"
            requests:
                #Optional amount of cpu resources requested from k8s
                cpu: "1"
                Optional amount of memory resource requested from k8s
                memory: "1Gi"
            limits:
                #Optional amount of cpu resources limit on k8s
                cpu: "1"
                #Optional amount of memory resource limit on k8s
                memory: "1Gi"
        # Other arbitrary configurations to use, for example to indicate some exception resources
        huge_machines:
            node_selectors:
                big_node_pool: huge.10x
            requests:
                cpu: "16"
                memory: "128Gi"
            limits:
                cpu: "32"
                memory: "256Gi"
    # Optional external dependencies configuration
    external_dependencies:
        # Can just select dag as a whole 
        - dag_id: upstream-dag
        # or detailed
        - dag_id: another-upstream-dag
        # with specific task to wait on
          task_id: with-precise-task
        # Maximum time (minute) to wait for the external dag to finish before this
        # pipeline fails, the default is 1440 == 1 day  
          timeout: 2
        # Checks if the external dag exists before waiting for it to finish. If it
        # does not exists, fail this pipeline. By default is set to true. 
          check_existence: False
        # Time difference with the previous execution to look at (minutes),
        # the default is 0 meaning no difference
          execution_delta: 10

    # Optional authentication to MLflow API    
    authentication:
      # Strategy that generates the tokens, supported values are: 
      # - Null
      # - GoogleOAuth2 (generating OAuth2 tokens for service account provided by GOOGLE_APPLICATION_CREDENTIALS)
      # - Vars (credentials fetched from airflow Variable.get - specify variable keys,
      # matching MLflow authentication env variable names, in `params`,
      # e.g. ["MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"])
      type: GoogleOAuth2 
      #params: []

    # Optional custom kubermentes pod templates applied on nodes basis
    kubernetes_pod_templates:
    # Name of the node you want to apply the custom template to.
    # if you specify __default__, this template will be applied to all nodes.
    # Otherwise it will be only applied to nodes tagged with `k8s_template:<node_name>`
      spark:
    # Kubernetes pod template.
    # It's the full content of the pod-template file (as a string)
    # `run_config.volume` and `MLFLOW_RUN_ID` env are disabled when this is set.
    # Note: python F-string formatting is applied to this string, so
    # you can also use some dynamic values, e.g. to calculate pod name.
        template: |-
          type: Pod
          metadata:
            name: {PodGenerator.make_unique_pod_id('{{ task_instance.task_id }}')}
            labels:
              spark_driver: {'{{ task_instance.task_id }}'}

    # Optionally, you can also override the image
    #   image:
    
    # Optional spark configuration
    spark:
      # Type of spark clusters to use, supported values: dataproc, k8s, kubernetes, custom
      type: dataproc
      # Optional factory of spark operators class
      operator_factory: my_project.factories.OperatorFactory
      # Region indicates location of cluster for public cloud configurations, for example region in GCP
      region: europe-west1
      # Project indicates logical placement inside public cloud configuration, for example project in GCP
      project_id: target-project
      # Name of the cluster to be created 
      cluster_name: ephemeral
      # Location where the spark artifacts are uploaded
      artifacts_path: gs://dataproc-staging-europe-west2-546213781-jabcdefp4/packages
      # Optional path in the project to the script portion preprended to generated init script
      user_init_path: relative_location_to_src/init_script.sh
      # Optional path in the project to the script portion appended to generated init script
      user_post_init_path: relative_location_to_src/post_init_script.sh
      # Optional configuration of the cluster, used during cluster creation, depends on type of the cluster
      cluster_config: # example dataproc configuration
        master_config:
          disk_config:
            boot_disk_size_gb: 35
        worker_config:
          disk_config:
            boot_disk_size_gb: 35
```

## Indicate resources in pipeline nodes

Every node declared in `kedro` pipelines is executed inside pod. Pod definition declares resources to be used based
on provided plugin configuration and presence of the tag `resources` in `kedro` node definition.

If no such tag is present, plugin will assign `__default__` from plugin `resources` configuration.
If no `__default__` is given in plugin `resources` configuration or no `resources` configuration is given, pod 
definition will not be given any information on how to allocate resources to pod, thus default k8s cluster values
will be used.

```python
# train_model node is assigned resources from `huge_machines` configuration, if no such configuration exists,
# `__default__` is used, and if __default__ does not exist, k8s cluster default values are used
node(func=train_model, inputs=["X_train", "y_train"], outputs="regressor", name='train_model', tags=['resources:huge_machines'])
# evaluate_model node is assigned resources `__default__` configuration and if it does not exist,
# k8s cluster default values are used
node(func=evaluate_model, inputs=["X_train", "y_train"], outputs="regressor", name='evaluate_model')
```

## Custom kubernetes pod templates

You can provide custom kubernetes pod templates using `kubernetes_pod_templates`. Pod template to be used is based on
the provided plugin configuration and presence of the tag `k8s_template` in `kedro` node definition.

If no such tag is present, plugin will assign `__default__.template` from plugin `kubernetes_pod_templates` configuration, if exists.
If no `__default__` is given in plugin `kubernetes_pod_templates` configuration or no `kubernetes_pod_templates` configuration is provided at all,
the following plugin's default minimal pod template will be used.

```yaml
type: Pod
  metadata:
    name: {PodGenerator.make_unique_pod_id('{{ task_instance.task_id }}')}
spec:
  containers:
    - name: base
      env:
        - name: MLFLOW_RUN_ID
          value: {{ task_instance.xcom_pull(key="mlflow_run_id") }}
  volumes:
    - name: storage
      persistentVolumeClaim:
        claimName: {self._pvc_name}
```

where environment and volumes sections are present only if kedro mlflow is used in the project and/or
`run_config.volume` is not disabled.

Note, that `claimName` is calculated the following way

```python
pvc_name = '{{ project_name | safe | slugify }}.{% raw %}{{ ts_nodash | lower  }}{% endraw %}'
```

If you do use a custom pod template and you want to keep the built-in mlflow/volume support you need to include
these sections in your template as well.

```python
# train_model node is assigned a custom pod template from `spark` configuration, if no such configuration exists,
# `__default__` is used, and if __default__ does not exist, the plugin's minimal pod template is used
node(func=train_model, inputs=["X_train", "y_train"], outputs="regressor", name='train_model', tags=['k8s_template:spark'])
# evaluate_model node is assigned a custom pod template `__default__` configuration and if it does not exist,
# the plugin's default minimal pod template
node(func=evaluate_model, inputs=["X_train", "y_train"], outputs="regressor", name='evaluate_model')
```

When using custom kubernetes pod templates the resulting pod configuration is a merge between
properties provided via plugin settings, e.g. `resources.__default__.annotations`, and those specified in a
template. In case of a conflict, plugin settings precede that of the template.

## Spark on Kubernetes configuration

In order to configure spark on kubernetes, custom `cluster_config` has to be provided. It has the following
structure:
```yaml
type: kubernetes # or k8s
cluster_name: spark_k8s # name of the Airflow connection id, that points to kubernetes control plane, default `spark_default`
cluster_config:
  # Location of the script that initialize the kedro session and runs the project; is invoked with `run --env=$ENV --node=$NODES --runner=ThreadRunner` kedro parameters
  # arguments
  run_script: local:///home/kedro/spark_run.py
  # Optional image to use for the driver and executor. If not provided, value from `run_config.image` is used
  image: airflow-k8s-plugin-spark-demo
  # Optional spark configuration
  conf:
    spark.dynamicAllocation.enabled: true
    spark.dynamicAllocation.maxExecutors: 4
  # Optional port of the driver
  driver_port: 10000
  # Optional port of the block managers
  block_manager_port: 10001
  # Optional dictionary of secrets to be mounted into driver and executor from the namespace of the project
  secrets:
    kedro-secret: /var/kedro_secret
  # Optional labels to be assigned to driver and executor
  labels:
    huge-machine: yes
  # Optional spark-dir local storage to be mounted from dynamically created PVC
  local_storage:
    class_name: standard
    size: 100Gi
  # Optional dictionary of environment variables to be injected into driver and executor pods
  env_vars:
    GOOGLE_APPLICATION_CREDENTIALS: /var/kedro_secret/sa
  # Optional request for cpu resources, for driver and executor (memory request equals memory limit)
  requests:
    cpu: 2
  # Optional limit for the cpu and memory resources, for driver and executor
  limits:
    cpu: 4
    memory: 16Gi # enforces memory request
  # Optional number of executors to spawn
  num_executors: 4 # by default 1
  # Optional list of jars used by the spark runtime, spark-submit format
  jars: local:///home/kedro/jars/gcs-connector.jar
  # Optional list of repositories used by the spark runtime, spark-submit format
  repositories: https://oss.sonatype.org/content/groups/public
  # Optional list of packages used by the spark runtime, spark-submit format
  packages: org.apache.spark:spark-streaming-kafka_2.10:1.6.0,org.elasticsearch:elasticsearch-spark_2.10:2.2.0
```

Additionally, the following parameters are used from the project configuration to create the pods: `run_config.image`, 
`run_config.image_pull_policy`, `run_config.namespace`,  `run_config.service_account_name`.

Further details on configuring spark jobs on k8s at https://spark.apache.org/docs/latest/running-on-kubernetes.html.

## Dynamic configuration support

kedro-airflow-k8s contains hook that enables TemplatedConfigLoader. It allows passing environment variables to 
configuration files. It reads all environment variables following KEDRO_CONFIG_<NAME> pattern, which you can later 
inject in configuration file using ${name} syntax.

There are two special variables KEDRO_CONFIG_COMMIT_ID, KEDRO_CONFIG_BRANCH_NAME with support specifying default when
variable is not set, e.g. ${commit_id|dirty}

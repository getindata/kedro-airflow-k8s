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
      type: GoogleOAuth2 
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

## Dynamic configuration support

kedro-airflow-k8s contains hook that enables TemplatedConfigLoader. It allows passing environment variables to 
configuration files. It reads all environment variables following KEDRO_CONFIG_<NAME> pattern, which you can later 
inject in configuration file using ${name} syntax.

There are two special variables KEDRO_CONFIG_COMMIT_ID, KEDRO_CONFIG_BRANCH_NAME with support specifying default when
variable is not set, e.g. ${commit_id|dirty}

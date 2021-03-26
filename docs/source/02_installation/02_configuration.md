# Configuration

Plugin maintains the configuration in the `conf/base/airflow-k8s.yaml` file.

```yaml
# Base url of the Apache Airflow, should include the schema (http/https)
host: {url}

# Directory from where Apache Airflow is reading DAGs definitions
output: {output}

# Configuration used to run the pipeline
run_config:

    # Name of the image to run as the pipeline steps
    image: {image}

    # Pull policy to be used for the steps. Use Always if you push the images
    # on the same tag, or Never if you use only local images
    image_pull_policy: IfNotPresent

    # Namespace for Airflow pods to be created
    namespace: {namespace}

    # Name of the Airflow experiment to be created
    experiment_name: {project}

    # Name of the dag as it's presented in Airflow
    run_name: {project}

    # Apache Airflow cron expression for scheduled runs
    cron_expression: "@daily"

    # Optional pipeline description
    #description: "Very Important Pipeline"

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
```

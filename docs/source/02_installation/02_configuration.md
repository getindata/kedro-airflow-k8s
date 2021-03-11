# Configuration

Plugin maintains the configuration in the `conf/base/airflow-k8s.yaml` file.

```yaml
# Image to be used during deployment to Kubernetes cluster
image: docker.registry.com/getindata/kedro-project-image
# Kubernetes namespace in which Airflow creates pods for node execution
namespace: airflow
# K8S access policy for persistent volumes: ReadWriteOnce or ReadWriteMany 
accessMode: ReadWriteOnce
# size of the temporary volume
requestStorage: 1Gi
```

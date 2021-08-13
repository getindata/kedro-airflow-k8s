# Authentication to Airflow API

Most of the operations provided by plugin uses Airflow API to either list dags or trigger them. 
By default, access to Airflow API is blocked and in order to enable it you need to modify `api.auth_backend` config variable [as described in the documentation](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html). Suggested setting for best plugin usage experience is to disable authentication on Airflow by setting value `airflow.api.auth.backend.default` and install middleware proxy blocking access to the API paths to users without expected JWT token in the header.

Sample configuration for istio filter and token issued by gcloud SDK can look like:

```yaml
apiVersion: security.istio.io/v1beta1
kind: RequestAuthentication
metadata:
  name: jwt-token-verification
spec:
  selector:
    matchLabels:
      component: webserver
  jwtRules:
  - issuer: https://accounts.google.com
    jwksUri: https://www.googleapis.com/oauth2/v3/certs
    audiences:
    - 32555940559.apps.googleusercontent.com # google token generator
---
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: airflow-api-access
spec:
  selector:
    matchLabels:
      component: webserver
  rules:
  # allow all users to access UI, but not API
  # UI has its own access management
  - to:
    - operation:
        notPaths: ["/api/*"]
  # enforce JWT token on API
  - when:
    - key: request.auth.audiences
      values:
      - 32555940559.apps.googleusercontent.com # isued by gcloud sdk
    - key: request.auth.presenter
      values:
      - [service-account]@[google-project].iam.gserviceaccount.com
    to:
    - operation:
        paths: ["/api/*"]
```

This setup ensures that all requests to the API paths are validated by Istio by checking the content of JWT token issued by Google (using [gcloud auth print-identity-token](https://cloud.google.com/sdk/gcloud/reference/auth/print-identity-token)]. In order to validate other tokens, modify `audiences` and `jwtRules` accordingly.

Token can be passed to `kedro airflow-k8s` commands by using environment variable `AIRFLOW_API_TOKEN`, for example:

```console
$ AIRFLOW_API_TOKEN=eyJhbGci... kedro airflow-k8s list-pipelines 2> /dev/null
2021-08-13 14:59:13,635 - root - INFO - Registered hooks from 3 installed plugin(s): kedro-kubeflow-0.3.1, kedro-mlflow-0.7.2
2021-08-13 14:59:13,680 - root - INFO - Registered CLI hooks from 1 installed plugin(s): kedro-telemetry-0.1.1
2021-08-13 15:05:38,800 - kedro_telemetry.plugin - INFO - You have opted into product usage analytics.
2021-08-13 14:59:14,764 - kedro.framework.session.store - INFO - `read()` not implemented for `BaseSessionStore`. Assuming empty store.
Name     ID
-------  ------------------
model1   model1-branch-name
```

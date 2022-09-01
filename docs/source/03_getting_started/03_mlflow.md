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

# Authentication to MLflow API

## GoogleOAuth2

Given that Airflow has access to `GOOGLE_APPLICATION_CREDENTIALS` variable, it's possible to configure plugin
to use Google service account to authenticate to secured MLflow API endpoint, by generating OAuth2 token.

All is required to have `GOOGLE_APPLICATION_CREDENTIALS` environment variable setup in Airflow installation and MLflow
to be protected by Google as an issuer. The other thing is to have environment variable `GOOGLE_AUDIENCE` which
indicates OAuth2 audience the token should be issued for.

Also, plugin configuration requires the following:

```yaml
run_config:
  authentication:
    type: GoogleOAuth2
```

## Vars

If you store your credentials in Airflow secrets backend, e.g. HashiCorp vault, it's possible to configure the plugin
to use Airflow Variables as MLFlow API credentials.

Names of the variables need to match expected MLflow environment variable names, e.g. `MLFLOW_TRACKING_TOKEN`.
You specify them in the authentication config. For instance, setting up Basic Authentication requires the following:

```yaml
run_config:
  authentication:
    type: Vars
    params: ["MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"]
```


> NOTE: Authentication is an optional element and is used when starting MLflow experiment, so if MLflow is enabled in
> project configuration. It does not setup authentication inside Kedro nodes, this has to be handled by the project.
> Check GoogleOAuth2Handler class for details.

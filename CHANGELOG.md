# Changelog

## [Unreleased]

-   Support spark projects on K8S

## [0.7.3] - 2021-11-16

-   Take DAG status from the final task
-   Fix finding pyspark DataFrames
-   `MLFLOW_RUN_ID` passed as environment variable to dataproc oriented pipelines

## [0.7.2] - 2021-10-25

-   Support annotations with quotes
-   For pyspark projects, allows to configure post script for dataproc initialization
-   FIX: Broken support for kedro&lt;0.17

## [0.7.1] - 2021-10-21

-   Support for failure notifications via slack
-   FIX: Missing jinja template for dataproc init script

## [0.7.0] - 2021-10-19

-   Schedule supports `dag-name` parameter
-   Support for kedro with pyspark, using Google Dataproc
-   Support for custom pod templates
-   FIX: adding missing dependency package: tabulate
-   FIX: Fix default config template
-   Support populating k8s node env_vars from Airflow variables
-   Generalize auth handler
-   Added `VarsAuthHandler` for MLflow authentication which gets credentials from Airflow variables
-   Changed logging level for pod creation request to debug

## [0.6.7] - 2021-09-01

-   Support for generation of authentication header for secured MLflow API endpoint (via GOOGLE_APPLICATION_CREDENTIALS)

## [0.6.6] - 2021-08-16

-   Support for passing Authorization header for secured Airflow API endpoint (via env variable: `AIRFLOW_API_TOKEN`)
-   Logging `dag_id` and `execution_date` in mlflow run params

## [0.6.5] - 2021-08-05

-   FIX: Adjust service account setup for image based tasks

## [0.6.4] - 2021-08-05

-   FIX: Adjusted operators to make them compatible with Airflow >= 2.1.1
-   FIX: Restore dependency versions that release process bumped unintentionally

## [0.6.3] - 2021-08-04

-   FIX: Avoid file based Jinja template for `data-volume-init`

## [0.6.2] - 2021-08-04

-   FIX: Add missing Jinja template for `data-volume-init` to module manifest 

## [0.6.1] - 2021-08-04

-   FIX: service_account_name or image_pull_secrets should be passed to `data-volume-init` step as well

## [0.6.0] - 2021-07-29

-   Added option to specify service_account_name or image_pull_secrets to executed dag. 

## [0.5.4] - 2021-07-21

-   Run config contains optional start_date in format YYYYMMDD, if not specified default is left to `days_ago(2)` 

## [0.5.3] - 2021-07-12

-   Support for airflow macro parameters and variables

## [0.5.2] - 2021-07-05

-   Support for Secrets in k8s
-   FIX: DeletePipelineStorageOperator was missing trigger rule 'all_done'

## [0.5.1] - 2021-05-17

-   Docker image added as a Mlflow run parameter (to support kedro inference pipeline)

## [0.5.0] - 2021-04-30

-   External dependencies can be added as optionals in configuration
-   Support for labels, tolerations and annotations in k8s
-   Logging added to operators

## [0.4.0] - 2021-04-20

-   Support of S3 as DAG destination
-   Operators extracted from DAG template as reusable components
-   Selection of specific pipeline by name from CLI

## [0.3.0] - 2021-04-15

-   Resources configuration added that allows describing cpu and memory resources required in k8s by pods
-   Shared persistent volume can be made optional
-   Pod startup timeout is configurable, with default to 600 seconds

## [0.2.0] - 2021-04-01

-   Command `list-pipelines` allows display of pipelines which were generated with the plugin
-   Command `ui` opens webbrowser with Apache Airflow
-   Move options to config file and restructure
-   `MLflow` enabled only if `kedro-mlflow` present in dependencies and configuration is in place 
-   Command `init` to initialize configuration for the plugin in kedro project

## [0.1.2] - 2021-03-24

-   Allow override `image` parameter from CLI for `upload_pipeline`, `run_once` and `compile`
-   Conditional SSL verification for Apache Airflow client, via AIRFLOW**CLIENT**SSL_VERIFY environment variable
-   `run-once` with `wait-for-completion` checks for task instance failure as well

## [0.1.1] - 2021-03-19

-   Inits temporary volume with data from image kedro data directory (/home/kedro/data)
-   Increased startup time for pods to 10 minutes

## [0.1.0] - 2021-03-17

-   Creates mlflow experiment on pipeline start if it does not exist
-   Temporary persistent volumes generation and removal for pipelines
-   `upload-pipeline` command that generates DAG to Airflow directory 
-   `schedule` command that generates DAG to Airflow directory with the given schedule
-   `run-once` command that uploads pipeline to Airflow and creates the DAG run

## [0.0.4] - 2021-03-10

### Fixed

-   Added DAG template to package manifest

## [0.0.3] - 2021-03-09

### Fixed

-   Drop dependency on `airflow`

## [0.0.2] - 2021-03-09

### Added

-   Initial implementation of `kedro_airflow_k8s` plugin stub. 

[Unreleased]: https://github.com/getindata/kedro-airflow-k8s/compare/0.7.3...HEAD

[0.7.3]: https://github.com/getindata/kedro-airflow-k8s/compare/0.7.2...0.7.3

[0.7.2]: https://github.com/getindata/kedro-airflow-k8s/compare/0.7.1...0.7.2

[0.7.1]: https://github.com/getindata/kedro-airflow-k8s/compare/0.7.0...0.7.1

[0.6.7]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.6...0.6.7

[0.6.6]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.5...0.6.6

[0.6.5]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.4...0.6.5

[0.6.4]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.3...0.6.4

[0.6.3]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.2...0.6.3

[0.6.2]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.1...0.6.2

[0.6.1]: https://github.com/getindata/kedro-airflow-k8s/compare/0.6.0...0.6.1

[0.6.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.5.4...0.6.0

[0.5.4]: https://github.com/getindata/kedro-airflow-k8s/compare/0.5.3...0.5.4

[0.5.3]: https://github.com/getindata/kedro-airflow-k8s/compare/0.5.2...0.5.3

[0.5.2]: https://github.com/getindata/kedro-airflow-k8s/compare/0.5.1...0.5.2

[0.5.1]: https://github.com/getindata/kedro-airflow-k8s/compare/0.5.0...0.5.1

[0.5.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.4.0...0.5.0

[0.4.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.1.2...0.2.0

[0.1.2]: https://github.com/getindata/kedro-airflow-k8s/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/getindata/kedro-airflow-k8s/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.4...0.1.0

[0.0.4]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.3...0.0.4

[0.0.3]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.2...0.0.3

[0.0.2]: https://github.com/getindata/kedro-airflow-k8s/compare/8f15485216cb040626b491d21e1b61eb3996be73...0.0.2

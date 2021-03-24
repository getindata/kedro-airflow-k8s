# Changelog

## [Unreleased]

-   Command `list-pipelines` allows display of pipelines which were generated with the plugin

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

[Unreleased]: https://github.com/getindata/kedro-airflow-k8s/compare/0.1.1...HEAD

[0.1.1]: https://github.com/getindata/kedro-airflow-k8s/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.4...0.1.0

[0.0.4]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.3...0.0.4

[0.0.3]: https://github.com/getindata/kedro-airflow-k8s/compare/0.0.2...0.0.3

[0.0.2]: https://github.com/getindata/kedro-airflow-k8s/compare/8f15485216cb040626b491d21e1b61eb3996be73...0.0.2

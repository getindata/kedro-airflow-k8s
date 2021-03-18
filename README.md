# Kedro Airflow K8S Plugin

[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8-blue.svg)](https://github.com/getindata/kedro-airflow-k8s)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/kedro-airflow-k8s.svg)](https://pypi.org/project/kedro-airflow-k8s/)
[![Downloads](https://pepy.tech/badge/kedro-airflow-k8s)](https://pepy.tech/project/kedro-airflow-k8s) 

[![Maintainability](https://api.codeclimate.com/v1/badges/f2ef65a9be497267c738/maintainability)](https://codeclimate.com/github/getindata/kedro-airflow-k8s/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/f2ef65a9be497267c738/test_coverage)](https://codeclimate.com/github/getindata/kedro-airflow-k8s/test_coverage)
[![Documentation Status](https://readthedocs.org/projects/kedro-airflow-k8s/badge/?version=latest)](https://kedro-airflow-k8s.readthedocs.io/en/latest/?badge=latest)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fgetindata%2Fkedro-airflow-k8s.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fgetindata%2Fkedro-airflow-k8s?ref=badge_shield)
## About

The main purpose of this plugin is to enable running kedro pipeline with Airflow on Kubernetes Cluster. In difference to 
[kedro-airflow](https://github.com/quantumblacklabs/kedro-airflow) this plugin does not require additional libraries installed
in airflow runtime, it uses K8S infrastructure instead. It supports translation
from Kedro pipeline DSL to [airflow](https://airflow.apache.org/docs/apache-airflow/stable/python-api-ref.html) (python API)
and generation of airflow [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags).

The plugin can be used together with `kedro-docker` to simplify preparation of docker image for pipeline execution.   

## Documentation

For detailed documentation refer to https://kedro-airflow-k8s.readthedocs.io/

## Usage guide

```
Usage: kedro airflow-k8s [OPTIONS] COMMAND [ARGS]...
 
Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile           Create an Airflow DAG for a project
  run-once          Uploads pipeline to Airflow and runs once
  schedule          Uploads pipeline to Airflow with given schedule
  upload-pipeline   Create and upload Airflow DAG to the directory
```

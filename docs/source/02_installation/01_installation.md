# Installation guide

## Kedro setup

First, you need to install base Kedro package in ``<17.0`` version

> Kedro 17.0 is supported by kedro-airflow-k8s, but [not by kedro-mlflow](https://github.com/Galileo-Galilei/kedro-mlflow/issues/144) yet, so the latest version from 0.16 family is recommended.

```console
$ pip install 'kedro<0.17'
```

## Plugin installation

### Install from PyPI

You can install ``kedro-airflow-k8s`` plugin from ``PyPi`` with `pip`:

```console
pip install --upgrade kedro-airflow-k8s
```

### Install from sources

You may want to install the develop branch which has unreleased features:

```console
pip install git+https://github.com/getindata/kedro-airflow-k8s.git@develop
```

## Available commands

You can check available commands by going into project directory and runnning:

```console
$ kedro airflow-k8s

Usage: kedro airflow-k8s [OPTIONS] COMMAND [ARGS]...

Options:
-e, --env TEXT  Environment to use.
-h, --help      Show this message and exit.

Commands:
compile  Create an Airflow DAG for a project
```

### `compile`

`compile` command takes one argument, which is the directory name containing configuration (relative to `conf` folder). 
As an outcome, `dag` directory contains python file with generated DAG.

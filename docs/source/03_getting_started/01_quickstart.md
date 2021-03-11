# Quickstart

## Preprequisites

Although the plugin does not perform deployment, it's recommended to have access to Airflow DAG directory in order to test run the generated DAG.

## Install the toy project with Kedro Airflow K8S support

It is a good practice to start by creating a new virtualenv before installing new packages. Therefore, use `virtalenv` 
command to create new env and activate it:

```console
$ virtualenv venv-demo
created virtual environment CPython3.8.5.final.0-64 in 145ms
  creator CPython3Posix(dest=/home/mario/kedro/venv-demo, clear=False, no_vcs_ignore=False, global=False)
  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/home/mario/.local/share/virtualenv)
    added seed packages: pip==20.3.1, setuptools==51.0.0, wheel==0.36.2
  activators BashActivator,CShellActivator,FishActivator,PowerShellActivator,PythonActivator,XonshActivator
$ source venv-demo/bin/activate
```

Then, `kedro` must be present to enable cloning the starter project, along with the latest version of `kedro-airflow-k8s`
plugin and kedro-docker.
```
$ pip install 'kedro<0.17' kedro-airflow-k8s kedro-docker
```

With the dependencies in place, let's create a new project:

```
$ kedro new --starter=git+https://github.com/getindata/kedro-starter-spaceflights.git --checkout allow_nodes_with_commas
Project Name:
=============
Please enter a human readable name for your new project.
Spaces and punctuation are allowed.
 [New Kedro Project]: Airflow K8S Plugin Demo

Repository Name:
================
Please enter a directory name for your new project repository.
Alphanumeric characters, hyphens and underscores are allowed.
Lowercase is recommended.
 [airflow-k8s-plugin-demo]: 

Python Package Name:
====================
Please enter a valid Python package name for your project package.
Alphanumeric characters and underscores are allowed.
Lowercase is recommended. Package name must start with a letter or underscore.
 [airflow_k8s_plugin_demo]: 

Change directory to the project generated in ${CWD}/airflow-k8s-plugin-demo

A best-practice setup includes initialising git and creating a virtual environment before running
 `kedro install` to install project-specific dependencies. Refer to the Kedro
  documentation: https://kedro.readthedocs.io/
```

> TODO: switch to the official `spaceflights` starter after https://github.com/quantumblacklabs/kedro-starter-spaceflights/pull/10 is merged

Finally, go the demo project directory and ensure that kedro-airflow-k8s plugin is activated:

```console
$ cd airflow-k8s-plugin-demo/
$ kedro install
(...)
Requirements installed!
$ kedro airflow-k8s --help
```console
$ kedro airflow-k8s

Usage: kedro airflow-k8s [OPTIONS] COMMAND [ARGS]...

Options:
-e, --env TEXT  Environment to use.
-h, --help      Show this message and exit.

Commands:
generate  Create an Airflow DAG for a project
```

## Build the docker image to be used on Kubeflow Pipelines runs

First, initialize the project with `kedro-docker` configuration by running:

```
$ kedro docker init
```

This command creates a several files, including `.dockerignore`. This file ensures that transient files are not 
included in the docker image and it requires small adjustment. Open it in your favourite text editor and extend the
section `# except the following` by adding there:

```console
!data/01_raw
```

This change enforces raw data existence in the image. Also, one of the limitations of running the Kedro 
pipeline on Airflow (and not on local environment) is inability to use MemoryDataSets, as the pipeline nodes do not
share memory, so every artifact should be stored as file. The `spaceflights` demo configures four datasets as 
in-memory, so let's change the behaviour by adding these lines to `conf/base/catalog.yml`:

```console
X_train:
  type: pickle.PickleDataSet
  filepath: data/05_model_input/X_train.pickle
  layer: model_input

y_train:
  type: pickle.PickleDataSet
  filepath: data/05_model_input/y_train.pickle
  layer: model_input

X_test:
  type: pickle.PickleDataSet
  filepath: data/05_model_input/X_test.pickle
  layer: model_input

y_test:
  type: pickle.PickleDataSet
  filepath: data/05_model_input/y_test.pickle
  layer: model_input
```

Finally, build the image:

```console
kedro docker build
```

When execution finishes, your docker image is ready. If you don't use local cluster, you should push the image to the remote repository:

```console
docker tag airflow_k8s_plugin_demo:latest remote.repo.url.com/airflow_k8s_plugin_demo:latest
docker push remote.repo.url.com/airflow_k8s_plugin_demo:latest
```

## Setup GIT repository

Plugin requires project to be under git repository. Perform [repository initialization](https://git-scm.com/docs/git-init) and commit project files  

## Generate DAG

Create configuration file in `conf/pipelines/airflow-k8s.yaml`:

```yaml
image: remote.repo.url.com/airflow_k8s_plugin_demo:latest
# This should match namespace in Kubernetes cluster, where pods will be created
namespace: airflow
```

Also mlflow configuration has to be set up as described in [mlflow section](./03_mlflow.md).

Having configuration ready, type:

```console
kedro airflow-k8s -e pipelines generate
```

This command generates DAG in `dag/airflow_k8s_plugin_demo.py`. This file should be copied manually into Airflow DAG
directory, 
that Airflow periodically scans. After it appears in airflow console, it is ready to be triggered. 
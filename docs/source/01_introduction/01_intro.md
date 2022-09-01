# Introduction

## What is Airflow?

[Airflow](https://airflow.apache.org/) is a platform to programmatically author, schedule and monitor workflows.
Workflows are represented as DAGs. Each DAG is represented by nodes, that define job to be executed.
The DAGs are stored in the file storage, allowing user 
to run the pipeline once or schedule the recurring run.

## What is Kubernetes?

[Kubernetes](https://kubernetes.io/) is a platform for managing containerized workloads and services,
that facilitates both declarative configuration and automation.

## Why to integrate Kedro project with Airflow and Kubernetes?

Airflow's main attitude is the portability. Once you define a pipeline,
it can be started on any Kubernetes cluster. The code to execute is stored inside 
docker images that cover not only the source itself, but all the libraries and 
entire execution environment. Portability is also one of key Kedro aspects, as 
the pipelines must be versionable and shippable in a containerized form. Kedro, with 
[Kedro-docker](https://github.com/quantumblacklabs/kedro-docker) plugin do a fantastic 
job to achieve this and Airflow looks like a nice add-on to run the pipelines 
on powerful remote Kubernetes clusters.

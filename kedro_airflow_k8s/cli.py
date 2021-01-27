import click


@click.group("airflow-k8s")
def commands():
    """Kedro plugin adding support for Airflow on K8S"""
    pass


@commands.group(
    name="airflow-k8s",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.option(
    "-e", "--env", "env", type=str, default="base", help="Environment to use."
)
@click.pass_obj
def airflow_group(metadata, env):
    pass


@airflow_group.command()
def hello():
    print("Hello world!")

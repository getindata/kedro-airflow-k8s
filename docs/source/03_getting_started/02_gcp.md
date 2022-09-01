# GCP AI Platform support

Google Cloud's AI Platform offers couple services that simplify Machine Learning 
tasks.

### Using `kedro` with AI Platform Notebooks

[AI Platform Notebooks](https://cloud.google.com/ai-platform-notebooks) provides 
an easy way to manage and host JupyterLab based data science workbench environment.
What we've found out is that the default images provided by a service cause some
dependency conflicts. To avoid this issues make sure you use isolated virtual
environment, e.g. [virtualenv](https://pypi.org/project/virtualenv/). New virtual 
environment can be created by simply invoking `python -m virtualenv venv` command.






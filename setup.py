"""kedro_airflow_k8s module."""

from setuptools import find_packages, setup

with open("README.md") as f:
    README = f.read()

# Runtime Requirements.
INSTALL_REQUIRES = [
    "kedro>=0.16,<=0.18",
    "click<8.0",
    "semver~=2.10",
    "python-slugify>=4.0.1",
]

# Dev Requirements
EXTRA_REQUIRE = {
    "tests": [
        "pytest>=6.2.2, <7.0.0",
        "pytest-cov>=2.8.0, <3.0.0",
        "tox==3.21.1",
        "pre-commit==2.9.3",
        "apache-airflow[kubernetes]==2.0.1",
        "apache-airflow-providers-cncf-kubernetes==1.1.0",
        "mlflow==1.14.1",
        "sqlalchemy==1.3.23",
        "responses>=0.13.0",
    ],
    "docs": [
        "sphinx==3.4.2",
        "recommonmark==0.7.1",
        "sphinx_rtd_theme==0.5.2",
    ],
    "gcp": [
        "gcsfs>=0.6.2, <0.7.0",
    ],
    "aws": ["s3fs>=0.6.0"],
    "mlflow": ["kedro-mlflow==0.4.1"],
}

setup(
    name="kedro-airflow-k8s",
    version="0.5.3",
    description="Kedro plugin with Airflow on Kubernetes support",
    long_description=README,
    long_description_content_type="text/markdown",
    license="Apache Software License (Apache 2.0)",
    python_requires=">=3",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="kedro airflow kubernetes k8s ml mlops plugin",
    author=u"Michal Zelechowski, Mariusz Strzelecki, Mateusz Pytel",
    author_email="mateusz@getindata.com",
    url="https://github.com/getindata/kedro-airflow-k8s/",
    packages=find_packages(exclude=["ez_setup", "examples", "tests", "docs"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRA_REQUIRE,
    entry_points={
        "kedro.project_commands": [
            "airflow-k8s = kedro_airflow_k8s.cli:commands"
        ],
        "kedro.hooks": [
            "kubeflow_cfg_hook = "
            "kedro_airflow_k8s.hooks:register_templated_config_loader",
        ],
    },
)

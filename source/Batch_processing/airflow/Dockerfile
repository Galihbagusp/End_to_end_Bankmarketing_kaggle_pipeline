FROM apache/airflow

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get upgrade -y && \
    apt-get install -y git

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

RUN pip install --upgrade pip
RUN pip install apache-airflow[google]
RUN pip install gcsfs
RUN pip install dbt-bigquery
RUN pip install pyarrow
RUN pip install kaggle
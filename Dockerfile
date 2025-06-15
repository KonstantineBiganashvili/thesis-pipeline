FROM europe-west3-docker.pkg.dev/masters-463009/thesis-docker/py311:base

RUN pip install apache-airflow==2.10.5 \
    apache-airflow-providers-google \
    google-cloud-storage \
    google-cloud-bigquery \
    pytest

COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/

ENTRYPOINT ["bash"]

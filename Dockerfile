FROM apache/airflow:2.11.0-python3.9
USER root
# Add the Python environment
RUN python -m pip install -U --no-cache-dir pip && \
    python -m pip install --no-cache-dir "scikit-learn>=1.2"
# Back to the airflow user
USER airflow

ENV AIRFLOW_UID=1000
ENV N_SHARDS=4
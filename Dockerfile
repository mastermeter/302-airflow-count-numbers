FROM apache/airflow:2.11.0-python3.9
ARG DOCKER_GID=1001
COPY --from=docker:dind /usr/local/bin/docker /usr/local/bin/

USER root
# Add the Python environment
RUN python -m pip install -U --no-cache-dir pip && \
    python -m pip install --no-cache-dir "scikit-learn>=1.2"

# Back to the airflow user
#RUN groupadd -g ${DOCKER_GID} docker && \
#usermod -aG docker airflow
USER airflow

ENV N_SHARDS=4
FROM apache/airflow:3.0.3

ARG AIRFLOW_USER_HOME=/home/airflow

USER root

RUN mkdir -p ${AIRFLOW_USER_HOME}/.config/gcloud/

# Copy the local credentials file into the image at the correct location
# This command assumes the file is in a 'config' directory at the build root.
COPY config/application_default_credentials.json ${AIRFLOW_USER_HOME}/.config/gcloud/

# Change the ownership of the copied credentials to the airflow user.
# This is a critical step for permissions.
RUN chown -R airflow ${AIRFLOW_USER_HOME}/.config

# Switch back to the airflow user for subsequent commands
USER airflow

# Copy our requirements.txt file into the container
COPY requirements.txt /

# Install the Python packages
RUN pip install -r /requirements.txt
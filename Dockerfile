FROM ubuntu:latest
LABEL authors="kayleedekker"

ENTRYPOINT ["top", "-b"]

# Use Python 3.8 Slim Buster as the base image
FROM python:3.8-slim-buster

# Set working directory in the Docker container
WORKDIR /app

# Install system dependencies required for Google Cloud SDK
RUN apt-get update && apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y

# Copy the requirements_dataflow.txt file and install Python dependencies
COPY requirements_dataflow.txt .
RUN pip install -r requirements_dataflow.txt

# Copy your application code into the container
COPY weather-data/ /app/weather-data/
COPY cloud_bucket/ /app/cloud_bucket/
COPY submit_dataflow_job.sh /app/

# Make sure your script is executable
RUN chmod +x /app/submit_dataflow_job.sh

# The command or entrypoint to run your application
CMD ["/app/submit_dataflow_job.sh"]


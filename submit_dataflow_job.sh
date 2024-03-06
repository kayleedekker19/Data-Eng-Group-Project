#!/bin/bash

# Set variables
DATA_PATH="gs://${BUCKET_NAME}/weather/data/dataflow"
TEMP_LOCATION="gs://${BUCKET_NAME}/weather/temp"
EXTRA_PACKAGE="./weather-data/dist/weather-data-1.0.0.tar.gz"

# We can activate our virtual environment if needed
/Users/kayleedekker/PycharmProjects/DataEngineeringProject/.venv/bin/activate

# Run the Dataflow job submission command
python cloud_bucket/create_datasets_beam.py \
  --data-path="${DATA_PATH}" \
  --runner="DataflowRunner" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --temp_location="${TEMP_LOCATION}" \
  --extra_package="${EXTRA_PACKAGE}"


# We simply run this script by executing the line below in the terminal
# ./submit_dataflow_job.sh
# This needs to be added to the Dockerfile

# To update this script we run this in the terminal
# chmod +x submit_dataflow_job.sh

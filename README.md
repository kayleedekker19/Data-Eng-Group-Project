# Data Engineering Group Project: Weather Decision-Making in Aviation

## Project Introduction

This repository is for our Data Engineering project, an project for gaining practical experience with key technologies and designing scalable solutions in line with best practices. Our project serves the aviation industry by developing capabilities for making informed weather-related decisions. It consists of two main components:
1. **Data Pipeline Development**: We construct a data pipeline that facilitates data scientists in creating predictive models based on satellite imagery for weather forecasting.
2. **Relational Database Construction**: We build a comprehensive database by collecting data from three sources: airport and forecast data via APIs, and historic weather data through web scraping. This enables the holistic storage and analysis of aviation-related weather information.

## Objectives

- Implementing scalable data pipeline architecture.
- Creating replicable environments using Docker.
- Employing version control with Git.
- Utilizing a variety of technologies and tools, such as cloud-based solutions.

## Tools and Technologies

- **Version Control**: Git
- **Containerization**: Docker, Docker Compose
- **Data Processing**: Apache Spark, Apache Beam
- **Cloud Computing**: Google Cloud Dataflow, Google Cloud Storage
- **Database Management**: Google Cloud SQL with PostgreSQL

## Setup and Installation Requirements

As our project is divided into two distinct parts, each with its unique Docker environment to cater to their specific dependency needs, we maintain two separate `requirements.txt` files:

- **For Part One (Data Pipeline for Weather Prediction)**: `requirements_Dataflow.txt` manages all dependencies required for setting up the data processing environment, including Apache Beam and Google Cloud Dataflow specifics.
  
- **For Part Two (Relational Database Design)**: `requirements.txt` is dedicated to the database part of our project, ensuring smooth operation of our Python scripts for ETL processes and PostgreSQL database connections without any dependency overlap with Part One.

This separation was a deliberate decision to combat potential dependency issues and ensure that each part of the project runs in its optimal environment.
## Project Structure

### Data Pipeline for Weather Prediction

- **Important Notebooks and Scripts**:
  - `weather-data/`, `cloud_bucket/`, `submit_Dataflow_job.sh` for pipeline construction.
  - `requirements_Dataflow.txt` for dependencies.
  - `Dockerfile`, `DockerfileCI`, and GitHub Actions workflow `main.yml` for CI/CD and Docker configurations.

### Relational Database Design

- **ETL Scripts**:
  - Main Scripts: `forecast_ETL.py`, `historic_data_to_sql.py`, and `airport_ETL.py` are used for the final versions of data extraction, transformation, and loading processes.
  - PostgreSQL Subdirectory: Contains additional scripts like `create_schema.py` for setting up our database schema and `queries.py` for executing queries. 
- **Folders**:
  - `data_sources/` for initial manual processes and local development.
  - `ETL process/` documenting the ETL steps, including iterations to get to the final versions (Main Scripts), and our attempts transferring data directly from GCS to Apache Spark.
- **Docker and Docker Compose**:
  - `Dockerfile_forecast`, `Dockerfile_historic`, `Dockerfile_airport`, and `docker-compose.yml` for database setup.
- **GitHub Actions**:
  - `compose-build-push.yml` for CI/CD of the database component.

### Dashboard
- **Visualization and Querying**:
  - `Flask_dashboard/query_flask_dashboard` for dashboard scripts.

## Data Collection and Processing

Our approach utilizes APIs and web scraping to collect data, as well as the use of three sources from Google Earth Engine for satellite images. This diverse method of data collection enables us to build a rich dataset for our data pipeline and relational database, and work with data in different formats.

## Team Members

- Adrien Farman: AdrienFrmn
- Alex Kartashov: kartagram
- Jonny Betts: jonnybetts14
- Kaylee Dekker: kayleedekker19

## License

This project is made available under the Apache License 2.0.

Copyright 2024 Adrien Farman, Alex Kartashov, Jonny Betts, and Kaylee Dekker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

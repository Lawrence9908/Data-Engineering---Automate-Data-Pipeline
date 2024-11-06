# Data Pipelines with Airflow for Sparkify

## Project Overview

Sparkify, a music streaming company, is enhancing their data warehouse ETL processes with Apache Airflow. This project aims to create robust, automated data pipelines that are dynamic, reusable, and easily monitored. The focus is on maintaining high data quality to support accurate analyses.

## Data Sources

- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

These datasets, stored in JSON format, contain user activity logs and song metadata respectively.

## Implementation Steps

### 1. S3 Data Preparation

Create a new S3 bucket and transfer data:

```bash
aws s3 mb s3://uc-de-airflow-aws/
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp ~/log-data/ s3://uc-de-airflow-aws/log-data/ --recursive
aws s3 cp ~/song-data/ s3://uc-de-airflow-aws/song-data/ --recursive
```

Verify the data transfer:

```bash
aws s3 ls s3://uc-de-airflow-aws/log-data/
aws s3 ls s3://uc-de-airflow-aws/song-data/
```

The DAG consists of the following custom operators:

- `Begin_execution` and `End_execution`: Dummy operators to mark DAG boundaries
- `Stage_events` and `Stage_songs`: Extract data from S3 to Redshift staging tables
- `Load_songplays_fact_table`: Populates the fact table from staging tables
- `Load_user_dim_table`, `Load_song_dim_table`, `Load_artist_dim_table`, `Load_time_dim_table`: Populate dimension tables
- `Run_data_quality_checks`: Executes data quality checks

## Execution Guide

1. Set up the S3 bucket and transfer the data as described above.
2. Configure AWS connection in Airflow UI.
3. Set up Redshift serverless and add the connection information to Airflow UI.
4. Execute the project DAG and monitor its progress through the Airflow UI.

This pipeline ensures efficient data transfer from S3 to Redshift, transforms the data into a star schema, and performs quality checks to maintain data integrity. The modular design allows for easy maintenance and scalability of the ETL process.

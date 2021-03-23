# Cloud Data Warehouse

## Overview

The purpose of this project is to help music streaming app Sparkify move their data onto the cloud dataware house from S3.

ETL pipeline is built to extract log data and song data from S3 and stages them in PostgreSQL hosted on AWS Redshift. A star schema optimised for queries on _song play_ analytics table, which is to analyse songs and user log data on their new music streaming app. The team is particularly interested in understanding what songs users are listening to.

## Project Structure

The project contains the following components: 

- `create_tables.py` creates Sparify star schema in Redshift
- `etl.py` run ETL pipeline from S3 and load into staging tables on AWS Redshift
- `sql_queries.py` SQL queries that underpin the creation of star schme and ETL pipeline

## Star Schema

The database has following tables:

- `songplays` user songs plays 

which is fact table has foreign keys to following dimension tables:

- `songs ` songs data
- `artists ` artists data
- `users` users data in the app
- `time` timestamps of records broken down into time units

| Tables    | Columns                                                      |
| --------- | ------------------------------------------------------------ |
| songplays | *songplay_id*, *start_time*, *user_id*, *level*, *song_id*, *artist_id*, *session_id*, *location*, *user_agent* |
| songs     | *song_id*, *title*, *artist_id*, *year*, *duration*          |
| artists   | *artist_id*, *name*, *location*, *latitude*, *longitude*     |
| users     | *user_id*, *first_name*, *last_name*, *gender*, *level*      |
| time      | *start_time*, *hour*, *day*, *week*, *month*, *year*, *weekday* |

## Instructions

You will need to create a configuration file with the file name `dwh.cfg` and the following structure: **file should not be pushed into repo or added it into .gitignore**

```
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>
DB_REGION=<your_db_region>
CLUSTER_IDENTIFIER=<your_cluster_identifier>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
ACCESS_KEY=<your_access_key>
SECRET_KEY=<your_secret_key>
```

**To run ETL pipeline, run code from commind line in the project home directory:**

```
python3 create_tables.py
python3 etl.py
```

## Query Example

Once the database is created, you can test out queries in Redshift console 

```sql
SELECT COUNT(song_id) FROM songplays ORDER BY DESC LIMIT 10;
```

To find out top 10 songs played by users.

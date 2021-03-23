## Data Warehouse and ETL from  with Python

### Brief
The purpose of this project is to help Sparkify move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

ETL pipeline is built to extract log data and song data from s3 and stages them in Redshift. A star schema optimised for queries on song play analysis, which is to analyze the data they've been collecting on songs and user activity on their new music streaming app. The team is particularly interested in understanding what songs users are listening to.

A star schema with a fact table songplays, which contains records in log data associated with song plays. Also four dimension tables users, songs, artists, time are respectively storaging users in the app, songs in music database, artists in music database, and timestamps of records in songplays broken down into specific time units. 

After creating four dimension tables, artist id and song id are found based on title, name, duration of the song from log data to create fact table. 

The team can simply query "SELECT COUNT(song_id) FROM songplays ORDER BY DESC LIMIT 10;" to tell top 10 songs played by users.

### Tables Description

**Staging Tables(1): staging_songs**
It contains *artist_id*, *artist_latitude numeric*, *artist_longitude numeric*, *artist_location*, *artist_name*, *song_id*, *title*, *duration*, *year*. Which is a table COPY from s3 to Redshift and stage on Redshift. 

**Staging Tables(2): staging_events**
It contains *artist*, *auth*, *firstName*, *gender*, *itemInSession*, *lastName*, *length*, *level*, *location*, *method*, *page*, *registration*, *sessionId*, *song*, *status*, *ts*, *userAgent*, *userId*. Which is another staging table COPY from s3 to Redshift. 

**Face Table: songplays**
It contains *songplay_id*, *start_time*, *user_id*, *level*, *song_id*, *artist_id*, *session_id*, *location*, *user_agent*. 
data are exported from staging tables. 

**Dimesion Table**
**songs**
It contains *song_id*, *title*, *artist_id*, *year*, *duration*.
Data are extracted from staging tables. 
**artists**
It contains *artist_id*, *name*, *location*, *latitude*, *longitude*.
Data are extracted from staging tables. 
**users**
It contains *user_id*, *first_name*, *last_name*, *gender*, *level*.
Data are extracted from staging tables. 
**time**
It contains *start_time*, *hour*, *day*, *week*, *month*, *year*, *weekday*
Timestamps of records in songplays broken down into specific units

### Files Description
**sql_queries.py**
The file contains all sql queries for tables creation, load data, and data insert.
**create_tables.py**
Run this file to connect into Redshift and create tables.
**etl.py**
Run this file to process songs and log data from s3 and create a ETL pipeline in order to move it to redshift. 
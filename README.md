# Data Lake

The fourth Udacity DEND project

### Getting started
The ETL project was protyped in the project workspace on the enviroment provided by Udacity. This was done on a small set of data. Once the proof of concept worked it was refined on the AWS EMR cluster.  


## Run me 

**command line**
```
python etl.py
```

**ipython notebook**
```
%run -i 'etl'
```

## Project Files


### etl.py

This file is resposible for creating a spark context and using it to extract files from an S3 before loading tham into a data lake 

This is broken down into two steps:
* Processing the song data and loading it into songs and artists table 
* Transform and load event data into a the data lake. The time and the users table are loaded from the event data. The event data and the song data file are joined to create the data set for songplays 

The is also a function to create the spark context needed for execute the ETL process. 

### Scrapbook.ipynb

This is the Scrapbook that I used to create my proof of concept for the Project. It has been cleaned up, refined and commented. 


## Tables Commentary
 
### songplays

The songplays data is partitioned on the year and the month keys. This basically are file directorys used to organize and index the data. Using month and year as predicates ensure fast retreval of the data. 

**Joining staging_events_table and staging_songs_table**
Two keys were used to join the events and songs staging tables. This took match (song, artist) in events to (title, artist_name) to ensure unique artist and song combinations because song and artist names are not unique.

### users

Special note that we de-duplicated the users data because users can appear ore then once in the log data. 

### artists

Special note that we de-duplicated the artists data because artists can appear ore then once in the log data. 

The songplays data is partitioned on the year and the month keys. This basically are file directorys used to organize and index the data. Using month and year as predicates ensure fast retreval of the data.


### time

Special note that we de-duplicated the time data because time can appear ore then once in the log data. 

The time data is partitioned on the year and the month keys. This basically are file directorys used to organize and index the data. Using month and year as predicates ensure fast retreval of the data.




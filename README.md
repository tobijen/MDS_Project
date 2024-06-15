# MDS_Project

## How to run the project

<div style="border:1px solid #f00; padding:10px; background-color:;">
  <strong>Important: Docker and Docker Compose need to be installed on the system! </strong> 
</div>

<br>

1. Open a terminal a navigate into the project directory where the docker-compose.yml file is.

2. Run the command:
```console
foo@bar/pathtofolder:~$ docker-compose up -d
```
3. Insert data into Postgres: run file ```insert_data_postgres.py```
4. Insert data into Postgres: run file ```insert_data_couchbase.py```

## Open databases

For opening and editing the postgres db it is suggested to DBeaver: https://dbeaver.io/
<br>
For opening and editing the couchbase simply go to the following url: http://localhost:8091/ui/index.html
<br>
<br>
Connect to the databases by adding the credentials from the docker-compose.yml

## Stop project

Run:
```console
foo@bar/pathtofolder:~$ docker-compose down
```
to stop shutdown the databases
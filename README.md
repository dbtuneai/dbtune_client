# Running the client ...

### ... by downloading it from the platform
Log in on your account at https://app.dbtune.ai/. Follow the instructions in the DBtune user guide at the top left of the screen to set up and start a tuning session.<br>

### ... locally
Run the client by running
```
python __main__.py <API-KEY> <JOB-ID>
```
You will get the api-key and job-id from the wget command on the platform. Follow the same instructions as in the user guide, except when you get the command from the wget button, dont run the whole command but instead retrieve only the last two strings in it, after "./dbtune-swclient". They look something like "98f44ca3-ebd8-4e9f-981f-8b417f4e24e5 fbdc942b-db03-4442-9983-d164fae8ec90".

### Reading the performance

#### Postgres performance
The client measures throughput by reading the total number of commits since the beginning of time, every second, via:
```
sudo -i -u <username> psql -c \"SELECT xact_commit FROM pg_stat_database WHERE datname='<datname>';
```
and calculates it using the last call and the time since the last call.

Query runtime is measured every second using this call:
```
"SELECT JSON_OBJECT_AGG(queryid, JSON_BUILD_OBJECT('calls',calls,'total_exec_time',{})) FROM (SELECT * FROM pg_stat_statements ORDER BY calls DESC) AS f"
```

#### System metrics
The command
```
psutil.virtual_memory()
```
is run by the client every second to get memory utilisation. The command
```
psutil.cpu_percent()
```
is run the by the client every second to get cpu utilisation. The command
```
iostat -xc -y '+ {physical_derive} + ' 1 1'
```
were physical_derive is the drive where the postgres database is stored is run by the client every second. This gives both iops and disk metrics.

### Restarting the DB
If the restart option is enabled via the platform, restarting postgres is performed by the system call 
```
sudo service postgresql restart
```
This causes a brief period of downtime. 

### Updating the config
The script creates a subdirectory conf.d and writes a file postgresql.conf in the directory:
```
/etc/postgresql/14/main/conf.d/postgresql.conf
```
Additionally, the following line is appended in the main configuration file
```
include_dir 'conf.d'
```
Configuration values in the temporary configuration file takes precedence in accordance with 18.1.5 here:
https://www.postgresql.org/docs/9.5/config-setting.html

### Advanced settings
If you select 'N' to the question 'Do you have a standard postgres installation' you can specify the path to the postgres binaries and also the command for restarting the database.
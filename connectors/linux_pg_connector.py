import os
import time
import datetime
import subprocess
import sys
import platform
from threading import Timer
import logging
import psutil
import distro
import json
from TuningError import TuningError
import getpass
from connectors.connector import Connector

def get_connect(pg_isready_path, port):
    p1 = subprocess.Popen([pg_isready_path, "-h", "localhost", "-p", str(port)],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    command_output, _ = p1.communicate()
    logging.debug(command_output.decode('ascii').strip())
    return p1.returncode


class LinuxPgConnector(Connector):
    def __init__(self, connector_data):
        user=os.popen("whoami").read().strip()
        if user!="root":
            sys.exit("You need to run the dbtune swclient as a root user")
        logging.info("Initiating LinuxPgConnector")
        self.connection_details = self.establish_connection()

        state = get_connect(self.connection_details["pg_isready_path"], self.connection_details["port"])
        while not state == 0:
            if state == 2:
                sys.exit("Unable to connect to postgres")
        postgres_server_version = self.psql("-At -c \"show server_version;\"").readlines()[-1].split(' ')[0].strip()
        self.postgres_major_version = postgres_server_version.split('.')[0].strip()
        postgres_client_version = self.psql("-At -V").readlines()[-1].split(' ')[2].strip()
        self.db_version = postgres_server_version
        self.os_type = platform.system()
        self.memory = psutil.virtual_memory().total
        self.no_of_cpu = psutil.cpu_count()
        client_info_log_list = ["Hardware and DBMS information:"]
        client_info_log_list.append('No of CPU(s): '+ str(self.no_of_cpu))
        client_info_log_list.append('Memory: ' + "%.2f" %(self.memory/1024**3) + 'GB')
        client_info_log_list.append('PostgreSQL-major-version:'+ self.postgres_major_version)
        client_info_log_list.append('PostgreSQL-client-version:'+ postgres_client_version)
        client_info_log_list.append('PostgreSQL-server-version:'+ postgres_server_version)
        logging.info('\n'.join(client_info_log_list))

    def establish_connection(self):
        db_connection_details = {}

        # Do they have iostat (sysstat) installed?
        try:
            subprocess.check_output('iostat', shell=True)
        except:
            logging.info(f"iostat threw an error. Please make sure the package sysstat is installed on your machine. Install with: sudo apt-get install sysstat, or corresponding on your system.")
            sys.exit()

        # Do they have a default installation of postgres or not?
        standard_install = ""
        i = 0
        while standard_install not in ['Y','y','N','n']:
            standard_install = input('Do you have a standard installation of postgres? [Y] yes   [N] no:   [Default: Y]: ').strip() or "Y"
            i += 1
            if i>4:
                sys.exit()

        if standard_install in ['N', 'n']:
            # finding out where they have their postgres binaries
            which_psql_output = subprocess.run(['which', 'psql'], stdout=subprocess.PIPE)
            which_psql = which_psql_output.stdout.decode('utf-8').strip()
            if which_psql == '':    # no default psql command exists. No default option available
                postgres_binaries_path = input(f'Enter the path to the postgres binaries. Example: "/usr/lib/postgresql/14/bin": ').strip() or ""
                path_exists = os.path.exists(postgres_binaries_path)
                while path_exists == False:
                    postgres_binaries_path = input("That path doesn't exist. Enter the path to the postgres binaries. Example: \"/usr/lib/postgresql/14/bin\": ").strip() or ''
                    path_exists = os.path.exists(postgres_binaries_path)
            else:                   # default psql command exists. Default option available
                postgres_binaries_path = input(f'Enter the path to the postgres binaries. Example: "/usr/lib/postgresql/14/bin". [Default: the "default" psql and pg_isready commands will be used]: ').strip() or "DEFAULT"
                if postgres_binaries_path != "DEFAULT":
                    path_exists = os.path.exists(postgres_binaries_path)
                else:
                    path_exists = True
                while path_exists == False:
                    postgres_binaries_path = input("That path doesn't exist. Enter the path to the postgres binaries. Example: \"/usr/lib/postgresql/14/bin\": ").strip() or ''
                    path_exists = os.path.exists(postgres_binaries_path)

            if postgres_binaries_path == "DEFAULT":
                psql_path = "psql"
                pg_isready_path = "pg_isready"
            else:
                psql_path = postgres_binaries_path + "/psql"
                pg_isready_path = postgres_binaries_path + "/pg_isready"
        else:
            psql_path = "psql"
            pg_isready_path = "pg_isready"


        # asking for username, (password), port and database name
        username = input("Enter the name for the postgres superuser [Default: postgres]: ").strip() or "postgres"
        password = ""
        password_auth = False
        if username != "postgres":
            while True:
                password = getpass.getpass("Enter the password for the postgres superuser: ")
                if not password:
                    password = getpass.getpass("Enter the password for the postgres superuser: ")
                    continue
                else:
                    password_auth = True
                    os.environ["PGPASSWORD"] = password
                    break

        port = input("Enter the TCP/IP port to connect to postgres [Default: 5432]: ").strip() or "5432"
        while True:
            if (len(port)!=4) or not (float(port).is_integer()):
                port = input("Please enter the correct port: ").strip()
                continue
            else:
                break

        database_name = input("Enter the name of the database: ").strip()
        while True:
            if not database_name:
                database_name = input("Please enter the correct database name: ").strip()
                continue
            else:
                break

        # testing psql and pg_isready
        psql_command = "-At -c \"show server_version;\""
        if password_auth:
            command = f'{psql_path} -p {port} -U {username} -d {database_name} -h localhost {psql_command}'    
        else:
            command = f'sudo -i -u postgres {psql_path} -p {port} -d {database_name} {psql_command}'
        try:
            subprocess.check_output(f'{command}', shell=True)
        except:
            logging.info(f"The example psql call: {command} threw an error")
            db_connection_details = self.establish_connection()
            return db_connection_details


        # what command to use to restart the postgres instance
        if standard_install in ['N', 'n']:
            postgres_restart_command = input(f'Enter the command for restarting the postgres instance. Only necessary if the restart option is enabled or if the tuning target is query runtime and pg_stats is not already installed (one restart required). [Default: systemctl restart postgresql]: ').strip() or "systemctl restart postgresql"
            
        else:
            postgres_restart_command = "systemctl restart postgresql"

        # testing the command (status instead of restart)
        restart_command_status = postgres_restart_command.replace("restart", "status")
        restart_command_status_worked = False
        while restart_command_status_worked == False:
            try:
                subprocess.check_output(f'{restart_command_status}', shell=True) 
                restart_command_status_worked = True
            except:
                logging.info(f"The command: {restart_command_status} threw an error")
                proceed_anyways = input(f'Proceed anyways (the restart option wont work)? [Y] yes   [N] no   [Default: N]: ').strip() or "N"
                if proceed_anyways in ['Y', 'y']:
                    restart_command_status_worked = True
                else:
                    restart_command_status_worked = False
                    postgres_restart_command = input(f'Enter the command for restarting the postgres instance. [Default: systemctl restart postgresql]: ') or "systemctl restart postgresql"
                    restart_command_status = postgres_restart_command.replace("restart", "status")
                
            


        state = os.system('{} -h {} -p {} -U {}'.format(pg_isready_path,'localhost', port, username))
        if state == 0:
            db_connection_details["psql_path"] = psql_path
            db_connection_details["pg_isready_path"] = pg_isready_path
            db_connection_details["username"] = username
            db_connection_details["password"] = password
            db_connection_details["password_auth"] = password_auth
            db_connection_details["port"] = int(port)
            db_connection_details["database_name"] = database_name
            db_connection_details["postgres_restart_command"] = postgres_restart_command
            return db_connection_details
        else:   # run again
            db_connection_details = self.establish_connection()
            return db_connection_details

    def get_client_info(self):
        logging.info("Getting client's system and DBMS information")
        client_info = {}
        client_info["DBVERSION"] = self.db_version
        client_info["OSTYPE"] = self.os_type
        client_info["NUMOFCPU"] = self.no_of_cpu
        client_info["MAXCONNECTIONS"] = int(self.psql("-At -c \"show max_connections;\"").readlines()[-1].strip())
        client_info["TOTALMEMORY"] = self.memory
        client_info["AVAILABLEMEMORY"] = psutil.virtual_memory().available
        cloud_provider, instance_type = self.get_cloud_and_instance_info()
        client_info["CLOUDPROVIDER"] =  cloud_provider
        client_info["INSTANCETYPE"] = instance_type
        data_directory_path = self.psql("-At -c \"show data_directory;\"").readlines()[-1].strip()
        client_info["DATABASESIZE"] = os.popen('sudo du -sb {}'.format(data_directory_path)).read().split('\t')[0]
        disk_info = os.popen('sudo df {}'.format(data_directory_path)).readlines()[1].split()
        client_info["DISKSIZE"] = int(disk_info[1])*1024
        hdd = bool(int(os.popen('lsblk {} -o name,rota'.format(disk_info[0])).readlines()[1].strip().split()[1]))
        if hdd:
            client_info["HDTYPE"] = "hdd"
        else:
            client_info["HDTYPE"] = "ssd"
        return client_info

    def psql(self, command):
        if self.connection_details["password_auth"]:
            command = "{} -p {} -U {} -d {} -h localhost {}".format(self.connection_details["psql_path"], self.connection_details["port"], self.connection_details["username"], self.connection_details["database_name"], command)
        else:
            command = "sudo -i -u postgres {} -p {} {}".format(self.connection_details["psql_path"], self.connection_details["port"], command)
        logging.debug(command)
        return os.popen(command)

    def get_cloud_and_instance_info(self):
        try:
            with open('/run/cloud-init/instance-data.json', 'r') as f:
                meta_data=json.load(f)
            cloud_name = meta_data['v1']['cloud-name']
            if cloud_name == 'aws':
                instance_type = meta_data['ds']['dynamic']['instance-identity']['document']['instanceType']
            elif cloud_name == 'azure':
                instance_type = meta_data['ds']['meta_data']['imds']['compute']['vmSize']
            else:
                instance_type = '-'
        except:
            instance_type = '-'
            cloud_name = '-'
        return cloud_name, instance_type
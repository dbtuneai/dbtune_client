import os
import time
import datetime
import subprocess
import sys
import platform
from threading import Timer, Condition, Thread
import logging
import psutil
import distro
import json
import numpy as np
from TuningError import TuningError
from adapters.adapter import Adapter
from adapters.utility import ensure_dir

LOG_LEVEL = os.getenv("PGTUNE_LOGGING", "NONE")
VERBOSE = "VERBOSE"

PG_CONFIG_UNITS = {
            "shared_buffers": "kB",
            "work_mem": "kB",
            "random_page_cost": "",
            "effective_io_concurrency": "",
            "max_wal_size": "kB",
            "max_parallel_workers_per_gather": "",
            "max_parallel_workers": "",
            "max_worker_processes": "",
            "checkpoint_completion_target": "",
            "checkpoint_timeout": "min",
            "bgwriter_lru_maxpages":"",
            "seq_page_cost":""
        }

def get_connect(pg_isready_path, port):
    p1 = subprocess.Popen([pg_isready_path, "-h", "localhost", "-p", str(port)],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    command_output, _ = p1.communicate()
    logging.debug(command_output.decode('ascii').strip())
    return p1.returncode

def wait_for_postgres_ready_for_connect(pg_isready_path, port):
    logging.info("UbuntuPgAdapter: Waiting for connect...")
    state = get_connect(pg_isready_path, port)
    while not state == 0 and not state == 2:
        print('.', end='', flush=True)
        time.sleep(1)
    return state

def shell_command(command):
    p1 = subprocess.Popen([command],stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    command_output, _ = p1.communicate()
    command_output = command_output.decode('ascii')
    return command_output


class UbuntuPgAdapter(Adapter):
    def __init__(self, adapter_data):
        self.pg_stats = None
        self.bestPointFound = None
        self.bestPerformance = None
        self.MODE = "pre-tuning"
        logging.info("Initiating PostgreSQL adapter")
        self.ALLOW_RESTART = adapter_data["restart_allowed"]
        self.OPTIMIZATION_OBJECTIVE = adapter_data["optimization_target"]
        self.PSQL_PATH = adapter_data["psql_path"]
        self.PG_ISREADY_PATH = adapter_data["pg_isready_path"]
        self.POSTGRES_RESTART_COMMAND = adapter_data["postgres_restart_command"]
        self.PG_PORT = adapter_data["port"]
        self.USERNAME = adapter_data["username"]
        self.PASSWORD_AUTH = adapter_data["password_auth"]
        self.DATABASE_NAME = adapter_data["database_name"]
        self.PG_STATS_STATEMENTS_ENABLE = False
        self.WARMUP_TIME = 300
        self.CRASH_DETECTED = False
        self.CRASH_DETECTION = Condition()
        self.EXPERIMENT_DURATION = 600
        self.DATA_DIRECTORY_PATH = os.path.dirname(self.psql("-At -c \"show data_directory\";").splitlines()[-1].strip())+"/"

        # Does the restart command work (only if they have restart enabled)
        if self.ALLOW_RESTART:
            modified_restart_command = self.POSTGRES_RESTART_COMMAND.replace("restart", "status")
            restart_command_status_worked = False
            i = 0
            while restart_command_status_worked == False:
                i += 1
                if i > 5:
                    self.safely_abort()
                try:
                    subprocess.check_output(f'{modified_restart_command}', shell=True)
                    restart_command_status_worked = True
                except:
                    logging.info(f"The command: {modified_restart_command} threw an error.")
                    self.POSTGRES_RESTART_COMMAND = input(f'Enter the command for restarting the postgres instance. [Default: systemctl restart postgresql]: ').strip() or "systemctl restart postgresql"
                    modified_restart_command = self.POSTGRES_RESTART_COMMAND.replace("restart", "status")
                     

        self.db_version = adapter_data["db_version"]
        if int(float(self.db_version)) > 12:
            self.pg_stat_col = "total_exec_time"
        else:
            self.pg_stat_col = "total_time"
        if {"WARMUP_TIME"} <= adapter_data.keys():
            self.WARMUP_TIME = adapter_data["WARMUP_TIME"]
        self.relative_os_paths()
        self.check_and_enable_pg_stat_statement()
        self.units = PG_CONFIG_UNITS
        logging.info("End of initiating PostgreSQL adapter\n")


    def relative_os_paths(self):
        self.CONF_PATH = os.path.dirname(self.psql("-At -c \"SHOW config_file\";").splitlines()[-1].strip())+"/"
        if not os.path.isdir(self.CONF_PATH):
            logging.error("Can't locate postgres main directory {}".format(self.CONF_PATH))
            raise

        self.CONF_OVERRIDE_PATH = self.CONF_PATH + "conf.d/"
        ensure_dir(self.CONF_OVERRIDE_PATH)
        self.CONF_OVERRIDE_FILE_PG_STATS = self.CONF_OVERRIDE_PATH + "99_dbtune_pg_stats.conf"

        self.CONF_OVERRIDE_FILE = self.CONF_OVERRIDE_PATH + "99_dbtune.conf"
        os.system('touch ' + self.CONF_OVERRIDE_FILE)

        self.BASE_CONF_FILE = self.CONF_PATH + "postgresql.conf"
        if not os.path.exists(self.BASE_CONF_FILE):
            logging.error("Can't locate postgresql.conf: {}".format(self.BASE_CONF_FILE))
            raise
        with open(self.BASE_CONF_FILE, "r+") as main_conf_file:
            include_line = "include_dir = 'conf.d'\n"
            for line in main_conf_file:
                if include_line in line:
                    break
            else:  # not found, we are at the eof
                main_conf_file.write(include_line)



    def get_xact_commit(self):
        output = self.psql("-At -c \"SELECT xact_commit FROM pg_stat_database WHERE datname='{}';\"".format(self.DATABASE_NAME))
        try:
            commit = int(output.splitlines()[-1].strip())
        except ValueError:
            logging.debug("Couldn't find anything in xaxt_commit, defaulting to 0")
            commit = 0
        return commit

    def calculate_query_latency(self, end_stats, start_stats):
        mean_exec_times_pr = []
        calls_pr = []
        total_calls_pr = 0
        #print("start_stats ", start_stats)
        for i in end_stats.keys():
            if i in start_stats.keys():
                exec_time_pr_pq = end_stats[i]['total_exec_time'] - start_stats[i]['total_exec_time']
                calls_pr_pq = end_stats[i]['calls'] - start_stats[i]['calls']
            elif i not in start_stats.keys():
                exec_time_pr_pq = end_stats[i]['total_exec_time'] - 0
                calls_pr_pq = end_stats[i]['calls'] - 0
            calls_pr.append(calls_pr_pq)
            #logging.info("calls_pr {}".format(calls_pr))
            #logging.info("calls_pr_pq {}".format(calls_pr_pq))
            if calls_pr_pq == 0:
                mean_exec_times_pr.append(0)
            else:
                mean_exec_time_pr_pq = exec_time_pr_pq / calls_pr_pq
                mean_exec_times_pr.append(mean_exec_time_pr_pq)
            total_calls_pr += calls_pr_pq
        #logging.info("mean_exec_times_pr {}".format(mean_exec_times_pr))
        latency = float(np.sum(np.divide(np.array(calls_pr), np.array(total_calls_pr)) * mean_exec_times_pr))
        return latency


    def get_pg_queries_statistics(self):
        query_runtime_stats = self.psql("-At -d {} -c \"SELECT JSON_OBJECT_AGG(queryid, JSON_BUILD_OBJECT('calls',calls,'total_exec_time',{})) FROM (SELECT * FROM pg_stat_statements ORDER BY calls DESC) AS f;\"".format(self.DATABASE_NAME, self.pg_stat_col)).splitlines()[-1]
        try:
            query_stats = json.loads(query_runtime_stats)
        except ValueError:
            logging.debug("Couldn't find anything in pg_stat_statments, defaulting to 0")
            query_stats = None
        return query_stats


    def wait_for_commits(self):
        start_commit = end_commit = self.get_xact_commit()
        logging.debug("Waiting for commits...")
        while end_commit - start_commit < 100:
            time.sleep(1)
            end_commit = self.get_xact_commit()
            

    def check_and_enable_pg_stat_statement(self):
        existsLines = self.psql("-At -c \"SELECT name, setting FROM pg_settings WHERE name LIKE 'shared_preload_libraries';\"").splitlines()[-1]
        exists = existsLines.split('|')[1]
        if "pg_stat_statements" not in exists:
            logging.debug("pg_stat_statements does not exist")
            while True:
                if self.OPTIMIZATION_OBJECTIVE == "query_runtime":
                    if self.ALLOW_RESTART:
                        response = "Y"
                    else:
                        response = input("To optimize for query_runtime database must be restarted at least once in order to enable pg_stat_statements. Would you like to continue the optimization? [Y] Restart once      [N] Abort optimization: ")
                elif self.OPTIMIZATION_OBJECTIVE == "throughput":
                    if self.ALLOW_RESTART:
                        response = "Y"
                    else:
                        response = input("Database must be restarted at least once in order to enable pg_stat_statements. Would you like to continue the optimization? [Y] Restart once      [N] Continue without query_runtime stats: ")
                if response not in ['Y','y','N','n']:
                    if self.OPTIMIZATION_OBJECTIVE == "query_runtime":
                        response = input("To optimize for query_runtime database must be restarted at least once in order to enable pg_stat_statements. Would you like to continue the optimization? [Y] Restart once      [N] Abort optimization: ")
                    elif self.OPTIMIZATION_OBJECTIVE == "throughput":
                        response = input("Database must be restarted at least once in order to enable pg_stat_statements. Would you like to continue the optimization? [Y] Restart once      [N] Continue without query_runtime stats: ")
                    continue
                else:
                    break
            if response in ["Y", "y"]:
                with open(self.CONF_OVERRIDE_FILE_PG_STATS, 'w') as pg_stat_file:
                    pg_stat_file.write("shared_preload_libraries = 'pg_stat_statements'")
                os.system(f"{self.POSTGRES_RESTART_COMMAND}")
            elif response in ["N", "n"]:
                if self.OPTIMIZATION_OBJECTIVE == "query_runtime":
                    logging.info("Restart not allowed, aborting optimization!")
                    sys.exit(0)
                else:
                    logging.info("Continuing Optimization without enabling pg_stat_statements extension")
                    return

            else:
                try:
                    with open(self.CONF_OVERRIDE_FILE_PG_STATS, 'w') as pg_stat_file:
                        pg_stat_file.write("shared_preload_libraries = 'pg_stat_statements'")
                except:
                    pass

        installed = int(self.psql("-At -d {} -c  \"SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements';\"".format(self.DATABASE_NAME)).splitlines()[-1])
        if not installed:
            logging.debug("Installing pg_stat_statements extension")
            self.psql("-At -d {} -c \"CREATE EXTENSION pg_stat_statements;\"".format(self.DATABASE_NAME))
        else:
            logging.debug("pg_stat_statements extension already installed")
            pass
        self.PG_STATS_STATEMENTS_ENABLE = True
        logging.debug("Resetting pg_stat_statements table")
        self.psql("-At -d {} -c \"SELECT pg_stat_statements_reset();\"".format(self.DATABASE_NAME))

    @staticmethod
    def s():
        return time.time()

    def restart(self):
        # We need to this here instead of in client since WARMUP_TIME is
        # handled here. We probably want to apply WARMUP_TIME even if we
        # don't restart.
        if self.ALLOW_RESTART:
            logging.info("UbuntuPgAdapter: Restarting Database")
            os.system(f"{self.POSTGRES_RESTART_COMMAND}")
            logging.info("UbuntuPgAdapter: Database restarted")
        else:
            logging.info("Skipping restart")
            self.psql("-At -c \"SELECT pg_reload_conf();\"")
        if wait_for_postgres_ready_for_connect(self.PG_ISREADY_PATH, self.PG_PORT) == 2:
            sys.exit("Unable to connect to postgres")
        else:
            #print("UbuntuPgAdapter: Sleeping for safety")
            #time.sleep(30)
            self.wait_for_commits()

    def get_metric_stats_monitoring(self, start_commit_monitoring, start_time_monitoring, start_query_stats_monitoring):
        stats = {}
        stats["db"] = {}
        physical_derive = shell_command('df '+ self.DATA_DIRECTORY_PATH).split('\n')[1].split()[0]
        physical_derive = os.path.basename(physical_derive)
        disk_stats_keys, disk_stats_values=shell_command('iostat -xc -y -p '+ physical_derive + ' 1 1').split('\n')[-5:-3]
        stats["io"] = dict(zip(disk_stats_keys.split(), disk_stats_values.replace(',','.').split()))
        stats["io"]["iops"] = float(stats["io"]['r/s'])+float(stats["io"]['w/s'])
        stats["mem"] = psutil.virtual_memory()._asdict()
        stats["cpu"] = {"cpu_util":psutil.cpu_percent()}
        stats["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        time.sleep(max(1-(time.time()-start_time_monitoring),0))
        try:
            end_commit = self.get_xact_commit()
            end_time = time.time()
            commits = end_commit-start_commit_monitoring
            if start_commit_monitoring <= 0 or end_commit <= 0 or commits < 0:
                commits = 0
            if self.PG_STATS_STATEMENTS_ENABLE:
                end_query_stats = self.get_pg_queries_statistics()
                if end_query_stats == None or start_query_stats_monitoring==None:
                    stats["db"]["query_runtime"] = 0
                else:
                    stats["db"]["query_runtime"] = self.calculate_query_latency(end_query_stats,start_query_stats_monitoring)
            stats["db"]["throughput"] = commits / (end_time-start_time_monitoring)
        except:
            stats["db"]["throughput"] = 0
            stats["db"]["query_runtime"] = 0
            end_query_stats = None
        return stats, end_commit, end_time, end_query_stats

    def get_metric_stats(self, state = "tuning" ):
        performance = {}
        if state=="monitoring":
            start_time = time.time()
            start_commit = self.get_xact_commit()
            if self.PG_STATS_STATEMENTS_ENABLE:
                start_query_stats = self.get_pg_queries_statistics()
            time.sleep(600)
        if state=="tuning":
            start_time_before_warmup = time.time()
            start_commit_before_warmup = self.get_xact_commit()
            if self.PG_STATS_STATEMENTS_ENABLE:
                start_query_stats_before_warmup = self.get_pg_queries_statistics()
            with self.CRASH_DETECTION:
                # start a new thread to check crash detection
                worker = Thread(args=(self.CRASH_DETECTION))
                worker.start()
                # wait to be notified
                if self.WARMUP_TIME and self.WARMUP_TIME > 0:
                    logging.info("UbuntuPgAdapter: Warming up the database for {}s after installing proposed configuration.".format(self.WARMUP_TIME))

                    # Early bad point detection
                    early_performance = {}
                    self.CRASH_DETECTION.wait(timeout=20)
                    start_time_early_exit = time.time()
                    start_commit_early_exit = self.get_xact_commit()
                    if self.PG_STATS_STATEMENTS_ENABLE:
                        start_query_stats_early_exit = self.get_pg_queries_statistics()
                    self.CRASH_DETECTION.wait(timeout=40)
                    worker.join()
                    end_commit_early_exit = self.get_xact_commit()
                    commits = end_commit_early_exit-start_commit_early_exit
                    if start_commit_early_exit <= 0 or end_commit_early_exit <= 0:
                        commits=0
                    end_time_early_exit = time.time()
                    early_performance["throughput"] = commits / (end_time_early_exit-start_time_early_exit)
                    early_performance["Valid"] = "true"
                    if self.PG_STATS_STATEMENTS_ENABLE:
                        end_query_stats_early_exit = self.get_pg_queries_statistics()
                        early_performance["query_runtime"] = self.calculate_query_latency(end_query_stats_early_exit, start_query_stats_early_exit)

                        if self.OPTIMIZATION_OBJECTIVE == "throughput":
                            if early_performance[self.OPTIMIZATION_OBJECTIVE] < self.bestPerformance * 0.4:
                                return early_performance

                        elif self.OPTIMIZATION_OBJECTIVE == "query_runtime":
                            if early_performance[self.OPTIMIZATION_OBJECTIVE] > self.bestPerformance * 1.6:
                                return early_performance

                        self.CRASH_DETECTION.wait(timeout=self.WARMUP_TIME-60)
                        if self.CRASH_DETECTED:
                            worker.join()
                            print("crash detected warmup phase")
                            end_commit_before_crash = self.get_xact_commit()
                            commits = end_commit_before_crash-start_commit_before_warmup
                            if start_commit_before_warmup <= 0 or end_commit_before_crash <= 0:
                                commits=0
                            end_time_before_crash = time.time()
                            performance["throughput"] = commits / (end_time_before_crash-start_time_before_warmup)
                            performance["Valid"] = "false"
                            if self.PG_STATS_STATEMENTS_ENABLE:
                                end_query_stats_before_crash = self.get_pg_queries_statistics()
                                performance["query_runtime"] = self.calculate_query_latency(end_query_stats_before_crash, start_query_stats_before_warmup)
                            self.revert_to_default_configuration()
                            self.restart()
                            self.CRASH_DETECTED = False
                            return performance
                    logging.info("Measuring database performance for {}s".format(self.EXPERIMENT_DURATION))
                    start_time = time.time()
                    start_commit = self.get_xact_commit()
                    if self.PG_STATS_STATEMENTS_ENABLE:
                        start_query_stats = self.get_pg_queries_statistics()
                    self.CRASH_DETECTION.wait(self.EXPERIMENT_DURATION)
                    if self.CRASH_DETECTED:
                        worker.join()
                        print("crash detected measurement phase")
                        end_commit_before_crash = self.get_xact_commit()
                        commits = end_commit_before_crash-start_commit_before_warmup
                        if start_commit_before_warmup <= 0 or end_commit_before_crash <= 0:
                            commits=0
                        end_time_before_crash = time.time()
                        performance["throughput"] = commits / (end_time_before_crash-start_time_before_warmup)
                        performance["Valid"] = "false"
                        if self.PG_STATS_STATEMENTS_ENABLE:
                            end_query_stats_before_crash = self.get_pg_queries_statistics()
                            performance["query_runtime"] = self.calculate_query_latency(end_query_stats_before_crash, start_query_stats_before_warmup)
                        self.revert_to_default_configuration()
                        self.restart()
                        self.CRASH_DETECTED = False
                        return performance

                worker.join()
        end_commit = self.get_xact_commit()
        commits = end_commit-start_commit
        if start_commit <= 0 or end_commit <= 0 or commits<0:
            commits=0
        end_time = time.time()
        performance["throughput"] = commits / (end_time-start_time)
        performance["Valid"] = "true"
        if self.PG_STATS_STATEMENTS_ENABLE:
            end_query_stats = self.get_pg_queries_statistics()
            performance["query_runtime"] = self.calculate_query_latency(end_query_stats, start_query_stats)
        return performance

    def get_default_configuration(self):
        default_configuration = {}
        number_of_knobs = len(PG_CONFIG_UNITS)
        default_settings = self.psql("-At -c \"SELECT name,setting,unit from pg_settings WHERE name in {}\";".format(tuple(PG_CONFIG_UNITS.keys()))).splitlines()[-number_of_knobs:]
        pg_units = {"B": 1/1024, "kB":1, "8kB": 8, "MB":1024, "GB":1024**2,"TB":1024**3, "s":1/60}
        for i in default_settings:
            name, setting, unit = map(lambda x: x.strip(),i.split('|'))
            try:
                default_configuration[name] = str(int(int(setting)*pg_units[unit]))
            except:
                default_configuration[name] = setting
        return default_configuration

    def update_config(self, tuning_request):
        logging.info("UbuntuPgAdapter: Installing proposed configuration")
        # write a temporary file to conf.d directory
        # 1. create conf.d directory if it not exists
        config_log_list = ["Proposed Configuration:"]
        ensure_dir(self.CONF_OVERRIDE_PATH)
        # 2. write the conf file in conf.d directory
        conf_file = open(self.CONF_OVERRIDE_FILE, "w")
        for tKey in tuning_request:
            t_value = tuning_request[tKey]
            conf_file.write(tKey + " = " + str(t_value) + self.units[tKey] + "\n")
            config_log_list.append(tKey + " = " + str(t_value) + self.units[tKey])
        conf_file.close()
        logging.debug('\n'.join(config_log_list))
        # 3. Make sure conf file is referenced at the end in the main conf file
        with open(self.BASE_CONF_FILE, "r+") as main_conf_file:
            include_line = "include_dir = 'conf.d'\n"
            for line in main_conf_file:
                if include_line in line:
                    break
            else:  # not found, we are at the eof
                main_conf_file.write(include_line)  # append missing data

    def abort_signal_handler(self, sig, frame, job=None):
        if job:
            self.abort_optimization(job=job)
        else:
            self.abort_optimization()

    def revert_to_default_configuration(self):
            if os.path.exists(self.CONF_OVERRIDE_FILE):
                try:
                    os.remove(self.CONF_OVERRIDE_FILE)
                except:
                    logging.warning(self.CONF_OVERRIDE_FILE + " does not exist.")

    def revert_to_default(self, job=None):
        if os.path.exists(self.CONF_OVERRIDE_FILE_PG_STATS) or os.path.exists(self.CONF_OVERRIDE_FILE):
            try:
                self.psql("-d {} -c \"DROP EXTENSION pg_stat_statements;\"".format(self.DATABASE_NAME))
                os.remove(self.CONF_OVERRIDE_FILE_PG_STATS)
            except:
                # print(self.CONF_OVERRIDE_FILE_PG_STATS + " does not exist.")
                pass
            try:
                os.remove(self.CONF_OVERRIDE_FILE)
            except:
                logging.warning(self.CONF_OVERRIDE_FILE + " does not exist.")
        if job:
            self.safely_abort(job)
        else:
            self.safely_abort()

    def pre_abort(self):
        if os.path.exists(self.CONF_OVERRIDE_FILE_PG_STATS):
            try:
                self.psql("-d {} -c \"DROP EXTENSION pg_stat_statements;\"".format(self.DATABASE_NAME))
                os.remove(self.CONF_OVERRIDE_FILE_PG_STATS)
            except:
                logging.warning(self.CONF_OVERRIDE_FILE_PG_STATS + " does not exist.")

    #best_config, default_config, selected_config
    def abort_optimization(self, abort_type = "terminal_interrupt", user_selected_configuration = None, job=None):
        if self.MODE == "pre-tuning":
            if abort_type in ["default_config", "best_config", "selected_config"]:
                self.pre_abort()
                self.safely_abort()
            elif abort_type == "terminal_interrupt":
                while True:
                    response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                    if response not in ['Y','y','N','n']:
                        response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                        continue
                    else:
                        break
                if response in ['Y','y']:
                    logging.info("Reverting back to default configuration!")
                    self.pre_abort()
                    self.safely_abort(job)
                elif response in ['N','n']:
                    logging.info("Resuming Optimization!")

        if self.MODE == "tuning":
            if abort_type == "default_config":
                self.revert_to_default()
            elif abort_type == "best_config":
                if self.bestPointFound:
                    logging.info("Installing the best found configuration!")
                    self.update_config(self.bestPointFound)
                    self.safely_abort()
                else:
                    logging.info("Best configuration not found, reverting back to default!")
                    self.revert_to_default()
            elif abort_type == "selected_config":
                if user_selected_configuration:
                    logging.info("Installing the user selected configuration!")
                    self.update_config(user_selected_configuration)
                    self.safely_abort()
                else:
                    logging.info("User selected configuration not found, reverting back to default!")
                    self.revert_to_default()
            elif abort_type == "terminal_interrupt":
                while True:
                    response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                    if response not in ['Y','y','N','n']:
                        response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                        continue
                    else:
                        break
                if response in ['Y','y']:
                    if self.bestPointFound:
                        while True:
                            next_response = input("[D] Revert back to default configuration    [I] Install best found configuration so far: ")
                            if next_response not in ['D','I']:
                                next_response = input("[D] Revert back to default configuration    [I] Install best found configuration so far: ")
                                continue
                            else:
                                break

                        if next_response == "D":
                            logging.info("Reverting back to default configuration!")
                            self.revert_to_default(job)

                        elif next_response == "I":
                            logging.info("Installing the best found configuration!")
                            self.update_config(self.bestPointFound)
                            self.safely_abort(job)
                    else:
                        logging.info("Reverting back to default configuration!")
                        self.revert_to_default(job)

                elif response in ['N','n']:
                    logging.info("Resuming Optimization!")


        if self.MODE == "post-tuning":
            if abort_type == "default_config":
                self.revert_to_default()
            elif abort_type == "best_config":
                if self.bestPointFound:
                    logging.info("Installing the best found configuration!")
                    self.update_config(self.bestPointFound)
                    self.safely_abort()
                else:
                    logging.info("Best configuration not found, reverting back to default!")
                    self.revert_to_default()
            elif abort_type == "selected_config":
                if user_selected_configuration:
                    logging.info("Installing the user selected configuration!")
                    self.update_config(user_selected_configuration)
                    self.safely_abort()
                else:
                    logging.info("User selected configuration not found, reverting back to default!")
                    self.revert_to_default()
            elif abort_type == "terminal_interrupt":
                while True:
                    response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                    if response not in ['Y','y','N','n']:
                        response = input("Do you want to abort the optimization?. [Y] Yes      [N] No: ")
                        continue
                    else:
                        break
                if response in ['Y','y']:
                    while True:
                        next_response = input("[D] Revert back to default configuration    [K] Keep the best found configuration: ")
                        if next_response not in ['D','K']:
                            next_response = input("[D] Revert back to default configuration    [K] Keep the best found configuration: ")
                            continue
                        else:
                            break
                    if next_response == "D":
                        logging.info("Reverting back to default configuration!")
                        self.revert_to_default(job)
                    elif next_response == "K":
                        logging.info("Keeping the best found configuration!")
                        self.safely_abort(job)

                elif response in ['N','n']:
                    logging.info("Resuming Optimization!")

    def safely_abort(self, job=None):
        if self.ALLOW_RESTART:
            logging.info("Restarting postgres")
            os.system(f"{self.POSTGRES_RESTART_COMMAND}")
        state = get_connect(self.PG_ISREADY_PATH, self.PG_PORT)
        while not state == 0:
            if state == 2:
                raise TuningError("FATAL ERROR: Could not restart postgresql after restoring config")

        logging.info("Tuning stopped. Configuration set. Postgres running.")
        if job == None:
            pass
        elif self.MODE == "post-tuning":
            job.update_tuning_status('completed')
        else:
            job.update_tuning_status('aborted')
        os._exit(0)

    def psql(self, command):
        if self.PASSWORD_AUTH:
            command = "{} -p {} -U {} -d {} -h localhost {}".format(self.PSQL_PATH, self.PG_PORT, self.USERNAME, self.DATABASE_NAME, command)
        else:
            command = "sudo -i -u postgres {} -p {} {}".format(self.PSQL_PATH, self.PG_PORT, command)
        p1 = subprocess.Popen([command],stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        command_output, _ = p1.communicate()
        command_output = command_output.decode('ascii')
        return command_output


def current_milli_time():
    return round(time.time() * 1000)

def get_timestamp():
    x = datetime.datetime.now()
    return x.strftime("%x %X")

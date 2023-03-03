import math
import time
import logging
import psutil
import datetime
import os
from _thread import interrupt_main
from threading import Thread
import subprocess


def shell_command(command):
    p1 = subprocess.Popen([command],stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    command_output, _ = p1.communicate()
    command_output = command_output.decode('ascii')
    return command_output

class Stats:
    def __init__(self, job, db):
        #Default config
        self.job = job
        self.thread = None
        self.db = db
        self.check_state = None
        self.abort_type = None
        self.user_selected_configuration = None
        self.stats = None
        self.pg_isready_path = self.db.PG_ISREADY_PATH
        self.pg_port = self.db.PG_PORT
        self.start_commit_monitoring = self.db.get_xact_commit()
        self.start_time_monitoring = time.time()
        if self.db.PG_STATS_STATEMENTS_ENABLE:
            self.start_query_stats_monitoring = self.db.get_pg_queries_statistics()

    def get_connect(self):
        self.pg_port
        p1 = subprocess.Popen([self.pg_isready_path, "-h", "localhost", "-p", str(self.pg_port)],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        command_output, _ = p1.communicate()
        logging.debug(command_output.decode('ascii').strip())
        return p1.returncode

    def get_timestamp(self):
        x = datetime.datetime.now()
        return x.strftime("%Y-%m-%d %H:%M:%S.%f")

    def run(self):
        if self.check_state == 'aborted':
            if self.user_selected_configuration:
                self.db.abort_optimization(abort_type=self.abort_type, user_selected_configuration=self.user_selected_configuration)
            else:
                self.db.abort_optimization(abort_type=self.abort_type)
        logging.debug("Stats running")
        try:
            self.stats, self.start_commit_monitoring, self.start_time_monitoring, self.start_query_stats_monitoring = self.db.get_metric_stats_monitoring(self.start_commit_monitoring, self.start_time_monitoring, self.start_query_stats_monitoring)
        except:
            time.sleep(0.1)
        self.thread = Thread(target=self.flush)
        self.thread.daemon = True
        self.thread.start()



    def abort(self):
        if self.thread is not None:
            self.thread.join()
            logging.debug("Stats aborted")

    def flush(self):
        try:
            logging.debug("Stats flushing")
            self.run()
            self.check_state_data = self.job.post_stats(self.stats)
            self.check_state = self.check_state_data["tuning_session_state"]
            self.abort_type = self.check_state_data["abort_tuning_type"]
            self.user_selected_configuration = self.check_state_data["applied_config_on_abort"]
            self.stats = None
        except:
            time.sleep(0.1)
            self.run()
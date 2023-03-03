import math
import time
import logging
import psutil
import os
from threading import Thread
import subprocess


class MemMonitoring:
    def __init__(self, db):
        #Default config
        self.thread = None
        self.db = db

    def run(self):
        memory = psutil.virtual_memory()._asdict()
        if float(memory["percent"]) > 90:
            self.db.CRASH_DETECTED = True
            with self.db.CRASH_DETECTION:
                self.db.CRASH_DETECTION.notify()

        time.sleep(0.1)
        self.thread = Thread(target=self.check_memory)
        self.thread.daemon = True
        self.thread.start()

    def abort(self):
        if self.thread is not None:
            self.thread.join()
            logging.debug("Crash detection aborted")

    def check_memory(self):
        logging.debug("Stats flushing")
        self.run()


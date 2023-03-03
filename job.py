import json
import time
import datetime
import logging
import requests
from requests.exceptions import HTTPError


class Job:
    def __init__(self, endpoint, api_key, job_id):
        logging.info("Starting tuning session {}".format(job_id))
        self.api_key = api_key
        self.job_id = job_id
        endpoint = endpoint + "job/"
        self.request = endpoint + self.job_id + '/request?v=2'
        self.response = endpoint + self.job_id + '/response?v=2'
        self.adapter = endpoint + self.job_id + '/adapter'
        self.stats = endpoint + self.job_id + "/stats"
        self.update_status = endpoint + self.job_id +"/update-status"
        self.default_performance = endpoint + self.job_id + "/default-performance"
        self.iteration = None

    def get_tuning_request(self):
        try:
            response = requests.get(self.request,
                                    headers={'X-HYPER-API-KEY': self.api_key}, )
            response.raise_for_status()
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6
        else:
            data = response.json()
            if "iteration_no" in data.keys():
                self.iteration = data["iteration_no"]
            logging.debug("Iteration {}".format(self.iteration))
            return data

    def update_tuning_status(self, state):
        try:
            response = requests.post(self.update_status,
                                 json={"tuning_status": state},
                                 headers={'X-HYPER-API-KEY': self.api_key}, )
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6

    def post_stats(self, stats_data):
        try:
            response = requests.post(self.stats,
                                 json=stats_data,
                                 headers={'X-HYPER-API-KEY': self.api_key}, )
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6
        else:
            data = response.json()
            return data

    def iterate(self, metric_stats, stats_data=None):
        try:
            response = requests.post(self.response, json={"metric_stats": metric_stats, "stats_data": stats_data, "iteration": self.iteration, "timestamp": get_timestamp()},
                                     headers={'X-HYPER-API-KEY': self.api_key}, )
            response.raise_for_status()
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
            logging.info("Backing off for 30 sec and trying to get the new request again...")
            time.sleep(30)
            return self.get_tuning_request()
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6
        else:
            data = response.json()
            if "iteration_no" in data.keys():
                self.iteration = data["iteration_no"]
            if "error" in data:
                raise Exception(data)
            return data

    def post_default_performance(self, default_performance, default_configuration):
        try:
            requests.post(self.default_performance, json={"default_performance": default_performance, "default_configuration":default_configuration, "default_configuration_timestamp":get_timestamp()}, headers={'X-HYPER-API-KEY': self.api_key}, )
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            logging.exception(f'Other error occurred: {err}')  # Python 3.6


def get_timestamp():
    x = datetime.datetime.now()
    return x.strftime("%Y-%m-%d %H:%M:%S.%f")
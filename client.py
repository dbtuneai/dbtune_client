import os
import sys
import time
import traceback
import config
import signal
import logging
from dbms_adapter_factory import AdapterFactory
from dbms_connector_factory import ConnectorFactory
from job import Job
from connect import Connect
from stats.stats import Stats
from crash_detection import MemMonitoring
API_ENDPOINT = config.ENDPOINT
if "DBTUNE_ENDPOINT" in os.environ:
    API_ENDPOINT = os.environ["DBTUNE_ENDPOINT"]

def establish_database_connection(api_key, db_id):
    connect = Connect(API_ENDPOINT, api_key, db_id)
    # is there an active session against this db_id?
    database_instance = connect.get_database_instance()
    connection = ConnectorFactory.get_connector(database_instance)
    connection_details = connection.connection_details
    if database_instance["db_connection_status"] == 't':
        pass
    else: # This block will run when the client runs for the first time
        client_info = connection.get_client_info()
        connect.post_client_info(client_info)
    logging.info("Waiting for tuning session to start...")
    timeout = time.time() + 60*5
    while True:
        print('.', end='', flush=True)
        tuning_session_response = connect.get_tuning_session_id()
        if tuning_session_response["status"] or time.time() > timeout:
            job_id = tuning_session_response["tuning_session"]["tuning_session_id"]
            tuning_session = tuning_session_response["tuning_session"]
            break
        time.sleep(1)
    print("\n")
    tuning_session["engine"] = database_instance["engine"]
    tuning_session["db_version"] = client_info["DBVERSION"]
    tuning_session = {**tuning_session, **connection_details}
    job = Job(API_ENDPOINT, api_key, job_id)
    db = AdapterFactory.get_adapter(tuning_session)
    signal.signal(signal.SIGINT, lambda sig, frame: db.abort_signal_handler(sig, frame, job))
    experiment_duration = 600
    logging.info("Monitoring default performance for {}s".format(experiment_duration))
    stats = Stats(job, db)
    stats.run()
    mem_monitoring = MemMonitoring(db)
    mem_monitoring.run()
    if tuning_session["default_performance"] == None:
        default_performance = db.get_metric_stats(state="monitoring")
        db.bestPerformance = default_performance[db.OPTIMIZATION_OBJECTIVE]
        default_performance["Valid"] = "true"
        default_configuration = db.get_default_configuration()
        job.post_default_performance(default_performance, default_configuration)

    tuning_request = job.get_tuning_request()
    while "endOfJob" not in tuning_request:
        knobs = tuning_request["knobs"]
        db.bestPointFound = tuning_request["best_found_configuration"][db.OPTIMIZATION_OBJECTIVE]
        db.bestPerformance = tuning_request["best_found_configuration"]["performance"][db.OPTIMIZATION_OBJECTIVE]
        state = stats.check_state
        iteration = tuning_request["iteration_no"]
        if state == "Tuning":
            db.MODE = "tuning"
            logging.info("Starting Iteration {}".format(iteration))
            db.update_config(knobs)
            db.restart()
            performance_metrics = db.get_metric_stats()
            logging.info("Iteration {} completed!\n".format(iteration))
            tuning_request = job.iterate(performance_metrics)

    logging.info("Tuning session is over")
    db.MODE = "post-tuning"
    if "bestPointFound" in tuning_request:
        bestPointFound = tuning_request["bestPointFound"][db.OPTIMIZATION_OBJECTIVE]
        if tuning_request["default"] == bestPointFound:
            logging.info("Installing the default configuration. Unable to find better configuration.")
            db.pre_abort()
            db.restart()
        else:
            logging.info("Installing the best found configuration!")
            db.update_config(bestPointFound)
            db.restart()
        # Monitoring after installing the best found configuration for 30 mins and then safely aborting the optimization.
        job.update_tuning_status('completed')
        logging.disable(logging.DEBUG)
        logging.info("Monitoring after installing the best configuration!")
        time.sleep(600)
        stats.abort()
        mem_monitoring.abort()
        db.safely_abort(job)
    else:
        logging.error("Error: Couldn't apply best point found")

    while not state == "completed":
        time.sleep(600)
        tuning_request = job.get_tuning_request()
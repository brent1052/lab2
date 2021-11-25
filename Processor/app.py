import connexion
from connexion import NoContent
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import logging.config
import yaml
import requests
import json
import datetime
import os

# Try to use externalized configuration, if it's not present, use local configuration
try:
    with open('/config/app_conf.yaml', 'r') as f:
        app_config = yaml.safe_load(f.read())
except IOError:
    with open('app_conf.yaml', 'r') as f:
        app_config = yaml.safe_load(f.read())

with open('log_conf.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Constant
STORE_SERVICE_ASSIGN_RESIDENCE_URL = app_config['eventstore']['url'] + "/rental/immediate"
STORE_SERVICE_SCHEDULE_RESIDENCE_URL = app_config['eventstore']['url'] + "/rental/schedule"
HEADERS = {"content-type": "application/json"}
SUCCESS_RESPONSE_CODE = 200
DATA_STORE_QUERY_PARAMETERS = "?startDate={}&endDate={}"

def get_residential_request_stats():
    """ Retrieves events stats for immediate and scheduled requests for residential rental requests """

    logger.info("GET Request:  Residential Rental requests started")

    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename']) as json_data_file:
            residential_request_stats = json.load(json_data_file)
            logger.debug("Contents of Residential Request Stats: {}".format(residential_request_stats))
            logger.debug("GET Request: Residential Rental requests completed")
            return residential_request_stats, 200


    logger.error(app_config['datastore']['filename'] + " doesn't exist.  No file to read previous stats from.")
    return NoContent, 404

def populate_stats():
    """ Periodically update stats """

    logger.info("Start Periodic Processing")

    json_data = {
        "num_immediate_requests": 0,
        "num_scheduled_requests": 0,
        "total_requests": 0,
        "updated_timestamp": "2016-08-29T09:12:33.001Z"
    }

    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename']) as json_data_file:
            json_data = json.load(json_data_file)

    current_datetime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')


    last_read_datetime = json_data["updated_timestamp"]

    new_immediate_events_response = requests.get(STORE_SERVICE_ASSIGN_RESIDENCE_URL + \
                                                      DATA_STORE_QUERY_PARAMETERS.format(last_read_datetime,
                                                                                        current_datetime),
                                                      headers=HEADERS)

    new_scheduled_events_response = requests.get(STORE_SERVICE_SCHEDULE_RESIDENCE_URL + \
                                                      DATA_STORE_QUERY_PARAMETERS.format(last_read_datetime,
                                                                                        current_datetime),
                                                      headers=HEADERS)

    if new_immediate_events_response.status_code == SUCCESS_RESPONSE_CODE:
        logger.info("Number of new immediate rental events: {}".format(len(new_immediate_events_response.json())))
    else:
        logger.error("Did not receive status code: {}".format(SUCCESS_RESPONSE_CODE))

    if new_scheduled_events_response.status_code == SUCCESS_RESPONSE_CODE:
        logger.info("Number of new scheduled rental events: {}".format(len(new_scheduled_events_response.json())))
    else:
        logger.error("Did not receive status code: {}".format(SUCCESS_RESPONSE_CODE))

    if json_data["num_immediate_requests"] != 0:
        json_data["num_immediate_requests"] += len(new_immediate_events_response.json())
    else:
        json_data["num_immediate_requests"] = len(new_immediate_events_response.json())

    if json_data["num_scheduled_requests"] != 0:
        json_data["num_scheduled_requests"] += len(new_scheduled_events_response.json())
    else:
        json_data["num_scheduled_requests"] = len(new_scheduled_events_response.json())

    if json_data["total_requests"] != 0:
        json_data["total_requests"] += len(new_immediate_events_response.json()) + len(new_scheduled_events_response.json())
    else:
        json_data["total_requests"] = json_data["num_immediate_requests"] + json_data["num_scheduled_requests"]

    json_data["updated_timestamp"] = current_datetime

    with open(app_config['datastore']['filename'], "w") as json_data_file:
        json.dump(json_data, json_data_file)
        logger.debug("Residential Request Stats: {}".format(json_data))

    logger.info("End Periodic Processing")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml")

if __name__ == "__main__":
    # run our standalone gevent server
    init_scheduler()
    app.run(port=8100, use_reloader=False)
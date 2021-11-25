import connexion
from datetime import datetime
import yaml
import json
import logging.config

from pykafka import KafkaClient
from flask_cors import CORS

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

# Get Logger and DB
logger = logging.getLogger('basicLogger')

# Utility function to convert a datetime object to a datetime string in a specified format
def get_dt_object(date_string):
    return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')

def get_most_common_immediate_residential_request():
    """ Return the most common immediate residential request between a date range """

    logger.info("Begin auditing for most common immediate residential type")

    residential_types_count = {}

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['hostname'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    consumer = topic.get_simple_consumer(consumer_group="immediate_requests",
                                         auto_commit_enable=True,
                                         auto_commit_interval_ms=1000,
                                         consumer_timeout_ms=1000,
                                         reset_offset_on_start=True)

    for message in consumer:
        if message is not None:
            msg_str = message.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.debug("Kafka message consumed: {}".format(msg))
            if msg["type"] == "residence_details":
                residence_type = msg["payload"]["residence_type"]
                if residence_type not in residential_types_count.keys():
                    residential_types_count[residence_type] = 1
                else:
                    residential_types_count[residence_type] += 1

    highest_count = 0
    popular_residence_type = ""
    for residence_type, count in residential_types_count.items():
        if highest_count < count:
            highest_count = count
            popular_residence_type = residence_type

    return_dict = {"most_common_immediate_residential_request": popular_residence_type}

    logger.info("End auditing for most common immediate residential type")

    return return_dict, 200

def get_most_common_scheduled_residential_request(startDate, endDate):
    """ Return the most common scheduled residential request between a date range """

    logger.info("Begin auditing for most common scheduled residential type, between".format(startDate, endDate))

    residential_types_count = {}
    startDateObj = get_dt_object(startDate)
    endDateObj = get_dt_object(endDate)

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['hostname'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]

    consumer = topic.get_simple_consumer(consumer_group="scheduled_requests",
                                         auto_commit_enable=True,
                                         auto_commit_interval_ms=1000,
                                         consumer_timeout_ms=1000,
                                         reset_offset_on_start=True)

    for message in consumer:
        if message is not None:
            msg_str = message.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.debug("Kafka message consumed: {}".format(msg))
            if msg["type"] == "future_residence_details":
                if startDateObj <= get_dt_object(msg["payload"]["date"]) <= endDateObj:
                    residence_type = msg["payload"]["residence_type"]
                    if residence_type not in residential_types_count.keys():
                        residential_types_count[residence_type] = 1
                    else:
                        residential_types_count[residence_type] += 1

    highest_count = 0
    popular_residence_type = ""
    for residence_type, count in residential_types_count.items():
        if highest_count < count:
            highest_count = count
            popular_residence_type = residence_type

    return_dict = {"most_common_scheduled_residential_request": popular_residence_type}

    logger.info("End auditing for most common scheduled residential type")

    return return_dict, 200

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml")

if __name__ == "__main__":
    app.run(port=8110)
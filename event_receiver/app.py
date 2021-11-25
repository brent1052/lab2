import connexion
import datetime
import yaml
import json
import logging.config

from pykafka import KafkaClient
from connexion import NoContent
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

def assign_residence(residenceDetails):
    """ Receives a request to fulfill an immediate residence rental """

    logger.info("Request received to assign immediate residence")
    logger.debug("Request details: {}".format(residenceDetails))

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['hostname'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    producer = topic.get_sync_producer()

    msg = {"type": "residence_details",
           "datetime":
               datetime.datetime.now().strftime(
                   "%Y-%m-%dT%H:%M:%S"),
           "payload": residenceDetails}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info("Request sent!")

    return NoContent, 200

def schedule_residence(residenceDetails):
    """ Receives a request to fulfill a schedule residence rental """

    logger.info("Request received to schedule residence")
    logger.debug("Request details: {}".format(residenceDetails))

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['hostname'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    producer = topic.get_sync_producer()

    msg = {"type": "future_residence_details",
           "datetime":
               datetime.datetime.now().strftime(
                   "%Y-%m-%dT%H:%M:%S"),
           "payload": residenceDetails}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info("Request sent!")

    return NoContent, 200

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml")

if __name__ == "__main__":
    app.run(port=8080)
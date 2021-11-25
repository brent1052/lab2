import connexion
from datetime import datetime
import logging.config
from threading import Thread

import yaml
import json

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from base import Base
from residence_details import ResidenceDetails
from future_residence_details import FutureResidenceDetails

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
DB_ENGINE = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(app_config['datastore']['user'],
                                                                  app_config['datastore']['password'],
                                                                  app_config['datastore']['hostname'],
                                                                  app_config['datastore']['port'],
                                                                  app_config['datastore']['db']))
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
    logger.info("Start Message Processing")

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['hostname'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    consumer = topic.get_simple_consumer(consumer_group="message_processor",
                                         auto_commit_enable=True,
                                         auto_commit_interval_ms=500,
                                         reset_offset_on_start=False)

    for message in consumer:
        if message is not None:
            msg_str = message.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.debug("Message Received: {}".format(msg))
            # Receives a request to assign an available residence immediately
            if msg["type"] == "residence_details":
                residenceDetails = msg["payload"]
                session = DB_SESSION()

                rd = ResidenceDetails(residenceDetails['user_id'],
                                      residenceDetails['user_name'],
                                      residenceDetails['residence_type'],
                                      residenceDetails['phone_num'],
                                      residenceDetails['capacity'])

                session.add(rd)

                session.commit()
                session.close()
                logger.info("Immediate Residential Request added to database: {}".format(residenceDetails))

            # Receives a request to assign an available residence at a future date
            elif msg["type"] == "future_residence_details":
                residenceDetails = msg["payload"]
                session = DB_SESSION()

                frd = FutureResidenceDetails(residenceDetails['user_id'],
                                             residenceDetails['user_name'],
                                             residenceDetails['residence_type'],
                                             residenceDetails['phone_num'],
                                             residenceDetails['capacity'],
                                             residenceDetails['date'])

                session.add(frd)

                session.commit()
                session.close()
                logger.info("Scheduled Residential Request added to database: {}".format(residenceDetails))

    logger.info("End Message Processing")

def get_immediate_residence(startDate, endDate):
    """ Get immediate residence rental requests from data store made between a date range """

    logger.info("Query database for immediate residential requetss between: ".format(startDate, endDate))

    results_list = []

    session = DB_SESSION()

    results = (session.query(ResidenceDetails)
         .filter(and_(ResidenceDetails.date_created >= datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%S.%fZ'),
                      ResidenceDetails.date_created <= datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%S.%fZ'))))

    for result in results:
        results_list.append(result.to_dict())
        logger.debug("Request: ", result.to_dict())

    session.close()

    return results_list, 200

def get_schedule_residence(startDate, endDate):
    """ Get future residence rental requests from data store made between a date range """

    logger.info("Query database for immediate residential requests between: ".format(startDate, endDate))

    results_list = []

    session = DB_SESSION()

    results = (session.query(FutureResidenceDetails)
         .filter(and_(FutureResidenceDetails.date_created >= datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%S.%fZ'),
                      FutureResidenceDetails.date_created <= datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%S.%fZ'))))

    for result in results:
        results_list.append(result.to_dict())
        logger.debug("Request: ", result.to_dict())

    session.close()

    return results_list, 200

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml")

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
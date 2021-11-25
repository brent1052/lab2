import mysql.connector
import yaml

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conn = mysql.connector.connect(host=app_config['datastore']['hostname'], user=app_config['datastore']['user'],
                                  password=app_config['datastore']['password'], database=app_config['datastore']['db'])

db_cursor = db_conn.cursor()

db_cursor.execute('''
          CREATE TABLE residence_details
          (id INT NOT NULL AUTO_INCREMENT, 
           user_id VARCHAR(250) NOT NULL,
           user_name VARCHAR(250) NOT NULL,
           residence_type VARCHAR(100) NOT NULL,
           phone_num VARCHAR(100) NOT NULL,
           capacity INTEGER NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT residence_details_pk PRIMARY KEY (id))
          ''')

db_cursor.execute('''
          CREATE TABLE future_residence_details
           (id INT NOT NULL AUTO_INCREMENT, 
           user_id VARCHAR(250) NOT NULL,
           user_name VARCHAR(250) NOT NULL,
           residence_type VARCHAR(100) NOT NULL,
           phone_num VARCHAR(100) NOT NULL,
           capacity INTEGER NOT NULL,
           date VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT future_residence_details_pk PRIMARY KEY (id))
          ''')

db_conn.commit()
db_conn.close()
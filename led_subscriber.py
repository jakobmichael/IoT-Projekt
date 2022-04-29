import paho.mqtt.client as mqttClient
import time
from datetime import datetime
from RPi import GPIO
import sqlite3 as sql

# database connections and queries
database_connection = sql.connect('iot_project.db', check_same_thread=False)

db_cursor = database_connection.cursor()
db_cursor.execute("DROP TABLE ACCESS_PROTOCOL")

ACCESS_PROTOCOL_TABLE = "ACCESS_PROTOCOL"
UID_TABLE = "VALID_UIDS"

database_connection.commit()

sql_access_insertion_query = ''' INSERT INTO ACCESS_PROTOCOL(valid,access_time)
              VALUES(?,?,?) '''
sql_uid_insertion_query = ''' INSERT INTO VALID_UIDS(rfid_uid,holder)
              VALUES(?,?,?) '''

sql_uid_search_query = ''' SELECT rfid_uid FROM VALID_UIDS WHERE rfid_uid = ?'''


def create_access_table(connection):
    with connection:
        connection.execute("""
            CREATE TABLE ACCESS_PROTOCOL (
                access_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                valid TEXT,
                access_time DATETIME
            );
        """)


def create_valid_uid_table(connection):
    with connection:
        connection.execute("""
            CREATE TABLE VALID_UIDS (
                uid_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                rfid_uid TEXT,
                holder DATETIME
            );
        """)


def query_table(connection, table):
    with connection:
        data = connection.execute("SELECT * FROM " + table)
        for row in data:
            print(row)


def insert_into_uid_database(uid, holder):
    with database_connection:
        database_connection.execute(sql_access_insertion_query, (uid, holder))

    database_connection.commit()


def insert_access_into_database():
    dt = datetime.now()
    with database_connection:
        database_connection.execute(sql_access_insertion_query, ("valid", dt))

    database_connection.commit()


# mqtt callbacks
MQTT_TOPICS = {
    "motion": "motion",
    "rfid": "rfid"
}

authentication_attempt = False


def on_connect(client, userdata, flags, rc):
    print("Connecting to broker")


def on_message(client, userdata, message):

    if message.topic == MQTT_TOPICS["motion"]:
        message_as_string = message.payload.decode('utf-8')
        print("Motion detected: " + message_as_string)

        insert_access_into_database()

    if message.topic == MQTT_TOPICS["rfid"]:
        message_as_string = message.payload.decode('utf-8')
        print("RFID authentication attempted with UID: " + message_as_string)

        print(database_connection.execute(
            sql_uid_search_query, (message_as_string,)))


# main program
MQTT_ADDRESS = input("Please specify broker IP address: ")


CLIENT = mqttClient.Client("Python")
CLIENT.on_connect = on_connect
CLIENT.connect(MQTT_ADDRESS)
CLIENT.subscribe(MQTT_TOPICS["motion"])
CLIENT.subscribe(MQTT_TOPICS["rfid"])
CLIENT.loop_start()

# create tables, if not already created
create_access_table(database_connection)
create_valid_uid_table(database_connection)

try:
    CLIENT.on_message = on_message
    while True:
        time.sleep(0)

except KeyboardInterrupt:
    query_table(database_connection, ACCESS_PROTOCOL_TABLE)
    database_connection.close()
    print("exiting")
    CLIENT.disconnect()
    CLIENT.loop_stop()

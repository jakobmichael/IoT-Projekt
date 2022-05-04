import paho.mqtt.client as mqttClient
import time
from datetime import datetime
from RPi import GPIO
import sqlite3 as sql

# set pins for status led
RED_LED=10
GREEN_LED=18

GPIO.setmode(GPIO.BOARD)
GPIO.setup(RED_LED, GPIO.OUT)
GPIO.setmode(GPIO.BOARD)
GPIO.setup(GREEN_LED, GPIO.OUT)

GPIO.output(RED_LED, True)

# database connections and queries
database_connection = sql.connect('iot_project.db', check_same_thread=False)

db_cursor = database_connection.cursor()



ACCESS_PROTOCOL_TABLE = "ACCESS_PROTOCOL"
UID_TABLE = "VALID_UIDS"

database_connection.commit()

sql_access_insertion_query = ''' INSERT INTO ACCESS_PROTOCOL(valid,uid,holder,access_time)
              VALUES(?,?,?,?) '''




def create_access_table(connection):
    with connection:
        connection.execute("""
            CREATE TABLE ACCESS_PROTOCOL (
                access_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                valid TEXT,
                uid TEXT,
                holder TEXT,
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





def insert_access_into_database(uid, holder, validity):
    dt = datetime.now()
    with database_connection:
        database_connection.execute(sql_access_insertion_query, (validity,uid,holder,dt))

    database_connection.commit()

    


# mqtt callbacks
MQTT_TOPICS = {
    "motion": "motion",
    "rfid": "/id"
}

fraud = False


def on_connect(client, userdata, flags, rc):
    print("Connecting to broker")


def on_message(client, userdata, message):
    
    global fraud
    
    if message.topic == MQTT_TOPICS["motion"]:
        message_as_string = message.payload.decode('utf-8')
        print("Motion detected: " + message_as_string)
        GPIO.output(RED_LED, False)
        fraud = True

    if message.topic == MQTT_TOPICS["rfid"]:
        if fraud:
            GPIO.output(RED_LED, True)
            
        messages_decoded = message.payload
        messages = messages_decoded.split(";")
    
        insert_access_into_database("valid",messages[0].decode('utf-8'),messages[1].decode('utf-8').rstrip())
        
        print("RFID authentication attempted with UID: " + messages[0].decode('utf-8')+ " from user " + messages[1].decode('utf-8').rstrip())
        time.sleep(5)
 


# main program
MQTT_ADDRESS = str(raw_input("Please specify broker IP address: "))

CLIENT = mqttClient.Client("Python")
CLIENT.on_connect = on_connect
CLIENT.connect(MQTT_ADDRESS)
CLIENT.subscribe(MQTT_TOPICS["motion"])
CLIENT.subscribe(MQTT_TOPICS["rfid"])
CLIENT.loop_start()

# create tables, if not already created
create_access_table(database_connection)


try:
    CLIENT.on_message = on_message
    while True:
        time.sleep(0)

except KeyboardInterrupt:
    query_table(database_connection, ACCESS_PROTOCOL_TABLE)
    db_cursor.execute("DROP TABLE ACCESS_PROTOCOL")
    database_connection.close()
    print("exiting")
    CLIENT.disconnect()
    CLIENT.loop_stop()
    GPIO.cleanup()

import paho.mqtt.client as mqttClient
import time
from datetime import datetime
import sqlite3 as sql

database_connection = sql.connect('iot_project.db',check_same_thread=False)
db_cursor = database_connection.cursor()
db_cursor.execute("DROP TABLE ACCESS_PROTOCOL")

database_connection.commit()


sql_access_insertion_query = ''' INSERT INTO ACCESS_PROTOCOL(valid,access_time)
              VALUES(?,?,?) '''
              
sql_uid_insertion_query = ''' INSERT INTO VALID_UIDS(rfid_uid,holder)
              VALUES(?,?,?) '''



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
        
        
def query_table(connection):
    with connection:
        data = connection.execute("SELECT * FROM ACCESS_PROTOCOL")
        for row in data:
            print(row)
     
     
def insert_into_uid_database(uid,holder):
    dt = datetime.now()
    with database_connection:
        database_connection.execute(sql_access_insertion_query, (uid, holder))
    
    database_connection.commit()       

def insert_access_into_database():
    dt = datetime.now()
    with database_connection:
        database_connection.execute(sql_access_insertion_query, ("valid", dt))
    
    database_connection.commit()


def on_connect(client, userdata, flags, rc):
    print("Connecting to broker")
  
def on_message(client, userdata, message):
    
    message_as_string = message.payload.decode('utf-8')
    print("Message received: "  + message_as_string)
    
    insert_access_into_database()
    
    
    


MQTT_ADDRESS= input("Please specify broker IP address: ")               

MQTT_TOPIC = "motion"
  
CLIENT = mqttClient.Client("Python")               
CLIENT.on_connect= on_connect                      
  
CLIENT.connect(MQTT_ADDRESS)          
CLIENT.subscribe(MQTT_TOPIC)
CLIENT.loop_start()        

create_access_table(database_connection)

try:
    CLIENT.on_message= on_message                      
    while True:
        time.sleep(0)
  
except KeyboardInterrupt:
    query_table(database_connection)
    database_connection.close()
    print("exiting")
    CLIENT.disconnect()
    CLIENT.loop_stop()
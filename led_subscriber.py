import paho.mqtt.client as mqttClient
import time
from datetime import datetime
import sqlite3 as sql

PROTOCOL_DATABASE = "ACCESS_PROTOCOL" 

database_connection = sql.connect('access_protocol.db',check_same_thread=False)
db_cursor = database_connection.cursor()
db_cursor.execute("DROP TABLE ACCESS_PROTOCOL")

database_connection.commit()

access_id = 1

sql_insertion_query = ''' INSERT INTO ACCESS_PROTOCOL(access_id,valid,access_time)
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
        
        
def query_table(connection):
    with connection:
        data = connection.execute("SELECT * FROM ACCESS_PROTOCOL")
        print(data)
        for row in data:
            print(row)
            

def insert_access_into_database(access_id):
    dt = datetime.now()
    with database_connection:
        database_connection.execute(sql_insertion_query, (access_id,"valid", dt))
    
    database_connection.commit()


def on_connect(client, userdata, flags, rc):
    print("Connecting to broker")
  
def on_message(client, userdata, message):
    global access_id
    
    message_as_string = message.payload.decode('utf-8')
    print("Message received: "  + message_as_string)
    
    insert_access_into_database(access_id)
    
    access_id += 1
    
    

  

  
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
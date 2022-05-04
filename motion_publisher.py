#Import der Python Module
import RPi.GPIO as GPIO
import time
from paho.mqtt import client as MQTT



CLIENT = MQTT.Client("Motion_Sensor")
MQTT_ADDRESS = str(raw_input("Please specify broker IP-address: "))
print("Connecting to  " + MQTT_ADDRESS)

CLIENT.connect(MQTT_ADDRESS)

PIR_SENSOR_PIN = 23

GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_SENSOR_PIN, GPIO.IN)


print("Starting motion detection...")

def Bewegung(PIR_SENSOR_PIN):
    print("Dein Electreeks Sensor hat eine Bewegung erkannt!")
    CLIENT.publish(topic = "motion", payload = 1)

    time.sleep(5)


#start main program
if __name__ == "__main__":
    try: 
        
        #call function, if motion is detected
        GPIO.add_event_detect(PIR_SENSOR_PIN, GPIO.RISING, callback=Bewegung)        
            
        while True:
            time.sleep(0)
        
    except KeyboardInterrupt:
        print ("Motion detection ceased.")
        GPIO.cleanup()
        
        

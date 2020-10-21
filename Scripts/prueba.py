import paho.mqtt.client as mqtt
import pandas as pd

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("device/r")

def on_message(client, userdata, msg):
  data = str(msg.payload.decode())
  data = pd.DataFrame(data, columns=['ID','TAG'])
  print(data)
  #data = data.to_csv (r'/Users/rafael/Documents/Proyectos/BioInventario\IDTAGS.csv', index = None) 
  # if msg.payload.decode() == "Hello world!":
  #   print("Yes!")
  # else:
  #   print("No!")
  #   #client.disconnect()
    
client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import os.path
import csv

def Convert(string): 
    li = list(string.split(",")) 
    return li

def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("device/r")

def on_message(client, userdata, msg):
	if os.path.isfile("readers.csv")==False: 
		d = msg.payload.decode()
		d=Convert(d)
		z = np.array(d)
		data = pd.DataFrame(z)
		data = data.T
		data = data.rename(columns={0:"ID",1:"TAG"})
		print(data)
		data.to_csv(r'readers.csv', index = False)
	else: #os.path.isfile("readers.csv")==True:
		dat = pd.read_csv("readers.csv")
		w = msg.payload.decode()
		w=Convert(w)
		y = np.array(w)
		data1 = pd.DataFrame(y)
		data1 = data1.T
		data1 = data1.rename(columns={0:"ID",1:"TAG"})
		u=dat.append(data1, ignore_index=True)
		print(u)
		u.to_csv(r'readers.csv', index = False)
		
client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
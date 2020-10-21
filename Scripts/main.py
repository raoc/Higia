import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
#import os.path
#import csv

def Convert(string): 
    li = list(string.split(",")) 
    return li

def search_reader(yy):
    data=pd.read_csv("readers.csv")
    r = data['ID'].tolist()
    yy = int(yy)
    if yy in r:
        print('Ahi esta')
    else:
        print('Ahi no esta')

def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("device/r")

def on_message(client, userdata, msg):
    d = msg.payload.decode()
    d=Convert(d)
    z = np.array(d)
    data1 = pd.DataFrame(z)
    data1 = data1.T
    data1 = data1.rename(columns={0:"ID",1:"TAG"}) 
    #tag=data1['TAG']
    xx = np.array(data1['ID'])
    xx = xx[0]
    #print(xx)
    search_reader(xx)

client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
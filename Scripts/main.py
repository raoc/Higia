import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import os.path
#import csv

def Convert(string): 
    li = list(string.split(",")) 
    return li

def search_reader(yy):
    yy = int(yy)
    if os.path.isfile("readersl.csv")==False: 
        print('Digite la ubicación del nuevo lector SERIAL: ', yy)
        loc=input()
        tucu=[yy, loc]
        data = pd.DataFrame(tucu)
        data = data.T
        data = data.rename(columns={0:"SERIAL",1:"LOC"})
        print(data)
        data.to_csv(r'readersl.csv', index = False)
    elif os.path.isfile("readersl.csv")==True:
        dat = pd.read_csv("readersl.csv")
        r = dat['SERIAL'].tolist()
        if yy in r:
            print('Ahi esta')
        else:
            print('Digite la ubicación del nuevo lector: ', yy)
            loc=input()
        tucu=[yy, loc]
        data1 = pd.DataFrame(tucu)
        data1 = data1.T
        data1 = data1.rename(columns={0:"SERIAL",1:"LOC"})
        u=dat.append(data1, ignore_index=True)
        print(u)
        u.to_csv(r'readersl.csv', index = False)


def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("device/r")

def on_message(client, userdata, msg):
    d = msg.payload.decode()
    d=Convert(d)
    z = np.array(d)
    data1 = pd.DataFrame(z)
    data1 = data1.T
    data1 = data1.rename(columns={0:"SERIAL",1:"TAG"}) 
    #tag=data1['TAG']
    xx = np.array(data1['SERIAL'])
    xx = xx[0]
    #print(xx)
    search_reader(xx)

client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
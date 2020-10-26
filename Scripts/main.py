import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import os.path
#import csv

def Convert(string): 
    li = list(string.split(",")) 
    return li

def search_mdevice(yy):
    yy = int(yy)
    if os.path.isfile("mdevice.csv")==False: 
        print('Digite el nombre del nuevo dispositivo medico con TAG: ', yy)
        loc=input()
        tucu=[yy, loc]
        data = pd.DataFrame(tucu)
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE"})
        print(data)
        data.to_csv(r'mdevice.csv', index = False)
    elif os.path.isfile("mdevice.csv")==True:
        dat = pd.read_csv("mdevice.csv")
        r = dat['TAG'].tolist()
        if yy in r:
            print('Ahi esta')
        else:
            print('Digite el nombre del nuevo dispositivo medico con TAG: ', yy)
            loc=input()
        tucu=[yy, loc]
        data1 = pd.DataFrame(tucu)
        data1 = data1.T
        data1 = data1.rename(columns={0:"TAG",1:"MDEVICE"})
        u=dat.append(data1, ignore_index=True)
        print(u)
        u.to_csv(r'mdevice.csv', index = False)


def search_reader(yy, zz):
    zz=int(zz)
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
           search_mdevice(zz)
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
    xx = np.array(data1['SERIAL'])
    xx = xx[0]
    tag = np.array(data1['TAG'])
    tag = tag[0]
    search_reader(xx, tag)
client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)
#client.connect("192.168.1.11",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
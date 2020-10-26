import paho.mqtt.client as mqtt #Librería utilizada para establecer comunicación mediante protocolo MQTT.
import pandas as pd             #Librería que contiene paquete de herramientas para manipulación de datos. 
import numpy as np              #Librería que contiene paquete de herramientas para operacines matriciales.
import os.path                  #Librería utilizada para poder obtener las rutas y caracteristicas del Sistema Operativo.

'''Esta función convierte en String los valores numericos envíados a ella'''
def Convert(string): 
    li = list(string.split(",")) 
    return li

''' Esta Funcion es la encargada de crear la base de datos de dispositivos medicos'''
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

''' Esta Funcion es la encargada de crear la base de datos de dispositivos lectores RFID'''
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

''' Esta Funcion es la encargada de suscribir la aplicación en el tópico device/r dentro del blocker'''
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("device/r")

''' Esta Funcion es la encargada de recibir los mensajes que envían los rectores al tópico device/r dentro del blocker.
Tambien envía dicha información a las funciones de registro de lectores y dispositivos medicos'''
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


client = mqtt.Client() #Utilizamos la case cliente para crear la instancia mqtt
#client.connect("broker.hivemq.com",1883,60) #Esta instrucción configura los parametros de conexión al brocker
client.connect("192.168.1.11",1883,60)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
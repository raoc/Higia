import paho.mqtt.client as mqtt #Librería utilizada para establecer comunicación mediante protocolo MQTT.
import pandas as pd             #Librería que contiene paquete de herramientas para manipulación de datos. 
import numpy as np              #Librería que contiene paquete de herramientas para operacines matriciales.
import os.path                  #Librería utilizada para poder obtener las rutas y caracteristicas del Sistema Operativo.
import os
import datetime

'''Esta función convierte en String los valores numericos envíados a ella'''
def Convert(string): 
    li = list(string.split(",")) 
    return li

def display(u):
    if os.path.isfile('display.csv')==False:
        data = pd.DataFrame(u) 
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        os.system('clear')
        print(data)
        data.to_csv(r'display.csv', index = False)
    elif os.path.isfile("display.csv")==True:       
        dat = pd.read_csv("display.csv")
        h = np.array(u["TAG"])
        j = np.array(dat["TAG"])
        if h in j:
            os.system('clear')
            i=list(dat[dat['TAG']==u['TAG']].index)
            k =  dat.drop(dat.index[i]).reset_index().drop(columns='index')
            print(k)
            k.to_csv(r'display.csv', index = False)
        else:
            data1 = pd.DataFrame(u)
            data1 = data1.T
            data1 = data1.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
            k = pd.DataFrame(pd.concat([data1, dat])).reset_index().drop(columns='index')
            os.system('clear')
            print(k)
            k.to_csv(r'display.csv', index = False)

''' Esta Funcion es la encargada guardar historial de movimientos de los equipos medicos'''
def save_history(tag, medevice, location):
    yy = int(tag)
    if os.path.isfile("hdevice.csv")==False: 
        time = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
        tucu=[yy, medevice, location, time]
        data = pd.DataFrame(tucu)
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        os.system('clear')
        display(data.iloc[0])
        data.to_csv(r'hdevice.csv', index = False)
    elif os.path.isfile("hdevice.csv")==True:        
        dat = pd.read_csv("hdevice.csv")
        time = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
        tucu = [yy, medevice, location, time]
        data1 = pd.DataFrame(tucu)
        data1 = data1.T
        data1 = data1.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        u = pd.DataFrame(pd.concat([data1, dat])).reset_index().drop(columns='index')
        os.system('clear')
        display(u.iloc[0])
        #print(u)
        u.to_csv(r'hdevice.csv', index = False)    

''' Esta Funcion es la encargada de crear la base de datos de dispositivos medicos'''
def search_mdevice(yy, location):
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
        t = dat.where(dat['TAG']==yy).dropna()
        t1 = t['MDEVICE'].tolist()
        if yy in r:
            save_history(yy, t1[0], location)
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
        t = dat.where(dat['SERIAL']==yy).dropna()
        t1 = t['LOC'].tolist()
        if yy in r:
           search_mdevice(zz, t1[0])
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
	client.subscribe("TrabajodegradoMIAM")
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
client.connect("broker.hivemq.com",1883,60) #Esta instrucción configura los parametros de conexión al brocker
#client.connect("192.168.1.11",1883,60)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
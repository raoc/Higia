import paho.mqtt.client as mqtt #Librería utilizada para establecer comunicación mediante protocolo MQTT.
import pandas as pd             #Librería que contiene paquete de herramientas para manipulación de datos. 
import numpy as np              #Librería que contiene paquete de herramientas para operacines matriciales.
import os.path                  #Librería utilizada para poder obtener las rutas y caracteristicas del Sistema Operativo.
import os                       
import datetime                 #Libreria utilizada para realizar los TIMESTAMPS.

'''Esta función convierte en String los valores numericos envíados a ella'''
def Convert(string): 
    li = list(string.split(",")) 
    return li

'''Esta función sera la que generara el archivo de localizacion principal
   y tendra el trabajo de presentar en pantalla la evolucion de dicho archivo
   en tiempo real'''
def display(data_gen):
    if os.path.isfile('display.csv')==False:
        data = pd.DataFrame(data_gen) 
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        os.system('clear')
        print(data)
        data.to_csv(r'display.csv', index = False)
    elif os.path.isfile("display.csv")==True:       
        dat = pd.read_csv("display.csv")
        tags_gen = np.array(data_gen["TAG"])
        tags_act = np.array(dat["TAG"])
        if tags_gen in tags_act:
            os.system('clear')
            index = list(dat[dat['TAG']==data_gen['TAG']].index)
            data_act =  dat.drop(dat.index[index]).reset_index().drop(columns='index')
            print(data_act)
            data_act.to_csv(r'display.csv', index = False)
        else:
            data_proc = pd.DataFrame(data_gen)
            data_proc = data_proc.T
            data_proc = data_proc.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
            data_act = pd.DataFrame(pd.concat([data_proc, dat])).reset_index().drop(columns='index')
            os.system('clear')
            print(data_act)
            data_act.to_csv(r'display.csv', index = False)

''' Esta Funcion es la encargada guardar historial de movimientos de los equipos medicos'''
def save_history(tag, medevice, location):
    tag = int(tag)
    if os.path.isfile("hdevice.csv")==False: 
        time = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
        table_data=[tag, medevice, location, time]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        os.system('clear')
        display(data.iloc[0])
        data.to_csv(r'hdevice.csv', index = False)
    elif os.path.isfile("hdevice.csv")==True:        
        dat = pd.read_csv("hdevice.csv")
        time = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
        table_data = [tag, medevice, location, time]
        data_proc = pd.DataFrame(table_data)
        data_proc = data_proc.T
        data_proc = data_proc.rename(columns={0:"TAG",1:"MDEVICE",2:"LOCATION",3:"IN_TIME"})
        data_gen = pd.DataFrame(pd.concat([data_proc, dat])).reset_index().drop(columns='index')
        os.system('clear')
        display(data_gen.iloc[0])
        #print(data_gen)
        data_gen.to_csv(r'hdevice.csv', index = False)    

''' Esta Funcion es la encargada de crear la base de datos de dispositivos medicos'''
def search_mdevice(tag, location):
    tag = int(tag)
    if os.path.isfile("mdevice.csv")==False: 
        print('Digite el nombre del nuevo dispositivo medico con TAG: ', tag)
        loc=input()
        table_data=[tag, loc]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(columns={0:"TAG",1:"MDEVICE"})
        print(data)
        data.to_csv(r'mdevice.csv', index = False)
    elif os.path.isfile("mdevice.csv")==True:
        dat = pd.read_csv("mdevice.csv")
        tag_list = dat['TAG'].tolist()
        tag_finder = dat.where(dat['TAG']==tag).dropna()
        dev_finder = tag_finder['MDEVICE'].tolist()
        if tag in tag_list:
            save_history(tag, dev_finder[0], location)
        else:
            print('Digite el nombre del nuevo dispositivo medico con TAG: ', tag)
            loc=input()
            table_data=[tag, loc]
            data_proc = pd.DataFrame(table_data)
            data_proc = data_proc.T
            data_proc = data_proc.rename(columns={0:"TAG",1:"MDEVICE"})
            data_gen=dat.append(data_proc, ignore_index=True)
            print(data_gen)
            data_gen.to_csv(r'mdevice.csv', index = False)

''' Esta Funcion es la encargada de crear la base de datos de dispositivos lectores RFID'''
def search_reader(ids, tag):
    tag = int(tag)
    ids = int(ids)
    if os.path.isfile("readersl.csv")==False: 
        print('Digite la ubicación del nuevo lector SERIAL: ', ids)
        loc = input()
        table_data = [ids, loc]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(columns={0:"SERIAL",1:"LOC"})
        print(data)
        data.to_csv(r'readersl.csv', index = False)
    elif os.path.isfile("readersl.csv")==True:
        dat = pd.read_csv("readersl.csv")
        ids_list = dat['SERIAL'].tolist()
        ids_finder = dat.where(dat['SERIAL']==ids).dropna()
        loc_finder = ids_finder['LOC'].tolist()
        if ids in ids_list:
           search_mdevice(tag, loc_finder[0])
        else:
            print('Digite la ubicación del nuevo lector: ', ids)
            loc=input()
            table_data=[ids, loc]
            data_proc = pd.DataFrame(table_data)
            data_proc = data_proc.T
            data_proc = data_proc.rename(columns={0:"SERIAL",1:"LOC"})
            data_gen=dat.append(data_proc, ignore_index=True)
            print(data_gen)
            data_gen.to_csv(r'readersl.csv', index = False)

''' Esta Funcion es la encargada de suscribir la aplicación en el tópico device/r dentro del blocker'''
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("TrabajodegradoMIAM")
''' Esta Funcion es la encargada de recibir los mensajes que envían los rectores al tópico device/r dentro del blocker.
Tambien envía dicha información a las funciones de registro de lectores y dispositivos medicos'''
def on_message(client, userdata, msg):
    read_broq = msg.payload.decode()
    read_broq = Convert(read_broq)
    read_array = np.array(read_broq)
    data_proc = pd.DataFrame(read_array)
    data_proc = data_proc.T
    data_proc = data_proc.rename(columns={0:"SERIAL",1:"TAG"}) 
    ids = np.array(data_proc['SERIAL'])
    ids = ids[0]
    tag = np.array(data_proc['TAG'])
    tag = tag[0]
    search_reader(ids, tag)


client = mqtt.Client() #Utilizamos la case cliente para crear la instancia mqtt
client.connect("broker.hivemq.com",1883,60) #Esta instrucción configura los parametros de conexión al brocker
#client.connect("192.168.1.11",1883,60)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
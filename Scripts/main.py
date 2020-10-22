<<<<<<< HEAD
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import os.path
=======
#### SE IMPORTAN LAS LIBRERIAS######
import paho.mqtt.client as mqtt #mqtt BROKER conection
import pandas as pd #Manipulacion de los datos
import numpy as np #Operacion de arreglos numericos
import os.path #Acceder a rutas del sistema operativo
####################################
>>>>>>> d1ac49ff9e14e7bfa6e0acb339533c03866a1f1a


############FUNCION PARA CONVERSION DE DATOS DE LECTURA EN STRING#########
def Convert(string): 
    li = list(string.split(",")) 
    return li
##########################################################################



########CREACION DEL DOCUMENTO CSV A PARTIR DE LOS DATOS DE LECTURA#########
def search_reader(yy):
<<<<<<< HEAD
    yy = int(yy)
    if os.path.isfile("readersl.csv")==False: 
        print('Digite la ubicación del nuevo lector SERIAL: ', yy)
=======
    yy = int(yy) #Se lee el valor de lectura(serial) y se convierte en una variable de tipo int
    if os.path.isfile("readersl.csv")==False: #Condicional "Si el archivo csv no existe, se crea y se registra el prime serial"
        print('Digite la ubicación del nuevo lector ID: ', yy)
>>>>>>> d1ac49ff9e14e7bfa6e0acb339533c03866a1f1a
        loc=input()
        tucu=[yy, loc]
        data = pd.DataFrame(tucu)
        data = data.T
        data = data.rename(columns={0:"SERIAL",1:"LOC"})
        print(data)
        data.to_csv(r'readersl.csv', index = False)
    elif os.path.isfile("readersl.csv")==True:	#Condicional "Si el archivo existe, se revisan los seriales que hay 
        dat = pd.read_csv("readersl.csv")		#Si el serial ya esta registrado imprime 'Ahi esta', si no esta registrado
        r = dat['ID'].tolist()					# Se registra y se actualiza el archivo csv"
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
##########################################################################


############Conexion con el Broker################
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("device/r")
###################################################


##############Lectura del mensaje#################
def on_message(client, userdata, msg):
    d = msg.payload.decode()
    d=Convert(d)
    z = np.array(d)
    data1 = pd.DataFrame(z)
    data1 = data1.T
<<<<<<< HEAD
    data1 = data1.rename(columns={0:"SERIAL",1:"TAG"}) 
    #tag=data1['TAG']
=======
    data1 = data1.rename(columns={0:"SERIAL",1:"TAG"}) 
>>>>>>> d1ac49ff9e14e7bfa6e0acb339533c03866a1f1a
    xx = np.array(data1['SERIAL'])
    xx = xx[0]
    search_reader(xx)
##################################################


###configuracion del protocolo, conexion con el server######
client = mqtt.Client()
client.connect("broker.hivemq.com",1883,60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
############################################################
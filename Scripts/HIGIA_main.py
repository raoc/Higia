import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import os.path
import os
import datetime

# This function converts the numerical values sent to it into String.
def Convert(string):
    li = list(string.split(","))
    return li


# This function will be the one that will generate the main localization file
# and it will have the job of displaying on screen the update of this file
# in real time
def display(data_gen):

    if os.path.isfile("display.csv") == False:
        data = pd.DataFrame(data_gen)
        data = data.T
        data = data.rename(
            columns={0: "TAG", 1: "MDEVICE", 2: "LOCATION", 3: "IN_TIME"}
        )
        os.system("clear")
        print(data)
        data.to_csv(r"display.csv", index=False)

    elif os.path.isfile("display.csv") == True:
        dat = pd.read_csv("display.csv")
        tags_gen = np.array(data_gen["TAG"])
        tags_act = np.array(dat["TAG"])

        if tags_gen in tags_act:
            os.system("clear")
            index = list(dat[dat["TAG"] == data_gen["TAG"]].index)
            data_act = dat.drop(dat.index[index]).reset_index().drop(columns="index")
            print(data_act)
            data_act.to_csv(r"display.csv", index=False)

        else:
            data_proc = pd.DataFrame(data_gen)
            data_proc = data_proc.T
            data_proc = data_proc.rename(
                columns={0: "TAG", 1: "MDEVICE", 2: "LOCATION", 3: "IN_TIME"}
            )
            data_act = (
                pd.DataFrame(pd.concat([data_proc, dat]))
                .reset_index()
                .drop(columns="index")
            )
            os.system("clear")
            print(data_act)
            data_act.to_csv(r"display.csv", index=False)


# This function is in charge of storing the movement history of the devices.
def save_history(tag, medevice, location):
    tag = int(tag)

    if os.path.isfile("hdevice.csv") == False:
        time = "{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())
        table_data = [tag, medevice, location, time]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(
            columns={0: "TAG", 1: "MDEVICE", 2: "LOCATION", 3: "IN_TIME"}
        )
        os.system("clear")
        display(data.iloc[0])
        data.to_csv(r"hdevice.csv", index=False)

    elif os.path.isfile("hdevice.csv") == True:
        dat = pd.read_csv("hdevice.csv")
        time = "{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())
        table_data = [tag, medevice, location, time]
        data_proc = pd.DataFrame(table_data)
        data_proc = data_proc.T
        data_proc = data_proc.rename(
            columns={0: "TAG", 1: "MDEVICE", 2: "LOCATION", 3: "IN_TIME"}
        )
        data_gen = (
            pd.DataFrame(pd.concat([data_proc, dat]))
            .reset_index()
            .drop(columns="index")
        )
        os.system("clear")
        display(data_gen.iloc[0])
        data_gen.to_csv(r"hdevice.csv", index=False)


# This function is in charge of creating the devices database.
def search_mdevice(tag, location):
    tag = int(tag)

    if os.path.isfile("mdevice.csv") == False:
        print("Digite el nombre del nuevo dispositivo medico con TAG: ", tag)
        loc = input()
        table_data = [tag, loc]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(columns={0: "TAG", 1: "MDEVICE"})
        print(data)
        data.to_csv(r"mdevice.csv", index=False)

    elif os.path.isfile("mdevice.csv") == True:
        dat = pd.read_csv("mdevice.csv")
        tag_list = dat["TAG"].tolist()
        tag_finder = dat.where(dat["TAG"] == tag).dropna()
        dev_finder = tag_finder["MDEVICE"].tolist()

        if tag in tag_list:
            save_history(tag, dev_finder[0], location)

        else:
            print("Digite el nombre del nuevo dispositivo medico con TAG: ", tag)
            loc = input()
            table_data = [tag, loc]
            data_proc = pd.DataFrame(table_data)
            data_proc = data_proc.T
            data_proc = data_proc.rename(columns={0: "TAG", 1: "MDEVICE"})
            data_gen = dat.append(data_proc, ignore_index=True)
            print(data_gen)
            data_gen.to_csv(r"mdevice.csv", index=False)


# This function is in charge of creating the database of RFID reader devices.
def search_reader(ids, tag):
    tag = int(tag)
    ids = int(ids)

    if os.path.isfile("readersl.csv") == False:
        print("Digite la ubicación del nuevo lector SERIAL: ", ids)
        loc = input()
        table_data = [ids, loc]
        data = pd.DataFrame(table_data)
        data = data.T
        data = data.rename(columns={0: "SERIAL", 1: "LOC"})
        print(data)
        data.to_csv(r"readersl.csv", index=False)

    elif os.path.isfile("readersl.csv") == True:
        dat = pd.read_csv("readersl.csv")
        ids_list = dat["SERIAL"].tolist()
        ids_finder = dat.where(dat["SERIAL"] == ids).dropna()
        loc_finder = ids_finder["LOC"].tolist()

        if ids in ids_list:
            search_mdevice(tag, loc_finder[0])

        else:
            print("Digite la ubicación del nuevo lector: ", ids)
            loc = input()
            table_data = [ids, loc]
            data_proc = pd.DataFrame(table_data)
            data_proc = data_proc.T
            data_proc = data_proc.rename(columns={0: "SERIAL", 1: "LOC"})
            data_gen = dat.append(data_proc, ignore_index=True)
            print(data_gen)
            data_gen.to_csv(r"readersl.csv", index=False)


# This function is in charge of subscribing the application to the device/r topic inside the broker.
def on_connect(client, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("*TOPIC NAME*")


# This function is in charge of receiving the messages sent by the rectors to the device/r topic inside the broker.
# It also sends this information to the readers and devices registration functions.
def on_message(msg):
    read_broq = msg.payload.decode()
    read_broq = Convert(read_broq)
    read_array = np.array(read_broq)
    data_proc = pd.DataFrame(read_array)
    data_proc = data_proc.T
    data_proc = data_proc.rename(columns={0: "SERIAL", 1: "TAG"})
    ids = np.array(data_proc["SERIAL"])
    ids = ids[0]
    tag = np.array(data_proc["TAG"])
    tag = tag[0]
    search_reader(ids, tag)


client = mqtt.Client()
client.connect("your.IP", 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()

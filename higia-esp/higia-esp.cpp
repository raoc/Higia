/*
  Este boceto lee un tag RFID con un lector RDM6300 y lo publica atravez de el protocolo MQTT 
  en un tópico de su preferencia acompañado de un tag 'S' definido por uster
*/

#include <ESP8266WiFi.h>// ESP
#include <PubSubClient.h>//MQTT
#include <rdm6300.h>//  LECTOR RFID
#include <WiFiManager.h>
#include <Ticker.h>

String data = "", S = "00003";
//=================LED=======================//

Ticker ticker;
#ifndef LEDD
#define LEDD 01
#endif

int LED = LEDD;
void tick()
{
  //toggle state
  digitalWrite(LED, !digitalRead(LED));     // set pin to the opposite state
}

//==================================================//

const char* mqtt_server = "broker.hivemq.com";
WiFiClient espClient;
PubSubClient client(espClient);

long lastMsg = 0;
char msg[50];
int value = 0;

void callback(char* topic, byte* payload, unsigned int length) {
  /*
    FUNCION PARA RECIBIR PAYLOAD POR MQTT
  */
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Attempting MQTT connection...");
    String clientId = "ESP8266Client-";
    clientId += String(random(0xffff), HEX);
    if (client.connect(clientId.c_str())) {
      client.publish("topico-preferencia", (S+",0").c_str());
      client.subscribe("topico-preferencia");
    } else {
      Serial.print("failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 5 seconds");
      delay(5000);
    }
  }
}

//============================================//

//=================RFID=======================//
bool state = true;
int cont = 0;
#define RDM6300_RX_PIN 02
#define RST_pin 03

Rdm6300 rdm6300;
//============================================//
WiFiManager wifiManager;
void setup()
{
  pinMode(RST_pin, INPUT);
  pinMode(LED, OUTPUT);
  ticker.attach(0.6, tick);
  wifiManager.autoConnect(S.c_str());//crea AP wi-fi para la config de red
  ticker.detach();
  digitalWrite(LED, LOW);
  //wifiManager.resetSettings();
  rdm6300.begin(RDM6300_RX_PIN);
  client.setServer(mqtt_server, 1883);
  client.publish("topico-preferencia", (S + ",0").c_str());
  client.setCallback(callback);
}

void loop()
{
  client.loop();
  if (!client.connected()) {
    reconnect();
  }
  delay(10);
  if (rdm6300.update()) {
    pub(rdm6300.get_tag_id());
  }
  delay(10);
  if (digitalRead(RST_pin) == LOW) {
    while (1) {
      client.publish("topico-preferencia", "RST");
      wifiManager.resetSettings();
      delay(2000);
      ESP.reset();
    }
  }
  delay(10);
}

void pub(long tag) {
  data = "";
  data = S + "," + String(tag);
  client.publish("topico-preferencia", data.c_str());
  ticker.detach();
}
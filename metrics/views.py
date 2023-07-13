from django.shortcuts import render
import sqlite3, time
import paho.mqtt.client as mqtt

DB_FILE = 'db.sqlite3'
MQTT_BROKER = 'mqtt.eclipseprojects.io'
MQTT_PORT = 1883
MQTT_TOPIC = 'metrics'

def create_table():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS mqtt_events
                 (timestamp INTEGER, topic TEXT, payload TEXT)''')
    conn.commit()
    conn.close()

def insert_mqtt_event(timestamp, topic, payload):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    print(payload, type(payload[2]))
    c.execute("INSERT INTO mqtt_events VALUES (?, ?, ?)", (timestamp, topic, payload))
    conn.commit()
    conn.close()

def metrics_view(request):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # c.execute("SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 10")
    # metrics = c.fetchall()
    c.execute("SELECT * FROM mqtt_events ORDER BY timestamp DESC LIMIT 10")
    mqtt_events = c.fetchall()
    conn.close()
    context = {
        # 'metrics': metrics,
        'mqtt_events': mqtt_events
    }
    return render(request, 'metrics.html', context)

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(MQTT_TOPIC)
    print("subscribed")

def on_message(client, userdata, msg):
    print("on connect")
    timestamp = int(time.time())
    print(msg.topic, msg.payload.decode())
    insert_mqtt_event(timestamp, msg.topic, msg.payload.decode())

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
print("here to")
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

create_table()

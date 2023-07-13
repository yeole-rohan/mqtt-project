import psutil
import time
import sqlite3
import paho.mqtt.client as mqtt

DB_FILE = 'metrics.db'
MQTT_BROKER = 'mqtt.eclipseprojects.io'
MQTT_PORT = 1883
MQTT_TOPIC = 'metrics'

def create_table():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS metrics
                 (timestamp INTEGER, cpu_percent REAL, total_memory INTEGER,
                 available_memory INTEGER, total_disk INTEGER, available_disk INTEGER)''')
    conn.commit()
    conn.close()

def insert_metrics(timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?)",
              (timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk))
    conn.commit()
    conn.close()

def collect_metrics():
    cpu_percent = psutil.cpu_percent(interval=None)
    memory = psutil.virtual_memory()
    total_memory = memory.total
    available_memory = memory.available
    disk = psutil.disk_usage('/')
    total_disk = disk.total
    available_disk = disk.free
    timestamp = int(time.time())
    return timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk

def publish_metrics(client, timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk):
    payload = {
        'timestamp': timestamp,
        'cpu_percent': cpu_percent,
        'total_memory': total_memory,
        'available_memory': available_memory,
        'total_disk': total_disk,
        'available_disk': available_disk
    }
    client.publish(MQTT_TOPIC, payload=str(payload))

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code " + str(rc))

def main():
    create_table()
    
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    while True:
        timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk = collect_metrics()
        insert_metrics(timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk)
        
        # Check if 10 minutes have passed
        if timestamp % 60 == 0:
            publish_metrics(mqtt_client, timestamp, cpu_percent, total_memory, available_memory, total_disk, available_disk)
        
        time.sleep(2)  # Sleep for 2 Sec

if __name__ == '__main__':
    main()
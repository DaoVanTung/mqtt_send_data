
import datetime
import json
import random
import paho.mqtt.client as mqtt
import schedule
import time
import pandas as pd
import numpy as np

file_path = './data.csv'
data = pd.read_csv(file_path)

broker_address = "14.232.240.250"
port = 1883
username = "mkdc"
password = "Tecotec@MKDC#2023"
topic = "python"


# Hàm callback khi kết nối thành công
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
    else:
        print("Connection failed with code", rc)

# Hàm callback khi gửi thông điệp thành công
def on_publish(client, userdata, mid):
    # print("Message published")
    pass

client = mqtt.Client()  # Không cần tham số trong hàm tạo
client.username_pw_set(username, password)  # Thiết lập username và password
client.on_connect = on_connect
client.on_publish = on_publish

client.connect(broker_address, port=port)
client.loop_start()

interval_minutes = 10
total_minutes = 24 * 60 
num_intervals = total_minutes // interval_minutes

# Hàm để gửi dữ liệu
def send_data():
    # Lấy thời gian hiện tại
    current_time = datetime.datetime.now()
    unix_time = current_time.timestamp()

    # Iterate through each row in the dataframe
    for index, row in data.iterrows():
        rowData = row.values.tolist()
        value = 0
        if isinstance(row.iloc[2], (int, float)):
            value = random.uniform(0, row.iloc[2] / num_intervals + 20)
        
        if np.isnan(value):
            value = random.uniform(0, 150)

        message = {
            'diem_khai_thac_id': row.iloc[0],
            'ma_cam_bien': row.iloc[1],
            'gia_tri': round(value, 2),
            'thoi_gian': int(unix_time),
        }

        client.publish(topic, json.dumps(message))

# Đặt lịch gửi dữ liệu 3p
schedule.every(10).seconds.do(send_data)

print("Scheduler started. Sending data every hour.")
while True:
    schedule.run_pending()
    time.sleep(1)

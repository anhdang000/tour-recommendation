# Import thư viện Kafka, json và os
from base64 import encode
from textwrap import indent
from unittest import removeResult
from kafka import KafkaConsumer
import json
import os
from time import sleep
# from log_processor import *

# Danh sách tên các topic trên kafka-server (K8s)
topic_name_list = ["log-user", "log-tour", "log-hotel", "log-flight", "log-social"]

# Current Path
current_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_path)

# create folder if not exist
if not os.path.exists('logs'):
    os.makedirs('logs')
os.chdir(os.path.join(current_path, 'logs'))

# Kêt nối với kafka-server(k8s)
consumer = KafkaConsumer (
    bootstrap_servers = '10.10.11.237:9094,10.10.11.238:9094,10.10.11.239:9094'
    , group_id= 'log_collector_ai_da_hung'
    , auto_offset_reset = 'earliest'
    #, enable_auto_commit=True
    #security_protocol =  'SASL_PLAINTEXT',
    #sasl_mechanism = 'SCRAM-SHA-512',
    #sasl_plain_username='admin-hahalolo',
    #sasl_plain_password='Hahalolo@2021'
    )

# Hàm lấy thông tin từ topic (hứng topic từ K8s) và lưu vào file
def consume_logs(topic_name, num_rows):

    # Kiểm tra file log có tồn tại hay chưa, nếu có rồi thì xóa đi
    if os.path.exists(topic_name + '.log'):
        os.remove(topic_name + '.log')

    # Hứng sự kiện topic trên kafka-server
    consumer.subscribe([topic_name])
    count = 1

    for message in consumer:
        # chuyển message.value thành json
        print(message.value)
        msg = json.loads(message.value.decode("utf-8")) 
        # xét điều kiện log từ API với dạng LOG-REQ-RESP
        if "LOG-REQ-RESP" in msg['log']:
            # result = process_log(msg["log"]) # msg["log"] là kiểu str, xử lý log
            # lưu log vào file
            with open(f'{topic_name}.json', mode='a', encoding='utf-8') as f: # mở file và ghi vào
                # f.write(result + '\n\n')
                # f.write(json.dumps(result, indent=4) + '\n\n')
                f.write(json.dumps(msg["log"], indent=4) + '\n\n')
            # Có điều kiện số dòng vì lượng log lớn nên không thể lấy hết  
            if count == num_rows: break
            count += 1
  

    # os.system("cls")
    print("Logs collected!\n===========================================================================================")

# # Chạy hàm main
if __name__== "__main__":
        os.system("cls")
        # xét điều kiện xem có lấy log từ topic nào không
        while True:
            topic = input("Choose your topic:\n\t1. log-user\n\t2. log-tour\n\t3. log-hotel\n\t4. log-flight\nType 1, 2, 3, 4 or all: ")
            num_rows = int(input("Enter number of rows: "))
            if topic in ["1", "2", "3", "4"]: 
                consume_logs(topic_name_list[int(topic)-1], num_rows)
                # Nếu không lấy nữa thì thoát
                condition = input("Do you want to continue? (y/n): ")
                if condition == "y": 
                    os.system("cls")
                    continue
                else:
                    print("Thanks for using this program!")
                    break
            elif topic == "all":
                for topic_name in topic_name_list:
                    consume_logs(topic_name, num_rows)
                break
            else: continue
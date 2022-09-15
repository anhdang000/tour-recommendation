import sys
import os
import os.path as osp

import json
import re
import argparse

from configs import *


class LogProcessor():
    """Collect logs from Kafka -> process logs -> save logs to database
    """
    def __init__(self, topics=['log-feed']):
        self.topics = topics

        # Connect to kafka-server(k8s)
        # self.consumer = KafkaConsumer (
        #     **KAFKA_CFGS
        #     )

    def consume(self, topic):
        self.consumer.subscribe([topic])

        for message in self.consumer:
            print(message.value)
            msg = json.loads(message.value.decode("utf-8")) 
            # Check for `LOG-REQ-RESP`
            if "LOG-REQ-RESP" in msg['log']:
                # result = process_log(msg["log"]) # msg["log"] là kiểu str, xử lý log
                # lưu log vào file
                with open(f'{topic}.json', mode='a', encoding='utf-8') as f:
                    f.write(json.dumps(msg["log"], indent=4) + '\n')

    def process(self, log, uri_check='/posts/socPostCreat/v1', type='post'):
        """ Process log and extract neccessary information

        Args:
            log (str): raw log
            uri_check (str, optional): uri for API checking. Defaults to '/posts/socPostCreat/v1'.
            type (str, optional): type of uri_check, either 'post' or 'interact'. Defaults to 'post'.
        """
        # regex by API
        log_regex = re.search(
            r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:(?P<header>\{.*?\}),\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}\s+',
            log
        )

        if log_regex is None:
            # regex by web
            log_regex = re.search(
                r'(?P<timestamp>\d{4}\-\d{2}\-\d{2}\s+\d{2}\:\d{2}\:\d{2}\.\d{3})\s+(?P<level>\w+)\s+\d+\s+---\s+(?P<processor>\[.*?\])\s+(?P<serverLayer>[^ *]*)\s+: :::LOG-REQ-RESP:::\{\"\w+\"\:\"(?P<query>.*?)\",\"\w+\"\:\"(?P<uri>.*?)\",\"\w+\"\:\"(?P<pathInfo>.*?)\",\"\w+\"\:\"(?P<url>.*?)\",\"\w+\"\:\"(?P<header>\[.*?\])\",\"\w+\"\:\"(?P<method>.*?)\",\"\w+\"\:\"(?P<className>.*?)\",\"\w+\"\:\"(?P<serverName>.*?)\",\"\w+\"\:\"(?P<user>.*?)\",(?P<body>.*)\}',
                log
            )

        if log_regex is not None:
            msg_obj = '{' + log_regex.group(14) + '}'
            msg_obj = json.loads(msg_obj)
            
            body_req = msg_obj['bodyReq']
            body_resp = msg_obj['bodyResp']
            time_req = msg_obj['timeReq']
            time_resp = msg_obj['timeResp']

            # Header
            header_obj = log_regex.group(9)
            header_obj = json.loads(header_obj)

            if log_regex.group(6) == uri_check:
                if type == 'interact':
                    pass
                elif type == 'post':
                    status = body_resp['status']['success']
                    if status is True:
                        post_date = log_regex.group(1)
                        # user_id = re.search(r".*pn100\D*(?P<pn100>\w+).*", log_regex.group(13)).group(1)
                        owner_id = body_resp['elements'][0]['owner']
                        post_id = body_resp['elements'][0]['id']
                        post_type = body_resp['elements'][0]['typpost']
                        content = body_resp['elements'][0]['content']

                        # Save to database
                        element_dict_ = {
                            "post_id": post_id,
                            "post_date": post_date,
                            "owner_id": owner_id,
                            "post_type": post_type,
                            "content": content,
                            "locations": [],
                            "frequency": []
                        }
                        if element_dict_['post_id'] not in DB.post.distinct('post_id'):
                            DB.post.insert_one(element_dict_)
                else:
                    pass
        
    def process_json(self, json_file):
        f = open(json_file)
        data = json.load(f)
        for element in data:
            log = element['value']['payload']['log']
            if 'LOG-REQ-RESP' in log:
                self.process(log)

        

if __name__ == '__main__':
    log_collector = LogProcessor()
    log_collector.process_json('raw_logs.json')

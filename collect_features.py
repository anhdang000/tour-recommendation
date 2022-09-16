import sys
import os
import os.path as osp
sys.path.append(osp.abspath('modules'))

import re
import json
import pandas as pd
import numpy as np
import threading

from kafka import KafkaConsumer
from pymongo import MongoClient

from modules import *

class FeatureCollector():
    """Proces logs and extract into features
    """
    def __init__(self):
        self.log_collector = LogCollector()
        # self.loc_extractor = LocationExtractor()

    def extract_locs(self):
        while True:
            # Retrieve posts with empty location entry
            cursor = DB.post.find({"location": {"$size": 0}})
            print(f"Number of post to be extracted: {len(list(cursor))}")

            for doc in cursor:
                lines = [str.strip(x) for x in re.split("[\.\n]", doc["content"])if str.strip(x) != ""]
                line_segment = self.loc_extractor.get_word_segment_data(lines)

                locs_list = self.loc_extractor.get_location(line_segment)

                linked_locs_list, freq = self.loc_extractor.link_locations(locs_list)
                self.loc_extractor.update_db(doc["post_id"], linked_locs_list, freq)

    def run(self):
        t1 = threading.Thread(target=self.log_collector.run, args=())
        t2 = threading.Thread(target=self.extract_locs, args=())

        t1.start()
        t2.start()
        
        t1.join()
        t2.join()

if __name__ == "__main__":
    collector = FeatureCollector()
    collector.run()
import sys
import os
import os.path as osp

import re
import json
import pandas as pd
import numpy as np

from kafka import KafkaConsumer
from pymongo import MongoClient

from modules import *

class FeatureCollector():
    """Proces logs and extract into features
    """
    def __init__(self):
        self.log_processor = LogProcessor()
        self.loc_extractor = LocationExtractor()

    def process_logs(self):
        pass

    def extract_locs(self):
        # Retrieve posts with empty location entry
        cursor = DB.post.find({"location": {"$size": 0}})
        print(f"Number of post to be extracted: {len(list(cursor))}")

        for doc in cursor:
            lines = [str.strip(x) for x in re.split("[\.\n]", doc["content"])if str.strip(x) != ""]
            line_segment = self.loc_extractor.get_word_segment_data(lines)

            locs_list = self.loc_extractor.get_location(line_segment)

            linked_locs_list, freq = self.loc_extractor.link_locations(locs_list)
            self.loc_extractor.update_db(doc["post_id"], linked_locs_list, freq)


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


class Recommender():
    """Wrapper class to execute scoring algorithms
    """
    def __init__(self):
        self.score_evaluator = ScoreEvaluator()

    def run(self, user_id):
        """Evaluating scores and output to kafka topic
        """
        self.score_evaluator.calc_scores(user_id)
        

if __name__ == "__main__":
    recommender = Recommender()
    recommender.run()

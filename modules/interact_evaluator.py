import sys
import os
import os.path as osp

import json
import re
import argparse

from modules.configs import *


class InteractEvaluator():
    def __init__(self, mongodb_uri=MONGODB_URI, db='test'):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db]
        self.score_map = [1, 1.2, 1.5, 2]
        self.max_score = sum(self.score_map)

    def compute_scores_by_user(self, user_id):
        scores = {"user_id": user_id, "post_scores": {}}
        for doc in self.db.interact.find({'user_id': {'$eq': user_id}}):
            if doc["post_id"] not in scores["post_scores"].keys():
                scores["post_scores"][doc["post_id"]] = self.score_map[doc["interact"]]
            else:
                scores["post_scores"][doc["post_id"]] += self.score_map[doc["interact"]]

        self.db.post_scores.insert_one(scores)


    def compute_scores(self):
        for user_id in self.db.interact.distinct('user_id'):
            self.compute_scores_by_user(user_id)

if __name__ == "__main__":
    interact_eval = InteractEvaluator()
    interact_eval.compute_scores()
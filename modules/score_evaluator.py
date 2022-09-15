import sys
import os
import os.path as osp

import json
import re
import argparse

from configs import *


class ScoreEvaluator():
    def __init__(self):
        self.score_map = [1, 1.2, 1.5, 2]
        self.max_score = sum(self.score_map)

    def calc_base_scores_by_user(self, user_id):
        scores = {"user_id": user_id, "scores": {}}
        for doc in DB.interact.find({'user_id': user_id}):
            if doc["post_id"] not in scores["scores"].keys():
                scores["scores"][doc["post_id"]] = {
                    "base": self.score_map[doc["interact"]]
                    }
            else:
                scores["scores"][doc["post_id"]]["base"] += self.score_map[doc["interact"]]

        # Check for `user_id` existance
        if len(list(DB.scores.find({"user_id": user_id}))) == 0:
            # Insert score document
            DB.scores.insert_one(scores)
        else:
            # Iterate through each caclulated post
            for post_id, score_dict in scores["scores"].items():
                query = {"user_id": user_id, f"scores.{post_id}": {"$exists": True}}
                doc = DB.scores.find_one(query)
                # If score is already in the database, update new score
                if doc:
                    curr_score = doc["scores"][post_id]["base"]
                    DB.scores.update_one(
                        query,
                        {"$set": {
                            f"scores.{post_id}.base": curr_score + score_dict["base"]
                            }
                        }
                    )
                else:
                    DB.scores.update_one(
                        query,
                        {
                            "$set": {
                            f"scores.{post_id}.base": curr_score
                            }
                        },
                        {"upsert": True}
                    )

    def calc_loc_scores_by_user(self, user_id):
        score_doc = DB.scores.find_one({"user_id": user_id})["scores"]
        for post_id in score_doc.keys():
            post_doc = DB.post.find_one({"post_id": post_id})
            loc_scores = []
            for loc, freq in zip(post_doc['locations'], post_doc['frequency']):
                loc_score = {"loc": loc, "score": sum(freq) * score_doc[post_id]["base"]}
                loc_scores.append(loc_score)
                
            DB.scores.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                    f"scores.{post_id}.loc": loc_scores
                    }
                }
            )

    def calc_scores(self, user_id):
        self.calc_base_scores_by_user(user_id)
        self.calc_loc_scores_by_user(user_id)

    def calc_scores_all(self):
        for user_id in DB.interact.distinct('user_id'):
            print(f'user_id: {user_id}')
            self.calc_base_scores_by_user(user_id)
            self.calc_loc_scores_by_user(user_id)

if __name__ == "__main__":
    score_eval = ScoreEvaluator()
    score_eval.calc_scores_all()
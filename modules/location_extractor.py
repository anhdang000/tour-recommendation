import sys
import os
import os.path as osp
import re
import logging
from importlib import import_module

import pandas as pd
import numpy as np
import transformers
from transformers import (
    AutoConfig,
    AutoModelForTokenClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    EvalPrediction,
    HfArgumentParser,
    Trainer,
    TrainingArguments,
    set_seed,
)
from vncorenlp import VnCoreNLP
from utils_ner import Split, TokenClassificationDataset, TokenClassificationTask

from modules.configs import *

logger = logging.getLogger(__name__)


def get_ranges(nums):
    nums = sorted(set(nums))
    gaps = [[s, e] for s, e in zip(nums, nums[1:]) if s+1 < e]
    edges = iter(nums[:1] + sum(gaps, []) + nums[-1:])
    edges = list(zip(edges, edges))
    return [list(range(edge[0], edge[1] + 1)) for edge in edges]


def no_accent_vietnamese(s):
    s = re.sub('[áàảãạăắằẳẵặâấầẩẫậ]', 'a', s)
    s = re.sub('[ÁÀẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬ]', 'A', s)
    s = re.sub('[éèẻẽẹêếềểễệ]', 'e', s)
    s = re.sub('[ÉÈẺẼẸÊẾỀỂỄỆ]', 'E', s)
    s = re.sub('[óòỏõọôốồổỗộơớờởỡợ]', 'o', s)
    s = re.sub('[ÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢ]', 'O', s)
    s = re.sub('[íìỉĩị]', 'i', s)
    s = re.sub('[ÍÌỈĨỊ]', 'I', s)
    s = re.sub('[úùủũụưứừửữự]', 'u', s)
    s = re.sub('[ÚÙỦŨỤƯỨỪỬỮỰ]', 'U', s)
    s = re.sub('[ýỳỷỹỵ]', 'y', s)
    s = re.sub('[ÝỲỶỸỴ]', 'Y', s)
    s = re.sub('đ', 'd', s)
    s = re.sub('Đ', 'D', s)
    return s

    
class LocationExtractor():
    def __init__(self, mongodb_uri=MONGODB_URI, db='test', data='./data'):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db]
        self.annotator = VnCoreNLP("VnCoreNLP/VnCoreNLP-1.1.1.jar", annotators="wseg", max_heap_size='-Xmx2g') 

        # Adiministrative divisions
        self.dvhc_df = pd.read_csv(osp.join(data, 'dvhc.csv'))
        self.dvhc_df = self.dvhc_df.fillna('')

        # Regex for special characters
        self.regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')


    def init_model(self):
        self.config = AutoConfig.from_pretrained(
            'underthesea/examples/ner/out',
            cache_dir='underthesea/examples/ner/bert-ner/data',
        )
        self.tokenizer = AutoTokenizer.from_pretrained(
            'underthesea/examples/ner/out',
            cache_dir='underthesea/examples/ner/bert-ner/data',
            ee_fast=False,
        )
        self.model = AutoModelForTokenClassification.from_pretrained(
            'underthesea/examples/ner/out',
            from_tf=bool('.ckpt' in 'underthesea/examples/ner/out'),
            config=self.config,
            cache_dir='underthesea/examples/ner/bert-ner/data',
        )

        module = import_module('underthesea.examples.ner.tasks')
        token_classification_task_class = getattr(module, 'NER')
        token_classification_task: TokenClassificationTask = token_classification_task_class()

        self.labels = token_classification_task.get_labels("")

        self.pipeline = transformers.pipeline("ner", model=self.model, tokenizer=self.tokenizer, device=0)


    def get_word_segment_data(self, data):
        text_data = []
        for sample in data:
            text = self.annotator.tokenize(sample)
            text = ' '.join([' '.join(x) for x in text])
            text_data.append(text)
        return text_data


    def get_location(self, line_segment):
        res_array = self.pipeline(line_segment)
        list_locs = []
        for _, value in enumerate(res_array):
            inds = [item["index"] for item in value]
            ranges_inds = get_ranges(inds)
            value_dict = dict(zip(inds, value))
            for range_inds in ranges_inds:
                for i, idx in enumerate(range_inds):
                    if i == 0:
                        loc = []
                    word = value_dict[idx]["word"]
                    loc.append(word)
                normalized_loc = ' '.join(' '.join(loc).replace('_', ' ').replace('@@ ','').replace(' @@', '').split())
                list_locs.append(normalized_loc)
        return list_locs


    def search_dvhc_csv(query, df):
        """Search location query and return level-2 location or list of (level-2, level-3) or (level-2, level-4) location

        Args:
            query (string): location text
            df (pd.Dataframe): dataframe to search

        """
        query = query.lower()
        found_locs_2 = df.loc[df['tinh-tp'].str.contains(query, case=False)]['tinh-tp']
        found_locs_3 = df.loc[df['quan-huyen'].str.contains(query, case=False)]
        found_locs_4 = df.loc[df['phuong-xa'].str.contains(query, case=False)]

        found_locs_2_u = found_locs_2.unique()

        # tinh-tp
        if len(found_locs_2_u) == 1:
            return 2, found_locs_2_u[0]

        # quan-huyen
        if len(found_locs_3) >= 1:
            lv2_list = []
            results = []
            for i, row in found_locs_3.iterrows():
                if row['tinh-tp'] not in lv2_list:
                    results.append((row['tinh-tp'], row['quan-huyen']))
            results = list(set(results))
            return 3, results

        # phuong-xa
        if len(found_locs_4) >= 1:
            lv2_list = []
            results = []
            for i, row in found_locs_4.iterrows():
                if row['tinh-tp'] not in lv2_list:
                    results.append((row['tinh-tp'], row['quan-huyen'], row['phuong-xa']))
            results = list(set(results))
            return 4, results

        return None, None


    def compute_freq_dict(locs_by_level):
        freq_dict = {}   # Store in word-frequency pairs
        for lvl, locs in locs_by_level.items():
            if lvl == 2:
                for loc in locs: 
                    if loc not in freq_dict.keys():
                        freq_dict[loc] = 1
                    else:
                        freq_dict[loc] += 1
            else:
                for loc in locs:
                    if loc[-1] not in freq_dict.keys():
                        freq_dict[loc[-1]] = 1
                    else:
                        freq_dict[loc[-1]] += 1
        return freq_dict


    def link_locations(self, locs_list):
        """Link locations from multiple levels and compute frequency of apprearance for each locations.

        Args:
            locs_list (List[str]): Location list generated by NER model

        Returns:
            linked_locs_list (List[Tuple[str]]): Linked location list
            freq (List[Tuple[int]]): Frequency of appearance corresponding to `linked_locs_list`

        """
        # Process locations in a post
        locs_by_level = {}
        for loc in locs_list:
            # loc_noaccent = self.no_accent_vietnamese(loc)
            if not self.regex.search(loc):
                lvl, loc_results = self.search_dvhc_csv(loc, self.dvhc_df)
                # `loc_results` is level-2 location
                if type(loc_results) == str:
                    if lvl in locs_by_level.keys():
                        locs_by_level[lvl].append(loc_results)
                    else:
                        locs_by_level[lvl] = [loc_results]
                elif type(loc_results) == list:
                    if lvl in locs_by_level.keys():
                        locs_by_level[lvl] += loc_results
                        locs_by_level[lvl] += loc_results
                    else:
                        locs_by_level[lvl] = loc_results
                
        # Compute frequency
        levels = list(locs_by_level.keys())
        for lvl in levels:
            if 2 < lvl < 5:
                locs_by_level[lvl] = [item for item in locs_by_level[lvl] if item[0] in locs_by_level[2]]
        freq_dict = self.compute_freq_dict(locs_by_level)

        # Remove duplicated entries
        locs_by_level = {k: list(set(v)) for k, v in locs_by_level.items()}

        # Sort by levels
        locs_by_level = dict(sorted(locs_by_level.items()))

        # Link locations
        # Traverse location from lowest to highest level
        linked_locs_list = []
        for lvl in levels[::-1]:
            if lvl == max(levels):
                linked_locs_list += locs_by_level[lvl]
            elif lvl > 2:
                check_list = list(set([item[:lvl-1] for item in locs_by_level[lvl+1]]))
                for item in locs_by_level[lvl]:
                    if item not in check_list:
                        linked_locs_list.append(item)

        freq = []
        for linked_loc in linked_locs_list:
            freq.append(tuple(freq_dict[l] if l in freq_dict.keys() else 1 for l in linked_loc))

        return linked_locs_list, freq

    def update_db(self, post_id, linked_locs, freq):
        self.db.post.update_one(
            {"post_id": post_id}, 
            {
                "$push" : {
                    "locations": {
                        "$each": linked_locs
                    }
                }
            }
        )
        self.db.post.update_one(
            {"post_id": post_id}, 
            {
                "$push" : {
                    "frequency": {
                        "$each": freq
                    }
                }
            }
        )
        
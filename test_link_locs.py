import sys
import os
import os.path as osp

import json
import re
import pandas as pd


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

def search_dvhc_csv(query, df):
    """Search location query and return level-2 location or list of (level-2, level-3) or (level-2, level-4) location

    Args:
        query (string): location text
        df (pd.Dataframe): dataframe to search
    
    Returns:
        level (int): Level of location
        location (List[Tuple[str]]): Linked locations found by querying database
        attraction (str): Tourist attractions (level-5 location)
    """
    query = query.lower()
    found_locs_2 = df.loc[df['tinh-tp'].str.contains(query, case=False)]['tinh-tp']
    found_locs_3 = df.loc[df['quan-huyen'].str.contains(query, case=False)]
    found_locs_4 = df.loc[df['phuong-xa'].str.contains(query, case=False)]

    found_locs_2_u = found_locs_2.unique()

    # tinh-tp
    if len(found_locs_2_u) == 1:
        return 2, found_locs_2_u[0], None

    # quan-huyen
    if len(found_locs_3) >= 1:
        lv2_list = []
        results = []
        for i, row in found_locs_3.iterrows():
            if row['tinh-tp'] not in lv2_list:
                results.append((row['tinh-tp'], row['quan-huyen']))
        results = list(set(results))
        return 3, results, None

    # phuong-xa
    if len(found_locs_4) >= 1:
        lv2_list = []
        results = []
        for i, row in found_locs_4.iterrows():
            if row['tinh-tp'] not in lv2_list:
                results.append((row['tinh-tp'], row['quan-huyen'], row['phuong-xa']))
        results = list(set(results))
        return 4, results, None

    return 5, None, query


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
    

if __name__ == "__main__":
    # Load DVHC data
    # dvhc_file = "dvhcvn.json"
    # dvhc_data = json.load(open(dvhc_file, encoding='utf-8'))
    
    dvhc_csv_file = 'data/dvhc.csv'
    dvhc_df = pd.read_csv(dvhc_csv_file)
    dvhc_df = dvhc_df.fillna('')

    # Sample output
    with open('sample_loc_outputs.txt', encoding='utf-8') as f:
        locs_list = [line.strip().split(',') for line in f.readlines()]
    
    regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')

    for locs in locs_list:
        # Process locations in a post
        locs_by_level = {}
        attractions = []
        all_locs = []
        for loc in locs:
            loc_noaccent = no_accent_vietnamese(loc)
            if not regex.search(loc):
                lvl, loc_result, attraction = search_dvhc_csv(loc, dvhc_df)
                # `loc_result` is level-2 location
                if type(loc_result) == str:
                    if lvl in locs_by_level.keys():
                        locs_by_level[lvl].append(loc_result)
                        all_locs.append(lvl)
                    else:
                        locs_by_level[lvl] = [loc_result]
                        all_locs.append(lvl)
                elif type(loc_result) == list:
                    if lvl in locs_by_level.keys():
                        locs_by_level[lvl] += loc_result
                        all_locs.append(lvl)
                    else:
                        locs_by_level[lvl] = loc_result
                        all_locs.append(lvl)
                elif attraction is not None:
                    attractions.append(attraction)
                    all_locs.append(attraction)
        print(f'All locs: {all_locs}')
        # Compute frequency
        levels = list(locs_by_level.keys())
        for lvl in levels:
            if 2 < lvl < 5:
                locs_by_level[lvl] = [item for item in locs_by_level[lvl] if item[0] in locs_by_level[2]]
        freq_dict = compute_freq_dict(locs_by_level)
        print(f'freq_dict: {freq_dict}')
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

        print(f'linked: {linked_locs_list}')

        freq = []
        for linked_loc in linked_locs_list:
            freq.append(tuple(freq_dict[l] if l in freq_dict.keys() else 1 for l in linked_loc))
                
        print(f'freq: {freq}')
        print('=======================')

        sys.exit()
        
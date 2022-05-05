#%% Import libraries.
import pandas as pd
import re
from os.path import join, exists
import os
import numpy as np

def cleaner(text: str) -> str:
    """
    Cleans the tweets using the fllowing sequential steps:
        - Remove all the words starting with '_' ("_ahiraj"), mentions starting with '_' ("@_Silent__Eyes__") and also Devanagiri ("@पाेखरा") and hashtags used with Devanagiri ("#पाेखरा").
        - Remove punctuations (selected manually). This does not include sentence enders like "|" or "." or "?" or "!".
        - Removes bad characters like "&gt;".
        - If a punctuation or a whitespace has been repeated multiple times, adjust it to a single occurence.

    Args:
        text (str): Input text.

    Returns:
        str: Cleaned text.
    """    
    pattern1 = r'(_[a-zA-Z0-9]+)|(#[\u0900-\u097F]+)|(@[\u0900-\u097F]+)|(_[\u0900-\u097F]+)'
    to_replace = """@#=/+…:"")(}{][*%_’‘'"""
    pattern2 = r'(\W)(?=\1)'
    text = re.sub(pattern1, '', text)
    text = text.translate(str.maketrans('', '', to_replace))
    text = text.translate(str.maketrans('', '', '&gt;'))
    text = re.sub(pattern2, '', text)
    return text


root = r'D:\ML_projects\IPV-Scraper\results\new_twitter'
filenames = os.listdir(root)
filepaths = [join(root, filename) for filename in filenames]

print(f'Found {len(filenames)} files in {root}.\n')

#%% Load Files.
dataframe_list = [pd.read_csv(my_path, encoding='utf-8', skip_blank_lines = True) for my_path in filepaths]

#%% Concatenate all DataFrames.
df_main = pd.concat(dataframe_list, axis = 0, ignore_index = True)

# Clean Tweets.
df_main['tweet'] = df_main['tweet'].apply(cleaner)

#%% To CSV.
df_main.to_csv(join(root, 'df_merged_search_terms_cleaned_25-Apr-2021_nep.csv'), index = None, encoding = 'utf-8')

df_main.keyword.value_counts().to_csv(join(root, 'tweet_counts_nep.csv'), encoding = 'utf-8', header = None)
# -*- coding: utf-8 -*-
"""
Created on Sat May 14 11:46:27 2022

@author: Sagun Shakya

Description: 

"""

#%% Import libraries.
import emoji
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
    pattern1 = r'(_[a-zA-Z0-9]+)|(#[\u0900-\u097F]+)|(@[\u0900-\u097F]+)|(_[\u0900-\u097F]+)|(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)|https?:\/\/\S*'
    to_replace = """@#=/+…:"")(}{][*%_’‘'"""
    pattern2 = r'(\W)(?=\1)'
    text = re.sub(pattern1, '', text)
    text = text.translate(str.maketrans('', '', to_replace))
    text = text.translate(str.maketrans('', '', '&gt;'))
    text = emoji.replace_emoji(text, "")
    text = re.sub(pattern2, '', text)
    return text

def organizer(root: str, output_filename: str):
    filenames = os.listdir(root)
    assert len(filenames) > 0, "There are no files in the root directory."

    filepaths = [join(root, filename) for filename in filenames]

    print(f'Found {len(filenames)} files in {root}.\n')

    #%% Load Files.
    dataframe_list = [pd.read_csv(my_path, encoding='utf-8', skip_blank_lines = True) for my_path in filepaths]

    #%% Concatenate all DataFrames.
    df_main = pd.concat(dataframe_list, axis = 0, ignore_index = True)

    # Clean Tweets.
    df_main['tweet'] = df_main['tweet'].apply(cleaner)

    # Select only the columns "date tweet link search".
    df_main = df_main["date tweet link search".split()]
    df_main.columns = "date text link keyword".split()

    # Select only those rows having sentence lengths greater than or equal to 3.
    condition = df_main['text'].apply(lambda x: len(x.split()) >= 3)
    df_main = df_main[condition]

    # Remove duplicated texts.
    df_main.drop_duplicates(subset = 'text', keep = 'first', ignore_index = True, inplace = True)

    # Reset index.
    df_main.reset_index(drop = True, inplace = True)

    #%% To CSV.
    df_main.to_excel(join(root, output_filename), index = None, encoding = 'utf-8')

    #%% Total keywords (by frequency).
    df_main.keyword.value_counts().to_csv(join(root, 'tweet_keyowrd_counts.csv'), encoding = 'utf-8', header = None)
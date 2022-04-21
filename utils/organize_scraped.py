#%% Import libraries.
import pandas as pd
from os.path import join, exists
import os
import numpy as np

root = r'D:\ML_projects\IPV-Scraper\results'
filenames = os.listdir(root)
filepaths = [join(root, filename) for filename in filenames]

print(f'Found {len(filenames)} files in {root}.\n')

#%% Load Files.
dataframe_list = [pd.read_csv(my_path, encoding='utf-8', skip_blank_lines = True) for my_path in filepaths]

#%% Concatenate all DataFrames.
df_main = pd.concat(dataframe_list, axis = 0, ignore_index = True)

#%% To CSV.
df_main.to_csv(join(root, 'df_merged_search_terms_21-Apr-2021.csv'), index = None, encoding = 'utf-8')

df_main.keyword.value_counts().to_csv(join(root, 'tweet_counts.csv'), encoding = 'utf-8', header = None)
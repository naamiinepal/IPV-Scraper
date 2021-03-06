# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 21:01:41 2022

@author: Sagun Shakya

Twint Dataclass attributes:
    
    Username: Optional[str] = None
    User_id: Optional[str] = None
    Search: Optional[str] = None
    Lookup: bool = False
    Geo: str = ""
    Location: bool = False
    Near: str = None
    Lang: Optional[str] = None
    Output: Optional[str] = None
    Elasticsearch: object = None
    Year: Optional[int] = None
    Since: Optional[str] = None
    Until: Optional[str] = None
    Email: Optional[str] = None
    Phone: Optional[str] = None
    Verified: bool = False
    Store_csv: bool = False
    Store_json: bool = False
    Custom = {"tweet": None, "user": None, "username": None}
    Show_hashtags: bool = False
    Show_cashtags: bool = False
    Limit: Optional[int] = None
    Count: Optional[int] = None
    Stats: bool = False
    Database: object = None
    To: str = None
    All = None
    Debug: bool = False
    Format = None
    Essid: str = ""
    Profile: bool = False
    Followers: bool = False
    Following: bool = False
    Favorites: bool = False
    TwitterSearch: bool = False
    User_full: bool = False
    # Profile_full: bool = False
    Store_object: bool = False
    Store_object_tweets_list: list = None
    Store_object_users_list: list = None
    Store_object_follow_list: list = None
    Pandas_type: type = None
    Pandas: bool = False
    Index_tweets: str = "twinttweets"
    Index_follow: str = "twintgraph"
    Index_users: str = "twintuser"
    Retries_count: int = 10
    Resume: object = None
    Images: bool = False
    Videos: bool = False
    Media: bool = False
    Replies: bool = False
    Pandas_clean: bool = True
    Lowercase: bool = True
    Pandas_au: bool = True
    Proxy_host: str = ""
    Proxy_port: int = 0
    Proxy_type: object = None
    Tor_control_port: int = 9051
    Tor_control_password: str = None
    Retweets: bool = False
    Query: str = None
    Hide_output: bool = False
    Custom_query: str = ""
    Popular_tweets: bool = False
    Skip_certs: bool = False
    Native_retweets: bool = False
    Min_likes: int = 0
    Min_retweets: int = 0
    Min_replies: int = 0
    Links: Optional[str] = None
    Source: Optional[str] = None
    Members_list: Optional[str] = None
    Filter_retweets: bool = False
    Translate: bool = False
    TranslateSrc: str = "en"
    TranslateDest: str = "en"
    Backoff_exponent: float = 3.0
    Min_wait_time: int = 0
    Bearer_token: str = None
    Guest_token: str = None
    deleted: list = None
"""

import os
from time import time
from typing import List, Optional
import twint

import nest_asyncio
nest_asyncio.apply()

def scrape_tweets(search_term: Optional[str],
                    fields: List[str] = 'all',
                    store_csv: bool = False, 
                    save_dir: str = r'results',
                    filename: str = 'scraped_tweets.csv',
                    verbose: bool = False):

    # Set up TWINT config
    c = twint.Config()
    c.Username = None
    c.User_id = None
    c.Search = search_term
    c.Retweets = False
    c.Replies = True
    c.Location = None
    c.Geo = None            #"27.700769,85.300140,10km"
    c.Lang = 'ne'           # "ne" for Nepali, "hi" for Hindi, "en" for English
    c.Since = "2015-01-01"

    # Custom output format
    c.Limit = 10000
    c.Pandas = True
    c.Store_csv = False
    c.Store_json = False
    c.Hide_output = not verbose

    # Run search.
    start = time()
    twint.run.Search(c)
    twint_to_pd = lambda column_names: twint.output.panda.Tweets_df[column_names]
    
    # To Pandas DataFrame.
    columns = twint.output.panda.Tweets_df.columns if fields == "all" else fields
    #columns = 'id date tweet language place hashtags user_id name link urls'.split()

    # Save the tweets in a DataFrame.
    tweet_df = twint_to_pd(columns)
    end = time()
    if verbose:
        print(f'Collected {len(tweet_df)} tweets in {end - start: 5.3f} seconds.\n')
    
    if store_csv:
        os.makedirs(save_dir, exist_ok = True)
        if search_term is not None:
            filename = filename[:-4] + "_" + search_term.replace("#", "") + ".csv"
        else:
            filename = 'scraped_tweets.csv'

        # Save to CSV file in the desired location.
        tweet_df.to_csv(os.path.join(save_dir, filename), index = None, encoding = 'utf-8')

    return tweet_df

import os
from time import sleep, time

from pandas import read_csv
from pipeline.get_replies import TweetReplies
from os.path import join, exists

# Local modules.
from pipeline.twint_scraper import scrape_tweets
from utils import utils

def collect_tweets_from_seed(seed: str):
    start = time()
    fields = 'id conversation_id date tweet language hashtags user_id_str username link retweet nreplies search reply_to'.split()
    df = scrape_tweets(search_term = seed, fields = fields, store_csv = True)

    end = time()
    print(f'Collected {len(df)} Tweets in {end - start: .5f} seconds.')

    return df


def main(target_dir = r'results/second_lot'):

    keywords = read_csv('keywords.txt', header = None, encoding = 'utf-8', skip_blank_lines = True)
    os.makedirs(target_dir, exist_ok=True)

    for SEED in keywords.iloc[39:, :][0].to_list():   # Lot 2.
        if not exists(join(target_dir, f'scraped_{SEED}.csv')):
            try:
                df = collect_tweets_from_seed(SEED)
                if len(df) > 0:
                    df.to_csv(join(target_dir, f'scraped_{SEED}.csv'), index = None, encoding = "utf-8")
            except:
                print(f"\nSkipping for seed = {SEED}.\n")
                continue

            sleep(5.0)

if __name__ == "__main__":
    main()

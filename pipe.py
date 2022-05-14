
import os
from time import sleep, time

from pandas import Series, read_csv
from os.path import join, exists

# Local modules.
from pipeline.get_replies import TweetReplies
from pipeline.twint_scraper import scrape_tweets
from utils import utils
from utils.organize_scraped import organizer

def collect_tweets_from_seed(seed: str):
    start = time()
    fields = 'id conversation_id date tweet language hashtags user_id_str username link retweet nreplies search reply_to'.split()
    print(f"\nSearch Term : {seed}\n{'_'*50}")

    df = scrape_tweets(search_term = seed, fields = fields, store_csv = False)

    end = time()
    print(f'Collected {len(df)} Tweets in {end - start: .5f} seconds.\n{"-" * 50}')

    return df


def main(target_dir:str = r'results/second_lot', output_filename: str = 'scraped_keywords_.xlsx', organize_tweets: bool = True):

    keywords = read_csv('keywords.txt', header = None, encoding = 'utf-8', skip_blank_lines = True)
    keywords = keywords.drop_duplicates(keep = 'first').values.ravel()
    print(f'\nFound {len(keywords)} keywords.\nStarting search...\n')

    os.makedirs(target_dir, exist_ok=True)

    for SEED in keywords:   # Lot 2.

        save_filename = f'scraped_{SEED}.csv'
        if not exists(join(target_dir, save_filename)):
            try:
                df = collect_tweets_from_seed(SEED)
                if len(df) > 0:
                    df.to_csv(join(target_dir, save_filename), index = None, encoding = "utf-8")
            except:
                print(f"\nSkipping for seed = {SEED}.\n")
                continue

            sleep(5.0)
    
    # Organize scraped Tweets in a file.
    if organize_tweets:
        organizer(root = target_dir, output_filename = 'scraped_all_keywords_04-05-022.xlsx')
    

if __name__ == "__main__":
    #df = scrape_tweets(search_term = None, store_csv = True, save_dir = r'results/temp', verbose = True)
    '''
    Driver Code.
    '''
    main(target_dir = r'results/all_keywords', output_filename = 'scraped_all_keywords_04-05-022.xlsx')
from ctypes import util
from time import time
from pipeline.get_replies import TweetReplies
from os.path import join

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


def main(batch_size: int = 1):
    assert batch_size > 0, "Batch size should be a Natural Number."

    # Scrape Tweets using a seed.
    SEED = 'कुकुर'
    
    df = collect_tweets_from_seed(SEED)
    assert len(df) > 0 and df is not None, "No items in the DataFrame."

    # Extract tweets with at least one reply.
    df_with_replies = df[df['nreplies'] > 0]

    # Extract Tweet IDs from the links so that their replies can be scraped.
    tweet_ids = df_with_replies['link'].apply(lambda string: string.split("/")[-1])
    tweet_ids = tweet_ids.to_list()

    if batch_size > 1:
        tweet_id_batches = utils.create_batch(tweet_ids)

        # WRITE CODE FOR EXTRACTING REPLIES USING BATCHES.


    # Load the configuration that contains the bearer tokens along with API and access tokens.
    config = utils.load_config('config.yaml')

    # Instantiate Replies Loader.
    loader = TweetReplies(config, clean_text=False)

    df2 = loader.get_replies_from_tweet_id_to_dataframe(tweet_id = tweet_ids[5])
    df2.to_csv(join('results', 'replies_sample.csv'), encoding = 'utf-8', index = None)
    
    # Supply Tweet ID.
    #agg_dict = loader.get_replies_from_tweet_id_list(tweet_ids, max_results = 100)
    
    # To JSON.
    #assert len(agg_dict) > 0 and agg_dict is not None, "Aggregated dictionary is empty."
    #utils.dict_to_json(agg_dict, join('results', 'agg_dict.json'))
    #print(df)
    #df.to_csv(join("results", f"newde.txt"), index = None, encoding = 'utf-8')

if __name__ == "__main__":
    main()

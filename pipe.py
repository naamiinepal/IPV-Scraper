from time import time

# Local modules.
from pipeline.twint_scraper import scrape_tweets

def main():
    start = time()
    df = scrape_tweets(search_term = '#nepse', store_csv = True)
    end = time()
    print(f'Collected {len(df)} Tweets in {end - start} milli seconds.')

if __name__ == "__main__":
    main()
import re
from pandas import DataFrame
import tweepy
import time
import emoji
#ssl._create_default_https_context = ssl._create_unverified_context

# Local Modules.
import utils

class TweetReplies:
    def __init__(self, config, clean_text: bool = True, verbose: bool = False) -> None:
        self.config = config
        self.bearer_token = self.config.bearer_token
        self.client = tweepy.Client(bearer_token = self.bearer_token)

        self.clean_text = clean_text
        self.verbose = verbose

    def get_replies_from_tweet_id(self, 
                                tweet_id: str = "1516429498731966467", 
                                max_results: int = 100, 
                                return_dict: bool = False) -> dict:
        
        #tweet_id = "1516429498731966467"
        query = "conversation_id:" + tweet_id

        # Get response from the API request.
        response = self.client.search_recent_tweets(query = query , max_results = max_results)

        # Data Reserve.
        reserve = dict() if return_dict else list()

        for tweet in response.data:
            assert hasattr(tweet, "id") == True, "Tweet has no attribute named 'id'."
            assert hasattr(tweet, "text") == True, "Tweet has no attribute named 'text'."

            id = tweet.id
            text = self._clean_tweet(tweet.text) if self.clean_text else tweet.text

            if self.verbose:
                print(f'ID : {id}\nTEXT : {text}')
                print("_"*20)
            
            if return_dict:
                reserve[id] = text
            else:
                reserve.append((id, text))
        
        if self.verbose:
            print(f'Total number of Replies for tweet ID {tweet_id} = {len(reserve)}.')

        return reserve

    def get_num_replies(self, 
                        tweet_id: str = "1516429498731966467", 
                        max_results: int = 100) -> int:

        return len(self.get_replies_from_tweet_id(tweet_id, max_results, return_dict = False))

    def get_replies_from_tweet_id_to_dataframe(self, 
                                                tweet_id: str = "1516429498731966467", 
                                                max_results: int = 100, 
                                            ) -> DataFrame:
        
        response = self.get_replies_from_tweet_id(tweet_id, max_results, return_dict=False)
        assert len(response) > 0, "The response is empty."

        # Convert to a Pandas DataFrame.
        result = DataFrame(response, columns = ['id', 'text'])
        return result

    def _clean_tweet(self, text):
        # Remove hyperlinks, hashtags and mentions.
        pattern = r'(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)|https?:\/\/\S*'
        text = re.sub(pattern, "", text)
        
        # Remove emojis.
        text = emoji.replace_emoji(text, "")

        return text


def main():
    # Load the configuration that contains the bearer tokens along with API and access tokens.
    config = utils.load_config('config.yaml')

    # Instantiate Replies Loader.
    loader = TweetReplies(config, clean_text=True)

    # Supply Tweet ID.
    tweet_id = "1516429498731966467"
    
    # Obtain dataframe.
    df = loader.get_replies_from_tweet_id_to_dataframe(tweet_id, 100)

    df.to_csv(f"replies_{tweet_id}_cleaned.txt", index = None, encoding = 'utf-8')

if __name__ == "__main__":
    main()


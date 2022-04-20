import tweepy
import re
from typing import List
from pandas import DataFrame
import time
import emoji
#ssl._create_default_https_context = ssl._create_unverified_context

# Local Modules.
import utils

class TweetReplies:
    def __init__(self, config, clean_text: bool = True, verbose: bool = False) -> None:
        """
        Obtains the Replies to a given tweet or a list of tweets along with the ID of the commentor.
        If clean_text is set to True, it will remove:
            - Hashtags.
            - Mentions.
            - Hyperlinks (or URLs in general (starts with "http" of "https")).
            - Emojis.

        Args:
            config (DotDict object): Configuration object. Obtain items using dot (.) notation.
            clean_text (bool, optional): Whether to clean the replies. Defaults to True.
            verbose (bool, optional): Verbosity. Defaults to False.
        """        
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

    def get_replies_from_tweet_id_list(self, tweet_ids: List[str], max_results: int = 100) -> dict:
        """
        Takes in a list of Tweet IDs and obtains replies from each one of them (5 second time gap between successive requests).

        Args:
            tweet_ids (List[str]): List of Tweet IDs.
            max_results (int, optional): Number of replies to obtain. Defaults to 100.

        Returns:
            dict: Dictionary where keys are the input tweet IDs and the values are the 
                  dictionary of tweet replies. 
        """        
        assert len(tweet_ids) > 0 and tweet_ids is not None, "Tweet ids list is empty."
        assert type(tweet_ids) == list, "The ids should be a list of strings."

        collection = dict()   # Dictionary to store a dictionary of tweet replies.

        for id in tweet_ids:
            replies = self.get_replies_from_tweet_id(id, max_results, return_dict = True)
            n_replies = len(replies)

            if self.verbose:
                print(f'nObtained {n_replies} replies from tweet ID {id}.\n')
                print('Sleep 5 seconds...\n')

            time.sleep(5)

            # Store in a collection if the replies list exists.
            if n_replies > 0:
                collection[str(id)] = replies
            
            return collection

    def _clean_tweet(self, text: str) -> str:
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


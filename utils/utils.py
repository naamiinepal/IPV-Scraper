import json
import re
import emoji
import yaml
from os.path import exists

from utils.read_configuration import DotDict

def load_config(config_path: str) -> DotDict:
    '''
    Loads the YAML config file.

    Parameters
    ----------
    config_path : str
        path to the config file.

    Raises
    ------
    FileNotFoundError
        If file does't exist.

    Returns
    -------
    config : dict
        config dict to be accessed for project.

    '''
    if exists(config_path):
        with open(config_path) as file:
            config = yaml.safe_load(file)
    else:
        raise FileNotFoundError()
        
    return DotDict(config)  

def create_batch(iterable, n=1):
    """
    Create batches of size 'n' out of a list of items.

    Args:
        iterable (_type_): 1D - List.
        n (int, optional): batch_size. Defaults to 1.

    Yields:
        _type_: _description_
    """    
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def dict_to_json(dictionary: dict, save_path: str):
    with open(save_path, encoding="utf8", mode = "w") as f:
        json.dump(dictionary, f)

def clean_tweet(text: str) -> str:
    # Remove hyperlinks, hashtags and mentions.
    pattern = r'(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)|https?:\/\/\S*'
    text = re.sub(pattern, "", text)
    
    # Remove emojis.
    text = emoji.replace_emoji(text, "")

    return text

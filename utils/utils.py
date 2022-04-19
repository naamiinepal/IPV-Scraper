import yaml
from os.path import exists

from read_configuration import DotDict

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
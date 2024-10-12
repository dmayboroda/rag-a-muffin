import os
import yaml
from .async_queue import AsyncQueue
from .aws_rds_helper import RDSHelper

def load_config(config_file):
    """
    Load the configuration file from local storage.

    Args:
        config_file (str): The path to the configuration file.
    """
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

"""
Initialize the global variables for the application.
"""
async_queue = AsyncQueue()
file_queue = AsyncQueue()
#AWS entities
dir_path = os.path.dirname(__file__)
config_path = os.path.join(dir_path, "config.yml")
rds_helper = RDSHelper(load_config(config_path))

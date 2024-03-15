import os
import yaml
import logging

log = logging.getLogger(__name__)

def read_yaml(file_path: os.PathLike) -> dict:
    """
    Read a YAML file and return its content as a dictionary.

    Args:
        file_path (os.PathLike): The path to the YAML file.

    Returns:
        dict: The content of the YAML file as a dictionary.
    """
    with open(file_path, 'r') as yaml_file:
        try:
            data = yaml.safe_load(yaml_file)
        except yaml.YAMLError as exc:
            log.error(exc)
    return data


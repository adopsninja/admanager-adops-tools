from pathlib import PurePath

import yaml


class ConfigReader:
    def __init__(self, path_to_configuration_file) -> None:
        self.path_to_configuration_file = path_to_configuration_file

    def read_yaml_config(self) -> dict:
        with open(PurePath(self.path_to_configuration_file), "r") as config_file:
            config = yaml.safe_load(config_file)
        return config

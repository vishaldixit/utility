import json
from ConstantsModule import Constants


class ConfigManager:
    def __init__(self, logger, config_file_path):
        self.logger = logger
        try:
            with open(config_file_path, "rb") as conf_file:
                self.config_data = json.load(conf_file)
                self.config_pipelines_data = self.get_config_value(Constants.CONFIG_SAX_MONITORING_PIPELINES)
                self.logger.debug(self.config_data)
            self.logger.debug("Configuration loaded")
        except Exception as e:
            self.logger.error("Config file is not in right format- {0}".format(str(e)))

    def get_config_value(self, key):
        return self.config_data[key]

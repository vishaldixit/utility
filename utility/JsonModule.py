import json


class JsonHelper:
    def __init__(self, logger):
        self.logger = logger

    def is_json(self, value):
        try:
            json_object = json.loads(value)
            self.logger.info("Valid Json")
        except ValueError:
            self.logger.warning("Invalid Json")
            return False
        return True

    def get_matched_key(self, dictionary, value):
        result_key = None
        for key in dictionary.keys():
            if dictionary[key] == value:
                result_key = key
                break
        self.logger.info("Matched key - {0}".format(result_key))
        return result_key

    def get_min_key_value(self, dictionary):
        min_value = min(dictionary.values())
        min_key = self.get_matched_key(dictionary, min_value)
        self.logger.info("min value - {0} and min key - {1}".format(min_value, min_key))
        return min_key, min_value

    def get_max_key_value(self, dictionary):
        max_value = max(dictionary.values())
        max_key = self.get_matched_key(dictionary, max_value)
        self.logger.info("max value - {0} and max key - {1}".format(max_value, max_key))
        return max_key, max_value

    def update_partition_corrupt_offset(self, dictionary, max_value, offset_difference):
        for key in dictionary.keys():
            if max_value - dictionary[key] > offset_difference:
                dictionary[key] = max_value
        self.logger.info("update_partition_corrupt_offset - {0}".format(dictionary))
        return dictionary

    def get_current_checkpoint(self, pipeline_config_json):
        try:
            current_checkpoint_directory = ""

            if 'processors' not in pipeline_config_json:
                raise ValueError("No processors element in given data")

            index = 0
            is_exist = False
            for processor in pipeline_config_json['processors']:
                if self.is_json_element_exist(processor):
                    self.logger.info("Pipelines existing checkpoint path -{0}".format(
                        processor['config']['checkpointLocation']))
                    current_checkpoint_directory = pipeline_config_json['processors'][index]['config'][
                        'checkpointLocation']
                    self.logger.info("Pipelines updated checkpoint path -{0}".format(
                        pipeline_config_json['processors'][index]['config']['checkpointLocation']))
                    is_exist = True
                    break
                else:
                    index += 1
                    continue
            if not is_exist:
                self.logger.warning("This pipeline does not contain processor with checkpoint location.")

            return current_checkpoint_directory

        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return pipeline_config_json

    def is_json_element_exist(self, data):
        is_exist = False
        try:
            if data['config']['checkpointLocation'] is not None:
                self.logger.info("Json element is exist.")
                is_exist = True
            return is_exist
        except (ValueError, Exception):
            return is_exist

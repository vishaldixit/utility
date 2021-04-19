import requests
import uuid
import json

from ConstantsModule import Constants


class RestApiManager:
    def __init__(self, logger, api_url, token=None):
        self.logger = logger
        self.logger.debug("Initialized SparkApi Manager")
        self.api_url = api_url
        self.token = None
        if token is not None:
            self.token = token

    def get_url(self, endpoint):
        return self.api_url + endpoint

    def mask_url(self, url):
        host_name = url.split(':')[1].split('//')[1]
        return url.replace(host_name, '*******')

    def get_api_response(self, endpoint):
        if self.token is not None:
            self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
            result = requests.get(self.get_url(endpoint), headers={'token': self.token})
        else:
            self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
            result = requests.get(self.get_url(endpoint))

        if result.status_code != 200:
            self.logger.error(
                'GET request url -{0}, status code- {1}'.format(self.mask_url(self.get_url(endpoint)),
                                                                result.status_code))
            raise ValueError("Invalid response received.")

        return result.json()

    def get_api_response_v2(self, endpoint):
        if self.token is not None:
            self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
            result = requests.get(self.get_url(endpoint), headers={'token': self.token})
        else:
            self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
            result = requests.get(self.get_url(endpoint))

        if result.status_code != 200:
            self.logger.error(
                'GET request url -{0}, status code- {1}'.format(self.mask_url(self.get_url(endpoint)),
                                                                result.status_code))
            raise ValueError("Invalid response received.")

        return result.json(), result.text

    def post_api_response(self, endpoint, payload=None):
        is_operation_valid = False
        try:
            header = {
                'token': self.token,
                'cache-control': "no-cache",
                'pipeline-monitor-token': str(uuid.uuid4())
            }
            self.logger.debug("Request headers are -{0}".format(header))

            if self.token is not None and payload is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
                result = requests.request("POST", self.get_url(endpoint), headers=header, data=json.dumps(payload))
            elif self.token is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
                result = requests.request("POST", self.get_url(endpoint), headers=header)
            else:
                result = requests.request("POST", self.get_url(endpoint))

            result_output = result.json()

            if result.status_code == 200 and result_output is not None and result_output['status'] == "SUCCESS":
                self.logger.info(
                    'Post request -{0}  StatusCode-{1} responseContent-{2}'.format(
                        self.mask_url(self.get_url(endpoint)),
                        result.status_code,
                        result.content))
                is_operation_valid = True
            else:
                self.logger.error(
                    'Error in Post request -{0}  StatusCode-{1} Response-{2}'.format(
                        self.mask_url(self.get_url(endpoint)),
                        result.status_code,
                        result_output))

            return is_operation_valid
        except(ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return is_operation_valid

    def post_api_response_string_payload(self, endpoint, payload=None):
        is_operation_valid = False
        try:
            header = {
                'token': self.token,
                'cache-control': "no-cache",
                'pipeline-monitor-token': str(uuid.uuid4())
            }
            self.logger.debug("Request headers are -{0}".format(header))

            if self.token is not None and payload is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
                result = requests.request("POST", self.get_url(endpoint), headers=header, data=payload)
            elif self.token is not None:
                self.logger.info("Request Url -{0}".format(self.mask_url(self.get_url(endpoint))))
                result = requests.request("POST", self.get_url(endpoint), headers=header)
            else:
                result = requests.request("POST", self.get_url(endpoint))

            result_output = result.json()

            if result.status_code == 200 and result_output is not None and result_output['status'] == "SUCCESS":
                self.logger.info(
                    'Post request -{0}  StatusCode-{1} responseContent-{2}'.format(
                        self.mask_url(self.get_url(endpoint)),
                        result.status_code,
                        result.content))
                is_operation_valid = True
            else:
                self.logger.error(
                    'Error in Post request -{0}  StatusCode-{1} Response-{2}'.format(
                        self.mask_url(self.get_url(endpoint)),
                        result.status_code,
                        result_output))

            return is_operation_valid
        except(ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return is_operation_valid

    def get_pipeline_config_json(self, pipeline_name):
        pipeline_config_json = None
        try:
            pipeline_config_json = self.get_api_response(
                Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))
            return pipeline_config_json
        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return pipeline_config_json

    def get_updated_checkpoint(self, pipeline_config_json, checkpoint_dir_name):
        try:

            if 'processors' not in pipeline_config_json:
                raise ValueError("No processors element in given data")

            index = 0
            is_updated = False
            for processor in pipeline_config_json['processors']:
                if self.is_json_element_exist(processor):
                    self.logger.info("Pipelines existing checkpoint path -{0}".format(
                        processor['config']['checkpointLocation']))
                    pipeline_config_json['processors'][index]['config']['checkpointLocation'] = checkpoint_dir_name
                    self.logger.info("Pipelines updated checkpoint path -{0}".format(
                        pipeline_config_json['processors'][index]['config']['checkpointLocation']))
                    is_updated = True
                    break
                else:
                    index += 1
                    continue
            if not is_updated:
                self.logger.warning("This pipeline does not contain processor with checkpoint location.")

            return pipeline_config_json

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

    # def update_checkpoint(self, pipeline_name, checkpoint_dir_name):
    #     is_valid = False
    #     try:
    #         pipeline_config_json = self.get_api_response(
    #             Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))
    #         # pipeline_config_json['config']['checkpointLocation'] = checkpoint_dir_name
    #         pipeline_config_json = self.get_updated_checkpoint(pipeline_config_json, checkpoint_dir_name)
    #         self.send_update_json_request(pipeline_config_json, pipeline_name)
    #
    #     except (ConnectionError, Exception) as ex:
    #         self.logger.error("Request connection error {}".format(str(ex)))
    #         return is_valid
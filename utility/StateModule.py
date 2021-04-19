from ConstantsModule import Constants


class StateManager:
    def __init__(self, logger, sax_manager):
        self.sax_api_manager = sax_manager
        self.logger = logger

    def start_pipeline(self, pipeline_name):
        is_valid = False
        try:
            is_valid = self.sax_api_manager.post_api_response(
                Constants.ENDPOINT_SAX_START_PIPELINE.format(pipeline_name))
            if is_valid:
                self.logger.info("Pipeline -{} has been started.".format(pipeline_name))
            else:
                self.logger.error("Pipeline -{} failed to start.".format(pipeline_name))
            return is_valid
        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return is_valid

    def kill_pipeline(self, pipeline_name):
        is_valid = False
        # try:
        retry_count = 3
        while retry_count >= 1:

            self.logger.info("Pipeline kill attempt- {0}".format(str(4 - retry_count)))
            is_pipeline_killed = self.sax_api_manager.post_api_response(
                Constants.ENDPOINT_SAX_KILL_PIPELINE.format(pipeline_name))
            self.logger.info("Pipeline kill status - {0}".format(str(is_pipeline_killed)))
            if is_pipeline_killed:

                pipeline_status_json = self.sax_api_manager.get_api_response(
                    Constants.ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON.format(pipeline_name))
                self.logger.debug("pipeline_status_json - {0}".format(pipeline_status_json))
                if pipeline_status_json is not None and pipeline_status_json['spark']['pipelines'][0]['status'] == \
                        "STOPPED":
                    self.logger.info("Pipeline -{} has been stopped.".format(pipeline_name))
                    is_valid = True

            if is_valid:
                break

            retry_count = retry_count - 1

        if not is_valid:
            self.logger.error("API is not able to stop the pipeline-{0}.".format(pipeline_name))
            raise Exception("API is not able to stop the pipeline")

        return is_valid

    def restart_pipeline(self, pipeline_name):
        is_pipeline_killed = self.sax_api_manager.post_api_response(
            Constants.ENDPOINT_SAX_KILL_PIPELINE.format(pipeline_name))
        if is_pipeline_killed:

            pipeline_status_json = self.sax_api_manager.get_api_response(
                Constants.ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON.format(pipeline_name))
            pipeline_status = pipeline_status_json['spark']['pipelines'][0]['status']

            self.logger.info("Pipeline {0} current status - {0}".format(pipeline_status))

            if pipeline_status_json is not None and pipeline_status == "STOPPED":
                self.logger.info("Pipeline -{} has been stopped.".format(pipeline_name))
                self.sax_api_manager.post_api_response(Constants.ENDPOINT_SAX_START_PIPELINE.format(pipeline_name))
                self.logger.info("Pipeline -{} has been started.".format(pipeline_name))
            else:
                self.logger.debug("Pipeline detail status Json -{0}".format(pipeline_status_json))
                self.logger.error("API is not able to stop the pipeline-{0}.".format(pipeline_name))
        else:
            self.logger.error("API is not able to stop the pipeline-{0}.".format(pipeline_name))
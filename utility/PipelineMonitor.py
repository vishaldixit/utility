import json
import logging
import sys
from logging import handlers
from StatsModule import OutlierHelper
from LinuxModule import LinuxHelper
from JsonModule import JsonHelper
from FileModule import FileHelper
from StringModule import StringHelper
from HDFSModule import HDFSManager
from ConstantsModule import Constants
from RestApiModule import RestApiManager
from ConfigModule import ConfigManager
from TimeModule import TimeHelper
from HdfsFramework import HDFSFrameworkHelper
from KafkaModule import KafkaManager
from StateModule import StateManager


class PipelineManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager(self.logger, "config.json")
        filename = self.config_manager.get_config_value(Constants.LOG_FILE_NAME)
        log_level = self.config_manager.get_config_value(Constants.LOG_LEVEL)
        log_formatter = self.config_manager.get_config_value(Constants.LOG_FORMATTER)
        log_max_size_mb = int(self.config_manager.get_config_value(Constants.LOG_MAX_SIZE_MB))
        log_file_backup_counts = int(self.config_manager.get_config_value(Constants.LOG_FILE_BACKUP_COUNTS))
        formatter = logging.Formatter(log_formatter)
        logHandler = handlers.RotatingFileHandler(filename, mode='a', maxBytes=log_max_size_mb * 1024 * 1024,
                                                  backupCount=log_file_backup_counts, encoding='utf-8', delay=False)

        logHandler.setLevel(log_level.upper())
        logHandler.setFormatter(formatter)

        # Setting the threshold of logger to DEBUG
        self.logger.setLevel(log_level.upper())
        self.logger.addHandler(logHandler)
        self.hdfs_url = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL)
        self.hdfs_url_standby = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL_STANDBY)
        self.user = self.config_manager.get_config_value(Constants.CONFIG_HDFS_USER)
        self.spark_api_manager = RestApiManager(self.logger,
                                                self.config_manager.get_config_value(Constants.CONFIG_SPARK_API_URL))
        self.logger.info("Initialize the spark_api_manager.")
        self.sax_api_manager = RestApiManager(self.logger,
                                              self.config_manager.get_config_value(Constants.CONFIG_SAX_API_URL),
                                              token=self.config_manager.get_config_value(
                                                  Constants.CONFIG_SAX_API_TOKEN))
        self.json_manager = JsonHelper(self.logger)
        self.string_manager = StringHelper(self.logger)
        self.hdfs_manager = HDFSManager(self.logger)
        self.file_manager = FileHelper(self.logger)
        self.linux_manager = LinuxHelper(self.logger)
        self.stats_helper = OutlierHelper(self.logger)
        self.kafka_manager = KafkaManager(self.logger, self.config_manager)
        self.state_manager = StateManager(self.logger, self.sax_api_manager)


    def is_job_hanged(self, response_app_jobs, config_pipeline_index):
        is_hanged = False
        try:
            current_submission_time = TimeHelper.get_json_value_as_datetime(response_app_jobs, 'submissionTime', 0)
            self.logger.info("Pipeline's current submission time is {0}".format(current_submission_time))
            time_difference_seconds = TimeHelper.get_time_difference_seconds(current_submission_time)
            self.logger.info(
                "Pipeline's job submission time difference in seconds - {0}".format(time_difference_seconds))
            if time_difference_seconds > int(self.config_manager.config_pipelines_data[config_pipeline_index][
                                                 Constants.CONFIG_RESTART_TIME_SECONDS]):
                self.logger.warning(
                    "Pipeline's job in hang state since {0} seconds.".format(time_difference_seconds))
                is_hanged = True

            return is_hanged
        except (ValueError, Exception) as ex:
            self.logger.error("Time conversion error - {0}".format(str(ex)))
            return is_hanged

    # noinspection PyPep8,PyPep8,PyPep8
    def monitor(self):
        try:
            configured_pipelines = self.config_manager.get_config_value(Constants.CONFIG_SAX_MONITORING_PIPELINES)
            pipeline_name = ""
            config_pipeline_index = 0
            for pipeline in configured_pipelines:
                try:
                    self.logger.info("-------------------------Monitoring started---------------------------------")
                    # removed the checkpoint configuration from config
                    # checkpoint_dir_name = str(pipeline['checkpoint']).format(TimeHelper.get_current_epoch_time_seconds())
                    checkpoint_dir_name = ""
                    pipeline_name = pipeline['name']
                    pipeline_detail_json = self.sax_api_manager.get_api_response(
                        Constants.ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON.format(pipeline_name))

                    if len(pipeline_detail_json['spark']['pipelines']) == 0:
                        self.logger.error(
                            "Configured pipeline does not have details in SAX {0}.".format(pipeline_detail_json))
                        continue

                    pipeline_status = pipeline_detail_json['spark']['pipelines'][0]['status']

                    # noinspection PyPep8,PyPep8
                    self.logger.info(
                        "Processing for pipeline name-{0}, pipeline status- {1} ".format(pipeline_name, pipeline_status))

                    if pipeline_status == "STOPPED":
                        pipeline_config_json = self.get_pipeline_config_json(pipeline_name)
                        pending_restart_count_old = int(pipeline_config_json['config']['pendingRestartCount'])
                        result = self.get_updated_pending_restart_count(pipeline_config_json)
                        pipeline_config_json = result[0]
                        is_config_json_updated = result[1]
                        if is_config_json_updated:
                            self.send_update_json_request(pipeline_config_json, pipeline_name)
                        self.updated_current_checkpoint_offsets_v2(pipeline_config_json, config_pipeline_index, pipeline_name)
                        if pending_restart_count_old == 0:
                            self.logger.info("Current pipeline has exhausted the pendicheckpointngRestartCount.")
                            self.logger.info("Going to start the pipeline.")
                            self.state_manager.start_pipeline(pipeline_name)

                    elif pipeline_status == "ACTIVE":
                        pipeline_spark_id = pipeline_detail_json['spark']['pipelines'][0]['id']
                        self.logger.info(
                            "Monitoring started for pipeline name -{0} and job id is- {1}".format(pipeline_name,
                                                                                                  pipeline_spark_id))

                        response_app_jobs = self.spark_api_manager.get_api_response(
                            Constants.ENDPOINT_SPARK_APP_RUNNING_JOBS.format(pipeline_spark_id))



                        if len(response_app_jobs) <= 0:
                            self.logger.info("There is no job initialized.")
                            self.update_pending_restart_count(pipeline_name)
                            continue

                        if self.is_job_hanged(response_app_jobs, config_pipeline_index):
                            pipeline_config_json = self.get_pipeline_config_json(pipeline_name)
                            # pipeline_config_json = self.get_updated_pending_restart_count(pipeline_config_json)
                            result = self.get_updated_pending_restart_count(pipeline_config_json)
                            pipeline_config_json = result[0]
                            is_config_json_updated = result[1]
                            self.state_manager.kill_pipeline(pipeline_name)
                            self.updated_current_checkpoint_offsets_v2(pipeline_config_json, config_pipeline_index, pipeline_name)
                            if is_config_json_updated:
                                self.send_update_json_request(pipeline_config_json, pipeline_name)
                            self.state_manager.start_pipeline(pipeline_name)
                        else:
                            self.update_pipeline_config_json(checkpoint_dir_name, pipeline_name, config_pipeline_index)

                        self.logger.info(
                            "Monitoring ended for pipeline name -{0} and job id is- {1}".format(pipeline_name,
                                                                                                pipeline_spark_id))
                    config_pipeline_index += 1
                except (ValueError, Exception) as ex:
                    self.logger.error(
                        "Monitoring process has got error {0} So skipping process for pipeline - {1}".format(str(ex),
                                                                                                             pipeline_name))

                self.logger.info("------------------------Monitoring Ended---------------------------------")
        except (ValueError, Exception) as ex:
            self.logger.error("Monitoring process has got error {0}".format(str(ex)))
            self.logger.info("------------------------Monitoring Ended---------------------------------")

    # noinspection PyUnusedLocal
    def update_pipeline_config_json(self, checkpoint_dir_name, pipeline_name, config_pipeline_index):
        pipeline_config_json = self.sax_api_manager.get_api_response(
            Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))

        if not (pipeline_config_json['config']['maxRestartCount']):
            self.logger.info("Restart count is not configured properly.")
        else:
            pending_restart_count = int(pipeline_config_json['config']['pendingRestartCount'])
            max_restart_count = int(pipeline_config_json['config']['maxRestartCount'])

            if (max_restart_count - pending_restart_count) > \
                    self.config_manager.config_pipelines_data[config_pipeline_index][
                        Constants.CONFIG_MAX_RESTART_COUNT_DIFFERENCE]:

                self.logger.info("Pipelines existing pending restart count -{0}".format(
                    pipeline_config_json['config']['pendingRestartCount']))
                self.logger.info("Pipelines existing max restart count -{0}".format(
                    pipeline_config_json['config']['maxRestartCount']))

                pipeline_config_json['config']['pendingRestartCount'] = pipeline_config_json['config'][
                    'maxRestartCount']

                self.state_manager.kill_pipeline(pipeline_name)
                self.updated_current_checkpoint_offsets_v2(pipeline_config_json, config_pipeline_index, pipeline_name)
                self.send_update_json_request(pipeline_config_json, pipeline_name)
                self.state_manager.start_pipeline(pipeline_name)

            elif pending_restart_count < max_restart_count:

                self.logger.info("Pipelines existing pending restart count -{0}".format(
                    pipeline_config_json['config']['pendingRestartCount']))
                self.logger.info("Pipelines existing max restart count -{0}".format(
                    pipeline_config_json['config']['maxRestartCount']))

                pipeline_config_json['config']['pendingRestartCount'] = pipeline_config_json['config'][
                    'maxRestartCount']

                self.send_update_json_request(pipeline_config_json, pipeline_name)
            else:
                self.logger.info("Pipeline has same max & pending restart count.")

    def get_updated_json_string(self, json_string, key, old_value, new_vale):
        json_string_old = "\"{0}\":{1}".format(key, old_value)
        self.logger.info("json_string_old {0}".format(json_string_old))
        json_string_new = "\"{0}\":{1}".format(key, new_vale)
        self.logger.info("json_string_new {0}".format(json_string_new))
        json_string_updated = json_string.replace(json_string_old, json_string_new)
        self.logger.debug("Updated Json \n {0}".format(json_string_updated))
        return json_string_updated

    def get_updated_json_string_v2(self, json_string, key, old_value, new_vale):
        json_string_old = "\"{0}\":\"{1}\"".format(key, old_value)
        self.logger.info("json_string_old {0}".format(json_string_old))
        json_string_new = "\"{0}\":\"{1}\"".format(key, new_vale)
        self.logger.info("json_string_new {0}".format(json_string_new))
        json_string_updated = json_string.replace(json_string_old, json_string_new)
        self.logger.debug("Updated Json \n {0}".format(json_string_updated))
        return json_string_updated

    def send_update_json_request(self, pipeline_config_json, pipeline_name):
        # send update pipeline json
        result = self.sax_api_manager.get_api_response_v2(
            Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))
        pipeline_config_json_old = result[0]
        pipeline_config_text_old = result[1]
        self.logger.debug("Raw Config Json \n {0}".format(pipeline_config_text_old))
        pending_restart_count_old = int(pipeline_config_json_old['config']['pendingRestartCount'])
        pending_restart_count_updated = int(pipeline_config_json['config']['pendingRestartCount'])
        string_json_key = "pendingRestartCount"
        pipeline_config_string = self.get_updated_json_string(pipeline_config_text_old, string_json_key,
                                                              pending_restart_count_old, pending_restart_count_updated)

        checkpoint_location_old = self.get_current_checkpoint(pipeline_config_json_old)
        checkpoint_location_new = self.get_current_checkpoint(pipeline_config_json)
        string_json_key = "checkpointLocation"
        pipeline_config_string = self.get_updated_json_string_v2(pipeline_config_string, string_json_key,
                                                                 checkpoint_location_old, checkpoint_location_new)
        self.logger.debug("Payload Config Json \n {0}".format(pipeline_config_string))
        is_valid = self.sax_api_manager.post_api_response_string_payload(
            Constants.ENDPOINT_SAX_UPDATE_PIPELINE_JSON.format(pipeline_name), pipeline_config_string)
        if is_valid:
            self.logger.info("Successfully updated the pipeline Json.")
        else:
            self.logger.error("Failed to update the pipeline json.")

    def get_pipeline_config_json(self, pipeline_name):
        pipeline_config_json = None
        try:
            pipeline_config_json = self.sax_api_manager.get_api_response(
                Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))
            return pipeline_config_json
        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return pipeline_config_json

    def get_updated_pending_restart_count(self, pipeline_config_json):
        is_updated = False
        try:
            if not (pipeline_config_json['config']['maxRestartCount']):
                self.logger.info("Restart count is not configured properly. Skipping Restart count update process")
                return pipeline_config_json

            self.logger.info("Pipelines existing pending restart count -{0}".format(
                pipeline_config_json['config']['pendingRestartCount']))
            self.logger.info("Pipelines existing max restart count -{0}".format(
                pipeline_config_json['config']['maxRestartCount']))

            if int(pipeline_config_json['config']['pendingRestartCount']) == int(pipeline_config_json['config']['maxRestartCount']):
                self.logger.info("Pending and Max count are same.")
            else:
                pipeline_config_json['config']['pendingRestartCount'] = pipeline_config_json['config']['maxRestartCount']

                self.logger.info("Pipelines current pending restart count -{0}".format(
                    pipeline_config_json['config']['pendingRestartCount']))
                self.logger.info("Pipelines current max restart count -{0}".format(
                    pipeline_config_json['config']['maxRestartCount']))
                is_updated = True

            return (pipeline_config_json, is_updated)
        except (ConnectionError, Exception) as ex:
            self.logger.error("Request connection error {}".format(str(ex)))
            return (pipeline_config_json, is_updated)

    def get_current_checkpoint(self, pipeline_config_json):
        try:
            current_checkpoint_directory = ""

            if 'processors' not in pipeline_config_json:
                raise ValueError("No processors element in given data")

            index = 0
            is_exist = False
            for processor in pipeline_config_json['processors']:
                if self.sax_api_manager.is_json_element_exist(processor):
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

    def updated_current_checkpoint_offsets_v2(self, pipeline_config_json, config_pipeline_index, pipeline_name):
        self.logger.info("Inside updated_current_checkpoint_offsets")

        current_checkpoint_directory = self.get_current_checkpoint(pipeline_config_json)
        current_checkpoint_directory_with_offset = current_checkpoint_directory + "/offsets/"
        self.logger.info(
            "Current checkpoint directory with offset - {0}".format(current_checkpoint_directory_with_offset))

        hdfs_connection = self.hdfs_manager.get_connection(self.hdfs_url, self.hdfs_url_standby, self.user)
        # is_offset_dir_exist = self.hdfs_manager.is_hdfs_file_exist(current_checkpoint_directory_with_offset)
        is_offset_dir_exist = self.hdfs_manager.is_path_exists(hdfs_connection,
                                                               current_checkpoint_directory_with_offset)
        if not is_offset_dir_exist:
            self.logger.error("Offset directory not exist within checkpoint directory")
            raise ValueError("Offset directory not exist within checkpoint directory")
        self.logger.info("Offset directory is exist within checkpoint directory")

        # ls_result = self.linux_manager.process_command(self.string_manager.get_command_as_list(
        #     Constants.COMMAND_HDFS_DIRECTORY_LS.format(current_checkpoint_directory_with_offset)))
        # self.logger.info("HDFS LS command result \n {0}".format(ls_result))

        #hdfs_latest_offset_file_path = self.string_manager.get_latest_checkpoint_file(ls_result)
        latest_dir_local = self.hdfs_manager.get_latest_offset_dir(current_checkpoint_directory_with_offset,
                                                             hdfs_connection)
        self.logger.info("hdfs_latest_offset_file_path = {}".format(latest_dir_local))

        offset_data_latest = None
        offset_content_latest = None
        offset_file_current_lines = None
        while (offset_data_latest is None) and (offset_data_latest is None) and latest_dir_local >= 0:
            offset_dir_path = current_checkpoint_directory_with_offset + str(latest_dir_local)
            try:

                self.logger.info("offset_dir_path = {0}".format(offset_dir_path))
                result = self.hdfs_manager.get_offset_data(offset_dir_path, hdfs_connection)
                offset_data_latest = result[0]
                self.logger.info("offset_data_latest = {0}".format(offset_data_latest))
                offset_content_latest = result[1]
                offset_file_current_lines = offset_content_latest.split('\n')
            except (ValueError, Exception) as ex:
                self.logger.warning("Offset file corrupted for path {0}.".format(offset_dir_path))
                # self.hdfs_manager.delete_offset_directory(offset_dir_path, hdfs_connection)
                self.handle_failure(hdfs_connection, current_checkpoint_directory_with_offset, pipeline_name,
                                    latest_dir_local - 1)
                latest_dir_local = latest_dir_local - 1
                offset_dir_path = current_checkpoint_directory_with_offset + str(latest_dir_local)

        if not self.hdfs_manager.is_latest_file_format_valid_lines(offset_file_current_lines):
            self.logger.error("Offset file is not in right format. Aborting pipeline progress.")
            raise ValueError("Offset file corrupted.")

        skip_offset_value = int(
            self.config_manager.config_pipelines_data[config_pipeline_index][
                Constants.CONFIG_OFFSET_SKIP_PER_PARTITION])
        self.logger.info("skip_offset_value = {0}".format(str(skip_offset_value)))
        result_offset_lines = self.get_updated_offset_lines(offset_file_current_lines,
                                                                  Constants.SKIP_OFFSET_FILE_LINES,
                                                                  skip_offset_value)
        offset_file_updated_lines = result_offset_lines[0]
        is_offset_treatment_applied = result_offset_lines[1]
        self.logger.info("offset_file_updated_lines = {0}".format(offset_file_updated_lines))

        if not is_offset_treatment_applied:
            self.logger.info("There is no treatement applied and offset file update not required.")
        else:
            is_success = self.hdfs_manager.write_hdfs_wrapper(hdfs_connection, offset_dir_path,
                                                              offset_file_updated_lines)
            if not is_success:
                self.logger.error("Not able to write the file.")
                raise IOError("Not able to write the file.")
            self.logger.info("HDFS Checkpoint offset updated successfully.")


    # def handle_failure(self, client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
    #                    previous_offset):
    #     # kill the pipeline
    #     is_killed = self.state_manager.kill_pipeline(pipeline_name_local)
    #     self.logger.info("Pipeline has been stopped.")
    #     if is_killed:
    #         # get latest directory again
    #         self.hdfs_manager.delete_invalid_directories(current_checkpoint_directory_with_offset_local,
    #                                                      previous_offset,
    #                                                      client)

    def handle_failure(self, client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                       previous_offset, is_kill_activate=False, is_start_activate=False):
        if is_kill_activate:
            # kill the pipeline
            is_killed = self.state_manager.kill_pipeline(pipeline_name_local)
            self.logger.info("Pipeline has been stopped.")
            if not is_killed:
                self.logger.error("Planned pipeline killed operation failed.")
                raise ValueError("Not able to kill pipeline planned way.")

        self.hdfs_manager.delete_invalid_directories(current_checkpoint_directory_with_offset_local,
                                                     previous_offset,
                                                     client)
        if is_start_activate:
            is_started = self.state_manager.start_pipeline(pipeline_name_local)
            self.logger.info("Pipeline has been started.")
            if not is_started:
                self.logger.error("Planned pipeline started operation failed.")
                raise ValueError("Not able to start pipeline planned way.")


    def updated_current_checkpoint_offsets(self, pipeline_config_json, config_pipeline_index):
        self.logger.info("Inside updated_current_checkpoint_offsets")

        current_checkpoint_directory = self.get_current_checkpoint(pipeline_config_json)
        current_checkpoint_directory_with_offset = current_checkpoint_directory + "/offsets/"
        self.logger.info(
            "Current checkpoint directory with offset - {0}".format(current_checkpoint_directory_with_offset))

        is_offset_dir_exist = self.hdfs_manager.is_hdfs_file_exist(current_checkpoint_directory_with_offset)
        if not is_offset_dir_exist:
            self.logger.error("Offset directory not exist within checkpoint directory")
            raise ValueError("Offset directory not exist within checkpoint directory")
        self.logger.info("Offset directory is exist within checkpoint directory")

        ls_result = self.linux_manager.process_command(self.string_manager.get_command_as_list(
            Constants.COMMAND_HDFS_DIRECTORY_LS.format(current_checkpoint_directory_with_offset)))
        self.logger.info("HDFS LS command result \n {0}".format(ls_result))

        hdfs_latest_offset_file_path = self.string_manager.get_latest_checkpoint_file(ls_result)
        self.logger.info("hdfs_latest_offset_file_path = {}".format(hdfs_latest_offset_file_path))

        # download offset hdfs file locally
        self.linux_manager.process_command(
            self.string_manager.get_command_as_list(
                Constants.COMMAND_HDFS_GET_FILE.format(hdfs_latest_offset_file_path)))
        local_offset_file_path = self.string_manager.get_file_name_from_path(hdfs_latest_offset_file_path)
        self.logger.info("Latest offset file downloaded, File Name = {0}".format(local_offset_file_path))

        offset_file_current_lines = self.file_manager.read_file_lines(local_offset_file_path)
        self.logger.info("offset_file_current_lines = {0}".format(offset_file_current_lines))
        if len(offset_file_current_lines) < 3:
            self.logger.error("Offset file is not in right format. Aborting pipeline progress.")
            raise ValueError("Offset file corrupted.")

        skip_offset_value = int(
            self.config_manager.config_pipelines_data[config_pipeline_index][
                Constants.CONFIG_OFFSET_SKIP_PER_PARTITION])
        self.logger.info("skip_offset_value = {0}".format(str(skip_offset_value)))
        offset_file_updated_lines = self.get_updated_offset_lines(offset_file_current_lines,
                                                                  Constants.SKIP_OFFSET_FILE_LINES,
                                                                  skip_offset_value)
        self.logger.info("offset_file_updated_lines = {0}".format(offset_file_updated_lines))
        if len(offset_file_updated_lines) < 3:
            self.logger.error("Offset file got corrupted while updating the offsets. Aborting pipeline progress.")
            raise ValueError("Offset file corrupted.")

        is_success = self.file_manager.write_file_lines(local_offset_file_path, offset_file_updated_lines)
        if not is_success:
            self.logger.error("Not able to write the file.")
            raise IOError("Not able to write the file.")
        self.logger.info("Successfully updated the local offset file.")

        # upload the checkpoint file into hdfs
        self.linux_manager.process_command(self.string_manager.get_command_as_list(
            Constants.COMMAND_HDFS_PUT_FILE.format(local_offset_file_path, current_checkpoint_directory_with_offset)))
        self.logger.info("Checkpoint offset skip updated successfully.")

    def dummy_kafka_offset(self, offset_data):
        dummy_kafka_earlier_dict = {}
        dummy_kafka_latest_dict = {}
        earlier = 2446
        latest = 19
        for key in offset_data.keys():
            if (offset_data[key] - earlier) <= 0:
                dummy_kafka_earlier_dict[key] = 0
            else:
                dummy_kafka_earlier_dict[key] = offset_data[key] - earlier

            dummy_kafka_latest_dict[key] = offset_data[key] + latest
        return dummy_kafka_earlier_dict, dummy_kafka_latest_dict

    def is_any_treatment_applied(self, offset_treatment_dict):
        is_treatment_applied = False
        if (1 in offset_treatment_dict.values()) or (2 in offset_treatment_dict.values()) or (
                3 in offset_treatment_dict.values()):
            is_treatment_applied = True
        return is_treatment_applied


    # noinspection PyPep8
    def get_updated_offset_lines(self, data_lines, skip_file_lines, skip_offset_value):
        index = 0
        self.logger.info("Length of offset file lines {0}".format(len(data_lines)))
        # noinspection PyUnusedLocal
        offset_data = {}
        while index < len(data_lines):
            if index + 1 <= skip_file_lines:
                index += 1
                continue
            self.logger.info("current topic offset details {0}".format(data_lines[index]))
            if not self.json_manager.is_json(data_lines[index]):
                self.logger.error("current offset line is not valid json")
                raise Exception("Not a valid Json.")

            offset_data = json.loads(data_lines[index])

            for topic in offset_data.keys():
                kafka_earliest_offset_dict = {}
                kafka_largest_offset_dict = {}

                # if self.logger.level == logging.NOTSET:
                #     dummy_offset_result = self.dummy_kafka_offset(offset_data[topic])
                #     kafka_earliest_offset_dict = dummy_offset_result[0]
                #     kafka_largest_offset_dict = dummy_offset_result[1]
                # else:

                # kafka_earliest_offset_dict = self.get_earliest_kafka_offsets(topic)
                kafka_earliest_offset_dict = self.kafka_manager.get_beginning_offsets(topic)
                # kafka_largest_offset_dict = self.get_latest_kafka_offsets(topic)
                kafka_largest_offset_dict = self.kafka_manager.get_end_offsets(topic)


                spark_current_offset_dict = offset_data[topic]
                self.logger.info("Spark current offset - {0}".format(spark_current_offset_dict))
                # 0- No treatment,
                # 1 - Kafka offset treatment,
                # 2 - outlier treatment,
                # 3 - skip value treatment
                offset_treatment = {}

                offset_difference_outlier_check = {}
                final_offset = {}

                # Kafka treatment

                self.logger.info("Going to apply Kafka treatment")
                for partition in spark_current_offset_dict.keys():
                    self.logger.info("Partition value - {0}".format(partition))
                    self.logger.info("spark_current_offset_dict value - {0}".format(spark_current_offset_dict[partition]))
                    possible_updated_value = int(spark_current_offset_dict[partition]) + skip_offset_value
                    self.logger.info("spark_current_offset_dict + skip_offset_value value - {0}".format(str(possible_updated_value)))
                    # noinspection PyPep8
                    if possible_updated_value < int(kafka_earliest_offset_dict[partition]):
                        self.logger.info("kafka_earliest_offset_dict value - {0}".format(kafka_earliest_offset_dict[partition]))
                        final_offset[partition] = kafka_earliest_offset_dict[partition]
                        offset_treatment[partition] = 1

                    elif possible_updated_value > int(kafka_largest_offset_dict[partition]):
                        self.logger.info("kafka_largest_offset_dict value - {0}".format(kafka_largest_offset_dict[partition]))
                        final_offset[partition] = kafka_largest_offset_dict[partition]
                        offset_treatment[partition] = 1
                    else:
                        offset_treatment[partition] = 0
                        offset_difference_outlier_check[spark_current_offset_dict[partition]] = \
                            kafka_largest_offset_dict[partition] - spark_current_offset_dict[
                                partition]

                if len(offset_treatment.keys()) > 0:
                    self.logger.info("Offset treatment values \n{0}".format(offset_treatment))
                if len(offset_treatment.keys()) > 0:
                    self.logger.info("final offset values \n {0}".format(final_offset))
                if len(offset_treatment.keys()) > 0:
                    self.logger.info(
                        "offset_difference_outlier_check value \n {0}".format(offset_difference_outlier_check))



                # Outlier treatment
                if len(spark_current_offset_dict.keys()) > 2 :

                    self.logger.info("Going to apply Outlier treatment")
                    if len(offset_difference_outlier_check.values()) <= 0:
                        self.logger.info("There is no other treatment required further.")
                        # --------------
                        self.logger.info("Offset data - {0}".format(offset_data))
                        offset_data[topic] = final_offset
                        self.logger.info("Before data lines - {0}".format(data_lines))
                        data_lines[index] = json.dumps(offset_data)
                        self.logger.info("Updated data lines - {0}".format(data_lines))
                        self.logger.info("Updated topic offset details {0}".format(data_lines[index]))
                        # --------------
                        # data_lines[index] = json.dumps(final_offset)
                        # self.logger.info("updated topic offset details {0}".format(data_lines[index]))
                        return (data_lines, self.is_any_treatment_applied(offset_treatment))

                    outlier_list = []
                    if self.logger.level != logging.NOTSET:
                        outlier_list = self.stats_helper.get_outlier(list(offset_difference_outlier_check.values()))

                    self.logger.info("Outliers - {0}".format(str(outlier_list)))
                    for key in offset_difference_outlier_check.keys():
                        if offset_difference_outlier_check[key] in outlier_list:
                            offset_difference_outlier_check.pop(key)
                    min_value = min(offset_difference_outlier_check.keys())
                    self.logger.info("Minimum offset value - {0}".format(str(min_value)))
                    max_value = max(offset_difference_outlier_check.keys())
                    self.logger.info("Maximum offset value - {0}".format(str(max_value)))

                    if len(offset_difference_outlier_check.values()) <= 0:
                        self.logger.info("There is no other treatment required further.")
                        # data_lines[index] = json.dumps(final_offset)
                        # self.logger.info("updated topic offset details {0}".format(data_lines[index]))
                        # return data_lines[index]
                        # --------------
                        self.logger.info("Offset data - {0}".format(offset_data))
                        offset_data[topic] = final_offset
                        self.logger.info("Before data lines - {0}".format(data_lines))
                        data_lines[index] = json.dumps(offset_data)
                        self.logger.info("Updated data lines - {0}".format(data_lines))
                        self.logger.info("Updated topic offset details {0}".format(data_lines[index]))
                        return (data_lines, self.is_any_treatment_applied(offset_treatment))

                    for key in spark_current_offset_dict.keys():
                        if spark_current_offset_dict[key] in offset_difference_outlier_check.keys():
                            if (spark_current_offset_dict[key] < min_value):

                                if (kafka_largest_offset_dict[key] <= min_value):
                                    final_offset[key] = kafka_largest_offset_dict[key]
                                else:
                                    final_offset[key] = min_value

                                offset_treatment[key] = 2
                            elif spark_current_offset_dict[key] > max_value:

                                if (kafka_largest_offset_dict[key] <= max_value):
                                    final_offset[key] = kafka_largest_offset_dict[key]
                                else:
                                    final_offset[key] = max_value

                                offset_treatment[key] = 2

                    if len(offset_treatment.keys()) > 0:
                        self.logger.info("Offset treatment values \n{0}".format(offset_treatment))
                    if len(offset_treatment.keys()) > 0:
                        self.logger.info("final offset values \n {0}".format(final_offset))

                # apply skip value treatments
                self.logger.info("Going to apply Skip Value treatment")
                for key in spark_current_offset_dict.keys():
                    if offset_treatment[key] == 0:
                        final_offset[key] = spark_current_offset_dict[key] + skip_offset_value
                        offset_treatment[key] = 3

                if len(offset_treatment.keys()) > 0:
                    self.logger.info("Offset treatment values \n{0}".format(offset_treatment))
                if len(offset_treatment.keys()) > 0:
                    self.logger.info("final offset values \n {0}".format(final_offset))

                self.logger.info("Before Offset data - {0}".format(offset_data))
                offset_data[topic] = final_offset
                self.logger.info("After Offset data - {0}".format(offset_data))

            self.logger.info("Offset data - {0}".format(offset_data))
            offset_data[topic] = final_offset
            self.logger.info("Before data lines - {0}".format(data_lines))
            data_lines[index] = json.dumps(offset_data)
            self.logger.info("Updated data lines - {0}".format(data_lines))
            self.logger.info("Updated topic offset details {0}".format(data_lines[index]))
            index += 1
        return (data_lines, self.is_any_treatment_applied(offset_treatment))

    # noinspection PyMethodMayBeStatic
    def get_dictionary_key(self, offset_dict, searching_value):
        for key, value in offset_dict.items():
            if searching_value == value:
                return key

        return "key doesn't exist"




    def update_pending_restart_count(self, app_name):
        # get pipeline json
        pipeline_json = self.sax_api_manager.get_api_response(
            Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(app_name))
        self.logger.debug("Received the pipeline config json - {}".format(pipeline_json))
        # update json with pending count equals to max retry count
        if pipeline_json is not None:

            if not (pipeline_json['config']['maxRestartCount']):
                self.logger.info("Restart count is not configured properly.")
                return

            if int(pipeline_json['config']['pendingRestartCount']) < int(pipeline_json['config']['maxRestartCount']):

                self.logger.info("Pipelines existing pending restart count -{0}".format(
                    pipeline_json['config']['pendingRestartCount']))
                self.logger.info("Pipelines existing max restart count -{0}".format(
                    pipeline_json['config']['maxRestartCount']))

                pipeline_json['config']['pendingRestartCount'] = pipeline_json['config']['maxRestartCount']

                # send update pipeline json
                is_valid = self.sax_api_manager.post_api_response(
                    Constants.ENDPOINT_SAX_UPDATE_PIPELINE_JSON.format(app_name), pipeline_json)
                if is_valid:
                    self.logger.info("Successfully updated the pipeline Json for pending restart.")
                else:
                    self.logger.error("Failed to update the pipeline json for pending restart.")
            else:
                self.logger.info("Pipeline has same max & pending restart count.")

    def update_pending_restart_count_mannually(self, app_name, pipeline_json,  pending_start_count):
        # get pipeline json

        # update json with pending count equals to max retry count
        if pipeline_json is not None:

            if not (pipeline_json['config']['maxRestartCount']):
                self.logger.debug("Restart count is not configured properly.")
                return

            pipeline_json['config']['pendingRestartCount'] = pending_start_count

            # send update pipeline json
            is_valid = self.sax_api_manager.post_api_response(
                Constants.ENDPOINT_SAX_UPDATE_PIPELINE_JSON.format(app_name), pipeline_json)
            if is_valid:
                self.logger.debug("Successfully updated the pipeline Json for pending restart.")
            else:
                self.logger.debug("Failed to update the pipeline json for pending restart.")


    def manually_update_pending_start_count(self):
        pipeline_config_json = self.get_pipeline_config_json("WireView_V109_54")
        pending_restart_count_old = int(pipeline_config_json['config']['pendingRestartCount'])
        self.update_pending_restart_count_mannually(pipeline_config_json, pipeline_config_json, 1)



def main(args):
    #live_monitoring = HDFSFrameworkHelper()
    if len(args) >1 and str(args[1]).lower() ==  'active':
        live_monitoring = HDFSFrameworkHelper()
        live_monitoring.process()
    else:
        #live_monitoring = None
        pipeline_manager = PipelineManager()
        pipeline_manager.monitor()
    sys.exit()


def offset_test():
    pipeline_manager = PipelineManager()
    pipeline_manager.manually_update_pending_start_count()
    #offset_file_current_lines = pipeline_manager.file_manager.read_file_lines("1")
    #pipeline_manager.logger.info("offset_file_current_lines = {0}".format(offset_file_current_lines))

    # skip_offset_value = int(
    #     pipeline_manager.config_manager.get_config_value(Constants.CONFIG_SPARK_OFFSET_SKIP_PER_PARTITION))
    # corrupt_offset_difference = int(
    #     pipeline_manager.config_manager.get_config_value(Constants.CONFIG_SPARK_CORRUPT_OFFSET_DIFFERENCE))
    # get_updated_offset_lines(self, data_lines, skip_file_lines, skip_offset_value):
    # offset_file_updated_lines = pipeline_manager.get_updated_offset_lines(offset_file_current_lines,
    #                                                                       Constants.SKIP_OFFSET_FILE_LINES,
    #                                                                       5)
    sys.exit()


if __name__ == '__main__':
    main(sys.argv)
    #offset_test()


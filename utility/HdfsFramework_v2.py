import logging
import json
import sys
import time
from logging import handlers
from hdfs import InsecureClient
from ConfigModule import ConfigManager
from ConstantsModule import Constants
from JsonModule import JsonHelper
from RestApiModule import RestApiManager


class HDFSFrameworkHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager(self.logger, "./config.json")
        filename = self.config_manager.get_config_value(Constants.LOG_FILE_NAME)
        log_level = self.config_manager.get_config_value(Constants.LOG_LEVEL)
        log_formatter = self.config_manager.get_config_value(Constants.LOG_FORMATTER)
        log_max_size_mb = int(self.config_manager.get_config_value(Constants.LOG_MAX_SIZE_MB))
        log_file_backup_counts = int(self.config_manager.get_config_value(Constants.LOG_FILE_BACKUP_COUNTS))
        formatter = logging.Formatter(log_formatter)
        filename = "{0}-live.log".format(filename.split('.')[0])
        logHandler = handlers.RotatingFileHandler(filename, mode='a', maxBytes=log_max_size_mb * 1024 * 1024,
                                                  backupCount=log_file_backup_counts, encoding='utf-8', delay=False)

        logHandler.setLevel(log_level.upper())
        logHandler.setFormatter(formatter)

        # Setting the threshold of logger to DEBUG
        self.logger.setLevel(log_level.upper())
        self.logger.addHandler(logHandler)

        self.logger.info("-------------------------Live Monitoring started---------------------------------")
        self.sax_api_manager = RestApiManager(self.logger,
                                              self.config_manager.get_config_value(Constants.CONFIG_SAX_API_URL),
                                              token=self.config_manager.get_config_value(
                                                  Constants.CONFIG_SAX_API_TOKEN))
        self.json_manager = JsonHelper(self.logger)
        self.hdfs_url = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL)
        self.hdfs_url_standby = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL_STANDBY)
        self.user = self.config_manager.get_config_value(Constants.CONFIG_HDFS_USER)
        self.live_monitoring_delay_seconds = self.config_manager.get_config_value(
            Constants.CONFIG_LIVE_MONITORING_DELAY_SECONDS)
        self.pipeline_offset_dict = {}

    # hdfs connection
    # def get_connection(self):
    #     client = InsecureClient(self.hdfs_url, user=self.user)
    #     if not self.is_path_exists(client, '/'):
    #         client = InsecureClient(self.hdfs_url_standby, user=self.user)
    #     self.logger.info("Hdfs connection created successfully.")
    #     return client

    # hdfs offset data
    # def get_offset_data(self, offset_dir_path, client):
    #     data = None
    #     content = ""
    #     with client.read(offset_dir_path, encoding='utf-8', length=1048576) as reader:
    #         content = reader.read()
    #         values = content.split('\n')
    #         self.logger.info("offset content ={0}".format(str(content)))
    #         if len(values) >= 3:
    #             data = json.loads(values[2])
    #             self.logger.info("offset data dict ={0}".format(data))
    #         else:
    #             raise ValueError("Offset file corrupted.")
    #     return data, content

    # hdfs data offset validation
    # def is_latest_offset_valid(self, offset_data_latest, offset_data_previous):
    #     is_valid = True
    #     for topic in offset_data_latest.keys():
    #         previous_keys = offset_data_previous[topic].keys()
    #         self.logger.info("previous offsets ={0}".format(previous_keys))
    #         for partition in offset_data_latest[topic].keys():
    #             if partition in previous_keys:
    #                 if int(offset_data_latest[topic][partition]) < int(offset_data_previous[topic][partition]):
    #                     is_valid = False
    #                     self.logger.warning("latest offset has has issue.")
    #                     self.logger.info("offset_data_latest[{0}][{1}] = {2}".format(topic, partition,
    #                                                                                  offset_data_latest[topic][
    #                                                                                      partition]))
    #                     self.logger.info("offset_data_previous[{0}][{1}] = {2}".format(topic, partition,
    #                                                                                    offset_data_previous[topic][
    #                                                                                        partition]))
    #                     break
    #     return is_valid

    # def is_valid_dir(self, dir_name):
    #     is_valid = True
    #     try:
    #         value = int(dir_name)
    #     except:
    #         self.logger.warning("Not a valid directory = {0}  Discarding this directory".format(dir_name))
    #         is_valid = False
    #     return is_valid

    # def get_latest_offset_dir(self, current_checkpoint_directory_with_offset, client):
    #     dirs = client.list(current_checkpoint_directory_with_offset)
    #     test_list = []
    #     for offset_file_name in dirs:
    #         if self.is_valid_dir(offset_file_name):
    #             test_list.append(int(offset_file_name))
    #     # test_list = [int(i) for i in dirs]
    #     test_list.sort(reverse=True)
    #     latest_dir = int(test_list[0])
    #     self.logger.info("Latest dir name - {0}".format(latest_dir))
    #     return latest_dir

    # def delete_invalid_directories(self, checkpoint_directory_with_offset, previous_correct_file_number, connection):
    #     current_offset_dirs = connection.list(checkpoint_directory_with_offset)
    #     current_offset_files = [int(i) for i in current_offset_dirs]
    #     current_offset_files.sort(reverse=True)
    #
    #     offsets_delete_list = [offset_file_number for offset_file_number in current_offset_files if
    #                            offset_file_number > previous_correct_file_number]
    #     self.logger.info("offsets_delete_list - {0}".format(offsets_delete_list))
    #
    #     for offset_file in offsets_delete_list:
    #         offset_path = checkpoint_directory_with_offset + str(offset_file)
    #         connection.delete(offset_path, recursive=True)
    #         self.logger.info("Directory deleted - {0}".format(offset_path))

    def process_data(self, latest_dir, current_checkpoint_directory_with_offset, pipeline_name, client):
        latest_dir_local = int(latest_dir)
        current_checkpoint_directory_with_offset_local = str(current_checkpoint_directory_with_offset)
        pipeline_name_local = str(pipeline_name)
        self.logger.info("Starting processing data....")
        self.logger.info(
            "Input values latest_dir= {0}, current_checkpoint_directory_with_offset= {1}, pipeline_name= {2}".format(
                latest_dir_local, current_checkpoint_directory_with_offset_local, pipeline_name_local))
        offset_dict = {}
        try:
            offset_dir_path = current_checkpoint_directory_with_offset_local + str(latest_dir_local)
            self.logger.info("offset_dir_path = {0}".format(offset_dir_path))
            result = self.get_offset_data(offset_dir_path, client)
            offset_data_latest = result[0]
            self.logger.info("offset_data_latest = {0}".format(offset_data_latest))
            offset_content_latest = result[1]
            self.logger.info("offset_content_latest = {0}".format(offset_content_latest))
            if (not self.pipeline_offset_dict) or (not pipeline_name_local in self.pipeline_offset_dict):
                self.logger.info("Initialize the directory")
                if self.is_latest_file_format_valid(offset_content_latest):
                    offset_dict[latest_dir_local] = (offset_data_latest, offset_content_latest)
                    self.pipeline_offset_dict[pipeline_name_local] = offset_dict
                    self.logger.debug("pipeline_offset_dict = {0}".format(self.pipeline_offset_dict))
                else:
                    raise ValueError("Offset file corrupted.")
            else:
                offset_dict_previous = self.pipeline_offset_dict[pipeline_name_local]
                previous_offset = max(offset_dict_previous.keys())
                self.logger.info("Previous max offset file name = {0}".format(previous_offset))
                offset_data_previous = offset_dict_previous[previous_offset][0]
                # offset_content_previous = offset_dict_previous[previous_offset][1]
                self.logger.debug("offset_data_previous = {0}".format(offset_data_previous))

                if not self.is_latest_file_format_valid(offset_content_latest):
                    self.handle_failure(client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                                        previous_offset)
                elif self.is_latest_offset_valid(offset_data_latest, offset_data_previous):
                    offset_dict[latest_dir_local] = (offset_data_latest, offset_content_latest)
                    self.pipeline_offset_dict[pipeline_name_local] = offset_dict
                    self.logger.info("Latest offset is valid - {0}".format(latest_dir_local))
                else:
                    self.handle_failure(client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                                        previous_offset)

        except (ValueError, Exception) as ex:
            print("Monitoring process has got error {0}".format(str(ex)))

    # def is_latest_file_format_valid(self, offset_content_latest):
    #     is_valid = True
    #     try:
    #         offset_file_current_lines = offset_content_latest.split('\n')
    #         if len(offset_file_current_lines) < 3:
    #             self.logger.error("Offset file is not in right format. Offset file corrupted.")
    #             is_valid = False
    #     except (ValueError, Exception) as ex:
    #         self.logger.error(
    #             "Offset file is not in right format. Offset file corrupted. File content as below\n {0}".format(
    #                 offset_content_latest))
    #         is_valid = False
    #     return is_valid

    def handle_failure(self, client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                       previous_offset):
        # kill the pipeline
        is_killed = self.kill_pipeline(pipeline_name_local)
        self.logger.info("Pipeline has been stopped.")
        if is_killed:
            # get latest directory again
            self.delete_invalid_directories(current_checkpoint_directory_with_offset_local, previous_offset,
                                            client)

            # self.current_file_update_treatment(client, current_checkpoint_directory_with_offset_local,
            #                                    offset_data_previous, offset_dict, offset_dict_previous,
            #                                    pipeline_name_local, previous_offset)
            self.start_pipeline(pipeline_name_local)
            self.logger.info("Pipeline has been started.")

    # def current_file_update_treatment(self, client, current_checkpoint_directory_with_offset_local,
    #                                   offset_data_previous, offset_dict, offset_dict_previous, pipeline_name_local,
    #                                   previous_offset):
    #     latest_dir_local = self.get_latest_offset_dir(current_checkpoint_directory_with_offset_local,
    #                                                   client)
    #     self.logger.debug("latest_dir_local = {0}".format(latest_dir_local))
    #     offset_dir_path = current_checkpoint_directory_with_offset_local + str(latest_dir_local)
    #     self.logger.debug("offset_dir_path = {0}".format(offset_dir_path))
    #     offset_content_previous = offset_dict_previous[previous_offset][1]
    #     self.logger.debug("offset_content_previous = {0}".format(offset_content_previous))
    #     # update the latest content
    #     client.write(offset_dir_path, encoding='utf-8', overwrite=True, data=offset_content_previous)
    #     self.logger.info("Previous content has been updated into latest offset dir.")
    #     offset_dict[latest_dir_local] = (offset_data_previous, offset_content_previous)
    #     self.pipeline_offset_dict[pipeline_name_local] = offset_dict
    #     self.logger.info("Stored dict updated")

    def process(self):
        try:
            client = self.get_connection()

            configured_pipelines = self.config_manager.get_config_value(Constants.CONFIG_SAX_MONITORING_PIPELINES)
            while True:
                for pipeline in configured_pipelines:
                    try:
                        pipeline_name = pipeline['name']
                        pipeline_config_json = self.get_pipeline_config_json(pipeline_name)

                        current_checkpoint_directory = self.json_manager.get_current_checkpoint(pipeline_config_json)

                        current_checkpoint_directory_with_offset = current_checkpoint_directory + "/offsets/"
                        self.logger.info(
                            "Current checkpoint directory with offset - {0}".format(
                                current_checkpoint_directory_with_offset))

                        is_offset_dir_exist = False
                        try:
                            file_status = client.status(current_checkpoint_directory_with_offset)
                            is_offset_dir_exist = True
                        except (ValueError, Exception) as ex:
                            is_offset_dir_exist = False

                        if not is_offset_dir_exist:
                            self.logger.error("Offset directory not exist within checkpoint directory")
                            raise ValueError("Offset directory not exist within checkpoint directory")
                        self.logger.info("Offset directory is exist within checkpoint directory")

                        latest_dir = self.get_latest_offset_dir(current_checkpoint_directory_with_offset, client)
                        self.process_data(latest_dir, current_checkpoint_directory_with_offset, pipeline_name, client)
                        # self.remove_previous_keys(pipeline_name)

                    except (ValueError, Exception) as ex:
                        self.logger.error("Monitoring process has got error {0}".format(str(ex)))

                time.sleep(int(self.live_monitoring_delay_seconds))
        except (ValueError, Exception) as ex:
            self.logger.error("Monitoring process has got error {0}".format(str(ex)))
            self.logger.info("------------------------Monitoring Ended---------------------------------")

    # def is_path_exists(self, connection, path):
    #     is_offset_dir_exist = True
    #     try:
    #         file_status = connection.status(path)
    #     except (ValueError, Exception) as ex:
    #         is_offset_dir_exist = False
    #     return is_offset_dir_exist

    # Not in Use
    # def remove_previous_keys(self, pipeline_name):
    #     current_offset_dirs = self.pipeline_offset_dict[pipeline_name].keys()
    #     current_offset_files = [int(i) for i in current_offset_dirs]
    #     current_offset_files.sort(reverse=True)
    #
    #     if len(current_offset_files) > 10:
    #         for index in range(9, len(current_offset_files) - 1):
    #             self.pipeline_offset_dict[pipeline_name].pop(current_offset_files[index])

    # moved in Rest API
    # def get_pipeline_config_json(self, pipeline_name):
    #     pipeline_config_json = None
    #     try:
    #         pipeline_config_json = self.sax_api_manager.get_api_response(
    #             Constants.ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON.format(pipeline_name))
    #         return pipeline_config_json
    #     except (ConnectionError, Exception) as ex:
    #         self.logger.error("Request connection error {}".format(str(ex)))
    #         return pipeline_config_json

    # moved into state module
    # def start_pipeline(self, pipeline_name):
    #     is_valid = False
    #     try:
    #         is_valid = self.sax_api_manager.post_api_response(
    #             Constants.ENDPOINT_SAX_START_PIPELINE.format(pipeline_name))
    #         if is_valid:
    #             self.logger.info("Pipeline -{} has been started.".format(pipeline_name))
    #         else:
    #             self.logger.error("Pipeline -{} failed to start.".format(pipeline_name))
    #         return is_valid
    #     except (ConnectionError, Exception) as ex:
    #         self.logger.error("Request connection error {}".format(str(ex)))
    #         return is_valid

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


def main():
    live_monitoring = HDFSFrameworkHelper()
    live_monitoring.process()
    sys.exit()


if __name__ == '__main__':
    main()

import logging
import sys
import time
from logging import handlers
from ConfigModule import ConfigManager
from ConstantsModule import Constants
from JsonModule import JsonHelper
from RestApiModule import RestApiManager
from HDFSModule import HDFSManager
from StateModule import StateManager


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
        self.hdfs_manager = HDFSManager(self.logger)
        self.state_manager = StateManager(self.logger, self.sax_api_manager)
        self.hdfs_url = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL)
        self.hdfs_url_standby = self.config_manager.get_config_value(Constants.CONFIG_HDFS_URL_STANDBY)
        self.user = self.config_manager.get_config_value(Constants.CONFIG_HDFS_USER)
        self.live_monitoring_delay_seconds = self.config_manager.get_config_value(
            Constants.CONFIG_LIVE_MONITORING_DELAY_SECONDS)
        self.pipeline_offset_dict = {}

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
            # --------
            offset_data_latest = None
            offset_content_latest = None
            while (offset_data_latest is None) and (offset_data_latest is None) and latest_dir_local >= 0:
                offset_dir_path = current_checkpoint_directory_with_offset_local + str(latest_dir_local)
                try:

                    self.logger.info("offset_dir_path = {0}".format(offset_dir_path))
                    result = self.hdfs_manager.get_offset_data(offset_dir_path, client)
                    offset_data_latest = result[0]
                    self.logger.info("offset_data_latest = {0}".format(offset_data_latest))
                    offset_content_latest = result[1]
                except (ValueError, Exception) as ex:
                    self.logger.warning("Offset file corrupted for path {0}.".format(offset_dir_path))
                    # self.hdfs_manager.delete_offset_directory(offset_dir_path, client)
                    self.handle_failure(client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                                        latest_dir_local - 1)
                    latest_dir_local = latest_dir_local - 1
            # --------
            # offset_dir_path = current_checkpoint_directory_with_offset_local + str(latest_dir_local)
            # self.logger.info("offset_dir_path = {0}".format(offset_dir_path))
            # result = self.hdfs_manager.get_offset_data(offset_dir_path, client)
            # offset_data_latest = result[0]
            # self.logger.info("offset_data_latest = {0}".format(offset_data_latest))
            # offset_content_latest = result[1]
            # self.logger.info("offset_content_latest = {0}".format(offset_content_latest))
            if (not self.pipeline_offset_dict) or (not pipeline_name_local in self.pipeline_offset_dict):
                self.logger.info("Initialize the directory")
                if self.hdfs_manager.is_latest_file_format_valid(offset_content_latest):
                    offset_dict[latest_dir_local] = (offset_data_latest, offset_content_latest)
                    self.pipeline_offset_dict[pipeline_name_local] = offset_dict
                    self.logger.debug("pipeline_offset_dict = {0}".format(self.pipeline_offset_dict))
                else:
                    raise ValueError("Offset file corrupted.")
            else:
                offset_dict_previous = self.pipeline_offset_dict[pipeline_name_local]
                previous_offset = max(offset_dict_previous.keys())
                if latest_dir_local <= previous_offset:
                    self.logger.info("Current and previous offset directories are same.")
                    return None
                self.logger.info("Previous max offset file name = {0}".format(previous_offset))
                offset_data_previous = offset_dict_previous[previous_offset][0]
                # offset_content_previous = offset_dict_previous[previous_offset][1]
                self.logger.debug("offset_data_previous = {0}".format(offset_data_previous))

                if not self.hdfs_manager.is_latest_file_format_valid(offset_content_latest):
                    self.handle_failure(client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                                        previous_offset)
                elif self.hdfs_manager.is_latest_offset_valid(offset_data_latest, offset_data_previous):
                    offset_dict[latest_dir_local] = (offset_data_latest, offset_content_latest)
                    self.pipeline_offset_dict[pipeline_name_local] = offset_dict
                    self.logger.info("Latest offset is valid - {0}".format(latest_dir_local))
                else:
                    self.handle_failure(client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                                        previous_offset)

        except (ValueError, Exception) as ex:
            self.logger.error("Monitoring process has got error {0}".format(str(ex)))

    def handle_failure(self, client, current_checkpoint_directory_with_offset_local, pipeline_name_local,
                       previous_offset):
        # kill the pipeline
        is_killed = self.state_manager.kill_pipeline(pipeline_name_local)
        self.logger.info("Pipeline has been stopped.")
        if is_killed:
            # get latest directory again
            self.hdfs_manager.delete_invalid_directories(current_checkpoint_directory_with_offset_local, previous_offset,
                                            client)
            self.state_manager.start_pipeline(pipeline_name_local)
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
            configured_pipelines = self.config_manager.get_config_value(Constants.CONFIG_SAX_MONITORING_PIPELINES)
            client = self.hdfs_manager.get_connection(self.hdfs_url, self.hdfs_url_standby, self.user)
            while True:
                if not self.hdfs_manager.is_path_exists(client, '/'):
                    client = self.hdfs_manager.get_connection(self.hdfs_url, self.hdfs_url_standby, self.user)

                for pipeline in configured_pipelines:
                    try:
                        pipeline_name = pipeline['name']
                        pipeline_config_json = self.sax_api_manager.get_pipeline_config_json(pipeline_name)

                        current_checkpoint_directory = self.json_manager.get_current_checkpoint(pipeline_config_json)

                        current_checkpoint_directory_with_offset = current_checkpoint_directory + "/offsets/"
                        self.logger.info(
                            "Current checkpoint directory with offset - {0}".format(
                                current_checkpoint_directory_with_offset))

                        is_offset_dir_exist = self.hdfs_manager.is_path_exists(client,
                                                                               current_checkpoint_directory_with_offset)
                        if not is_offset_dir_exist:
                            self.logger.error("Offset directory not exist within checkpoint directory")
                            raise ValueError("Offset directory not exist within checkpoint directory")
                        self.logger.info("Offset directory is exist within checkpoint directory")

                        latest_dir = self.hdfs_manager.get_latest_offset_dir(current_checkpoint_directory_with_offset,
                                                                             client)
                        self.process_data(latest_dir, current_checkpoint_directory_with_offset, pipeline_name, client)

                    except (ValueError, ConnectionError, Exception) as ex:
                        self.logger.error("Monitoring process has got error {0}".format(str(ex)))

                time.sleep(int(self.live_monitoring_delay_seconds))
        except (ValueError, ConnectionError, Exception) as ex:
            self.logger.error("Monitoring process has got error {0}".format(str(ex)))
            self.logger.info("------------------------Monitoring Ended---------------------------------")


def main():
    live_monitoring = HDFSFrameworkHelper()
    live_monitoring.process()
    sys.exit()


if __name__ == '__main__':
    main()

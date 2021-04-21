import logging
import sys
from logging import handlers
from ConfigModule import ConfigManager
from ConstantsModule import Constants
from FileModule import FileHelper
from KafkaModule import KafkaManager
from FileQueueModule import FileQueueManager


class DataLossPublisherManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager(self.logger, "./config.json")
        filename = self.config_manager.get_config_value(Constants.LOG_FILE_NAME)
        log_level = self.config_manager.get_config_value(Constants.LOG_LEVEL)
        log_formatter = self.config_manager.get_config_value(Constants.LOG_FORMATTER)
        log_max_size_mb = int(self.config_manager.get_config_value(Constants.LOG_MAX_SIZE_MB))
        log_file_backup_counts = int(self.config_manager.get_config_value(Constants.LOG_FILE_BACKUP_COUNTS))
        formatter = logging.Formatter(log_formatter)
        filename = "{0}-data_loss_publisher.log".format(filename.split('.')[0])
        logHandler = handlers.RotatingFileHandler(filename, mode='a', maxBytes=log_max_size_mb * 1024 * 1024,
                                                  backupCount=log_file_backup_counts, encoding='utf-8', delay=False)

        logHandler.setLevel(log_level.upper())
        logHandler.setFormatter(formatter)

        # Setting the threshold of logger to DEBUG
        self.logger.setLevel(log_level.upper())
        self.logger.addHandler(logHandler)
        self.file_manager = FileHelper(self.logger)
        self.queue_manager = FileQueueManager(self.logger)
        self.kafka_manager = KafkaManager(self.logger, self.config_manager)
        self.logger.info("-------------------------Data Loss Publisher Initialised---------------------------------")

    def process(self):
        queue_directory_name = self.config_manager.get_config_value(Constants.CONFIG_DATA_LOSS_QUEUE_DIRECTORY_NAME)
        queue_directory_path = "./{0}/".format(queue_directory_name)
        queue_path_list = self.file_manager.get_directories(queue_directory_path)
        self.logger.info("queue_name_list = {0}".format(queue_path_list))
        for queue_path in queue_path_list:
            # queue_path = "./{0}/{1}".format(queue_directory_name, queue_name)
            lost_data = self.queue_manager.get(queue_path)
            self.logger.info("queue name - [{0}] lost data value - [{0}]".format(lost_data))
            path_values = queue_path.split('/')
            topic_names = path_values[len(path_values)-1]
            key_values = topic_names.split('__')
            from_topic_name = key_values[0]
            partition_value = key_values[1]
            to_topic_name = key_values[2]





def main():
    kafka_manager = DataLossPublisherManager()
    kafka_manager.process()
    sys.exit()


if __name__ == '__main__':
    main()
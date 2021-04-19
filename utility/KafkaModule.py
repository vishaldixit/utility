import logging
import sys
from logging import handlers
from kafka import KafkaConsumer, TopicPartition
from ConstantsModule import Constants
from ConfigModule import ConfigManager


class KafkaManager:
    def __init__(self, logger, config_manager):
        self.logger = logger
        self.config_manager = config_manager
        self.bootstrap_servers = self.config_manager.get_config_value(Constants.CONFIG_KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)

    def get_partition_list(self, topic):
        partitions = [TopicPartition(topic, partition) for partition in self.consumer.partitions_for_topic(topic)]
        return partitions

    def get_beginning_offsets(self, topic):
        partition_details = self.consumer.beginning_offsets(self.get_partition_list(topic))
        self.logger.info("Kafka beginning offsets {0}".format(partition_details))
        return self.get_offset_dictionary(partition_details)

    def get_end_offsets(self, topic):
        partition_details = self.consumer.end_offsets(self.get_partition_list(topic))
        self.logger.info("Kafka end offsets {0}".format(partition_details))
        return self.get_offset_dictionary(partition_details)

    def get_offset_dictionary(self, partition_details):
        offset_dictionary = {}
        for partition in partition_details:
            offset_dictionary[str(partition[1])] = partition_details[partition]
        self.logger.info("Offset Dictionary - {0}".format(offset_dictionary))
        return offset_dictionary

def main():
    logger = logging.getLogger(__name__)
    config_manager = ConfigManager(logger, "./config.json")
    filename = config_manager.get_config_value(Constants.LOG_FILE_NAME)
    log_level = config_manager.get_config_value(Constants.LOG_LEVEL)
    log_formatter = config_manager.get_config_value(Constants.LOG_FORMATTER)
    log_max_size_mb = int(config_manager.get_config_value(Constants.LOG_MAX_SIZE_MB))
    log_file_backup_counts = int(config_manager.get_config_value(Constants.LOG_FILE_BACKUP_COUNTS))
    formatter = logging.Formatter(log_formatter)
    filename = "{0}-kafka-test.log".format(filename.split('.')[0])
    logHandler = handlers.RotatingFileHandler(filename, mode='a', maxBytes=log_max_size_mb * 1024 * 1024,
                                              backupCount=log_file_backup_counts, encoding='utf-8', delay=False)

    logHandler.setLevel(log_level.upper())
    logHandler.setFormatter(formatter)

    # Setting the threshold of logger to DEBUG
    logger.setLevel(log_level.upper())
    logger.addHandler(logHandler)
    kafka_manager = KafkaManager(logger, config_manager)
    beginning_offsets = kafka_manager.get_beginning_offsets("targetedwireviewtopiclongrunv1")
    print(beginning_offsets)
    end_offsets = kafka_manager.get_beginning_offsets("targetedwireviewtopiclongrunv1")
    print(end_offsets)
    sys.exit()


if __name__ == '__main__':
    main()


class Constants:
    ENDPOINT_SPARK_RUNNING_APPS = "/applications?status=running"
    # Append spark application id
    ENDPOINT_SPARK_APP_JOBS = "/applications/{0}/1/jobs"
    ENDPOINT_SPARK_APP_RUNNING_JOBS = "/applications/{0}/1/jobs?status=running"

    # Append pipeline name
    ENDPOINT_SAX_GET_PIPELINE_CONFIG_JSON = "/json/list/{0}"
    ENDPOINT_SAX_GET_PIPELINE_STATUS_JSON = "/pipeline/detail?pipelineName={0}"
    ENDPOINT_SAX_UPDATE_PIPELINE_JSON = "/subsystem/update/json/{0}"
    ENDPOINT_SAX_START_PIPELINE = "/subsystem/action?name={0}&engine=spark&action=start"
    ENDPOINT_SAX_KILL_PIPELINE = "/subsystem/action?name={0}&engine=spark&action=kill"

    CONFIG_SPARK_API_URL = "spark_api_url"
    CONFIG_SAX_API_URL = "sax_api_url"
    CONFIG_SAX_API_TOKEN = "sax_api_token"
    CONFIG_RESTART_TIME_SECONDS = "restart_time_seconds"
    CONFIG_OFFSET_SKIP_PER_PARTITION = "offset_skip_per_partition"
    CONFIG_SAX_MONITORING_PIPELINES = "sax_monitoring_pipelines"
    CONFIG_MAX_RESTART_COUNT_DIFFERENCE = "max_restart_count_difference"
    CONFIG_COMMAND_KAFKA_LATEST_OFFSET = "command_kafka_latest_offset"
    CONFIG_COMMAND_KAFKA_EARLIEST_OFFSET = "command_kafka_earliest_offset"
    CONFIG_HDFS_URL = "hdfs_url"
    CONFIG_HDFS_URL_STANDBY = "hdfs_url_standby"
    CONFIG_HDFS_USER = "hdfs_user"
    CONFIG_KAFKA_BOOTSTRAP_SERVERS = "kafka_bootstrap_servers"
    CONFIG_LIVE_MONITORING_DELAY_SECONDS = "live_monitoring_delay_seconds"

    #CONFIG_SPARK_CORRUPT_OFFSET_DIFFERENCE = "spark_corrupt_offset_difference"

    HEADER_SAX_TOKEN = "token"

    # add directory here
    COMMAND_HDFS_DIRECTORY_LS = "hadoop fs -ls {0}"
    COMMAND_HDFS_DIRECTORY_CHECK = "hadoop fs -test -e {0}"
    COMMAND_HDFS_SHELL_STATUS = "echo $?"
    # add directory or file
    COMMAND_HDFS_GET_FILE = "hadoop fs -get -f {0}"
    # add local file and hdfs directory
    COMMAND_HDFS_PUT_FILE = "sudo -u livy hadoop fs -put -f {0} {1}"
    COMMAND_YARN_APPLICATION_KILL = "yarn application --kill {0}"
    #COMMAND_KAFKA_LATEST_OFFSET = "sh /usr/hdp/3.1.4.0-315/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:6667 --topic {0} --time -1"
    #COMMAND_KAFKA_EARLIEST_OFFSET = "sh /usr/hdp/3.1.4.0-315/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:6667 --topic {0} --time -2"

    SKIP_OFFSET_FILE_LINES = 2

    LOG_FILE_NAME = "log_file_name"
    LOG_FORMATTER = "log_formatter"
    LOG_LEVEL = "log_level"
    LOG_MAX_SIZE_MB = "log_max_size_mb"
    LOG_FILE_BACKUP_COUNTS = "log_file_backup_counts"

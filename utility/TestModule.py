import logging
from ConstantsModule import Constants
from PipelineMonitor import PipelineManager
from TimeModule import TimeHelper
from HdfsFramework import HDFSFrameworkHelper


class TestHelper:
    def __init__(self):
        # Create and configure logger
        logging.basicConfig(
            filename="../utility/pipeline-monitoring-test_{0}.log".format(TimeHelper.get_current_date_time_string()),
            format='%(asctime)s - %(levelname)s: %(message)s',
            filemode='w')

        # Creating an object
        self.logger = logging.getLogger()

        # Setting the threshold of logger to DEBUG
        self.logger.setLevel(logging.INFO)

        self.pipeline_manager = PipelineManager()

    def test_restart_count_test(self):
        pipeline_name = "Targeted_Transaction_V109_18_v2"
        old_pipeline_config_json = self.pipeline_manager.get_pipeline_config_json(pipeline_name)
        updated_pipeline_config_json = self.pipeline_manager.get_updated_pending_restart_count(old_pipeline_config_json)

        if updated_pipeline_config_json['config']['pendingRestartCount'] == updated_pipeline_config_json['config'][
            'maxRestartCount']:
            self.logger.info("Success- pendingRestartCount and maxRestartCount has matched.")
        else:
            self.logger.error("Failed- pendingRestartCount and maxRestartCount did not match.")

        self.pipeline_manager.send_update_json_request(updated_pipeline_config_json, pipeline_name)
        self.logger.info("Pipelines before json update pending restart count -{0}".format(
            updated_pipeline_config_json['config']['pendingRestartCount']))
        self.logger.info("Pipelines before json update max restart count -{0}".format(
            updated_pipeline_config_json['config']['maxRestartCount']))

        pipeline_config_json_new = self.pipeline_manager.get_pipeline_config_json(pipeline_name)
        self.logger.info("Pipelines after json update pending restart count -{0}".format(
            pipeline_config_json_new['config']['pendingRestartCount']))
        self.logger.info("Pipelines after json update max restart count -{0}".format(
            pipeline_config_json_new['config']['maxRestartCount']))

        is_pending_matched = (int(pipeline_config_json_new['config']['pendingRestartCount']) == int(updated_pipeline_config_json['config']['pendingRestartCount']))
        is_max_matched = (int(pipeline_config_json_new['config']['maxRestartCount']) == int(updated_pipeline_config_json['config']['maxRestartCount']))
        if is_pending_matched and is_max_matched:
            self.logger.info("Success- pendingRestartCount and maxRestartCount has matched after json update.")
        else:
            self.logger.error("Failed- pendingRestartCount and maxRestartCount did not match after json matched")


    def test_update_pending_count_zero(self):
        pipeline_name = "WireView_V109_44"
        old_pipeline_config_json = self.pipeline_manager.get_pipeline_config_json(pipeline_name)

        old_pipeline_config_json['config']['pendingRestartCount'] = 0
        self.pipeline_manager.send_update_json_request(old_pipeline_config_json, pipeline_name)



    def test_offset_file_update(self):
        offset_file_current_lines = self.pipeline_manager.file_manager.read_file_lines("651")
        self.logger.info("offset_file_current_lines = {0}".format(offset_file_current_lines))
        self.logger.setLevel(logging.DEBUG)
        offset_file_updated_lines = self.pipeline_manager.get_updated_offset_lines(offset_file_current_lines,
                                                                                   Constants.SKIP_OFFSET_FILE_LINES,
                                                                                   5)
        self.logger.info(offset_file_updated_lines)


def main():
    hdfs_framework = HDFSFrameworkHelper()
    hdfs_framework.process()
    #test_manager = TestHelper()
    #test_manager.test_restart_count_test()
    #test_manager.test_offset_file_update()
    #test_manager.test_update_pending_count_zero()


if __name__ == '__main__':
    main()

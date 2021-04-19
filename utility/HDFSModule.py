from LinuxModule import LinuxHelper
from StringModule import StringHelper
from ConstantsModule import Constants
from hdfs import InsecureClient
import json


class HDFSManager:
    def __init__(self, logger):
        self.logger = logger
        self.string_manager = StringHelper(self.logger)
        self.linux_manager = LinuxHelper(self.logger)

    def is_hdfs_file_exist(self, file_path):
        is_exist = False
        command_list = self.string_manager.get_command_as_list(Constants.COMMAND_HDFS_DIRECTORY_CHECK.format(file_path))
        self.linux_manager.process_command(command_list)
        command_return = int(self.linux_manager.get_command_status(Constants.COMMAND_HDFS_SHELL_STATUS))
        if command_return == 0:
            is_exist = True
            self.logger.info("Hdfs file exist - {0}".format(str(is_exist)))

        return is_exist

    def get_connection(self, hdfs_url, hdfs_url_standby, hdfs_user):
        client = InsecureClient(hdfs_url, user=hdfs_user)
        if not self.is_path_exists(client, '/'):
            client = InsecureClient(hdfs_url_standby, user=hdfs_user)
        self.logger.info("Hdfs connection created successfully.")
        return client

    def get_offset_data(self, offset_dir_path, client):
        data = None
        content = ""
        with client.read(offset_dir_path, encoding='utf-8', length=1048576) as reader:
            content = reader.read()
            values = content.split('\n')
            self.logger.info("offset content ={0}".format(str(content)))
            if len(values) >= 3:
                data = json.loads(values[2])
                self.logger.info("offset data dict ={0}".format(data))
            else:
                raise ValueError("Offset file corrupted.")
        return data, content

    def is_latest_offset_valid(self, offset_data_latest, offset_data_previous):
        is_valid = True
        for topic in offset_data_latest.keys():
            previous_keys = offset_data_previous[topic].keys()
            self.logger.info("previous offsets ={0}".format(previous_keys))
            for partition in offset_data_latest[topic].keys():
                if partition in previous_keys:
                    if int(offset_data_latest[topic][partition]) < int(offset_data_previous[topic][partition]):
                        is_valid = False
                        self.logger.warning("latest offset has has issue.")
                        self.logger.info("offset_data_latest[{0}][{1}] = {2}".format(topic, partition,
                                                                                     offset_data_latest[topic][
                                                                                         partition]))
                        self.logger.info("offset_data_previous[{0}][{1}] = {2}".format(topic, partition,
                                                                                       offset_data_previous[topic][
                                                                                           partition]))
                        break
        return is_valid

    def is_valid_dir(self, dir_name):
        is_valid = True
        try:
            value = int(dir_name)
        except:
            self.logger.warning("Not a valid directory = {0}  Discarding this directory".format(dir_name))
            is_valid = False
        return is_valid

    def get_latest_offset_dir(self, current_checkpoint_directory_with_offset, client):
        dirs = client.list(current_checkpoint_directory_with_offset)
        test_list = []
        for offset_file_name in dirs:
            if self.is_valid_dir(offset_file_name):
                test_list.append(int(offset_file_name))
        # test_list = [int(i) for i in dirs]
        test_list.sort(reverse=True)
        latest_dir = int(test_list[0])
        self.logger.info("Latest dir name - {0}".format(latest_dir))
        return latest_dir

    def delete_offset_directory(self, offset_path, connection):
        connection.delete(offset_path, recursive=True)
        self.logger.info("Directory deleted - {0}".format(offset_path))
        has_deleted = True
        return has_deleted

    def delete_invalid_directories(self, checkpoint_directory_with_offset, previous_correct_file_number, connection):
        current_offset_dirs = connection.list(checkpoint_directory_with_offset)
        current_offset_files = [int(i) for i in current_offset_dirs]
        current_offset_files.sort(reverse=True)

        offsets_delete_list = [offset_file_number for offset_file_number in current_offset_files if
                               offset_file_number > previous_correct_file_number]
        self.logger.info("offsets_delete_list - {0}".format(offsets_delete_list))

        for offset_file in offsets_delete_list:
            offset_path = checkpoint_directory_with_offset + str(offset_file)
            connection.delete(offset_path, recursive=True)
            self.logger.info("Directory deleted - {0}".format(offset_path))

    def is_latest_file_format_valid(self, offset_content_latest):
        is_valid = True
        try:
            offset_file_current_lines = offset_content_latest.split('\n')
            if len(offset_file_current_lines) < 3:
                self.logger.error("Offset file is not in right format. Offset file corrupted.")
                is_valid = False
        except (ValueError, Exception) as ex:
            self.logger.error(
                "Offset file is not in right format. Offset file corrupted. File content as below\n {0}".format(
                    offset_content_latest))
            is_valid = False
        return is_valid


    def is_latest_file_format_valid_lines(self, offset_file_current_lines):
        is_valid = True
        try:
            if len(offset_file_current_lines) < 3:
                self.logger.error("Offset file is not in right format. Offset file corrupted.")
                is_valid = False
        except (ValueError, Exception) as ex:
            self.logger.error(
                "Offset file is not in right format. Offset file corrupted. File content as below\n {0}".format(
                    offset_file_current_lines))
            is_valid = False
        return is_valid
    def write_hdfs_wrapper(self, client, offset_dir_path, offset_content_lines):
        if len(offset_content_lines) < 3:
            self.logger.error("Offset file got corrupted while updating the offsets. Aborting pipeline progress.")
            raise ValueError("Offset file corrupted.")

        append_line = "\n"
        offset_content = append_line.join(offset_content_lines)
        return self.write_hdfs_offset(client, offset_dir_path, offset_content)

    def write_hdfs_offset(self, client, offset_dir_path, offset_content):
        is_write_success = False
        client.write(offset_dir_path, encoding='utf-8', overwrite=True, data=offset_content)
        self.logger.info("Offset content has been updated into offset dir {0}.".format(offset_dir_path))
        self.logger.info("Updated content is as below \n{0}.".format(offset_content))
        is_write_success = True
        return is_write_success

    # not in use
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

    def is_path_exists(self, connection, path):
        is_offset_dir_exist = True
        try:
            file_status = connection.status(path)
            self.logger.info("{0} is exist and status is {1}".format(path, file_status))
        except (ValueError, Exception) as ex:
            is_offset_dir_exist = False
            self.logger.info("{0} does not exist.".format(path))
        return is_offset_dir_exist

    # def remove_previous_keys(self, pipeline_name):
    #     current_offset_dirs = self.pipeline_offset_dict[pipeline_name].keys()
    #     current_offset_files = [int(i) for i in current_offset_dirs]
    #     current_offset_files.sort(reverse=True)
    #
    #     if len(current_offset_files) > 10:
    #         for index in range(9, len(current_offset_files) - 1):
    #             self.pipeline_offset_dict[pipeline_name].pop(current_offset_files[index])

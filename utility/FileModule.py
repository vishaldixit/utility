import os


class FileHelper:
    def __init__(self, logger):
        self.logger = logger

    def read_file_lines(self, local_file_path):
        file_obj = open(local_file_path, 'r')
        lines = file_obj.readlines()
        file_obj.close()
        if len(lines) == 0:
            self.logger.error("Local file is empty.")
            raise Exception("Empty File")
        self.logger.info("Local file read successfully.")
        return lines

    def write_file_lines(self, local_file_path, lines):
        if len(lines) == 0:
            self.logger.error("Local file is empty.")
            raise Exception("Empty File")
        file_obj = open(local_file_path, 'w')
        file_obj.writelines(lines)
        file_obj.close()
        is_success = True
        self.logger.info("Local file write successfully.")
        return is_success

    def get_directories(self, directory_path):
        directories = [directory_path + name for name in os.listdir(directory_path) if os.path.isdir(directory_path + name)]
        self.logger.info("Directories list - \n {0}".format(directories))
        return directories

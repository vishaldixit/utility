
class StringHelper:
    def __init__(self, logger):
        self.logger = logger

    def get_latest_checkpoint_file(self, file_records):
        latest_path = ""
        self.logger.info("Current file records {0}".format(file_records))
        if not file_records:
            self.logger.error("Empty file records {0}".format(file_records))
            return latest_path

        path_dict = {}
        for line in file_records.splitlines():
            if '/' not in line:
                self.logger.info("Skipping result - {0}".format(line))
                continue
            clean_space = line.split(' ')
            offset_full_path = clean_space[len(clean_space) - 1]
            self.logger.info("offset_full_path-{0}".format(offset_full_path))
            path_values = offset_full_path.split('/')
            key = path_values[len(path_values) - 1]
            path_dict[key] = offset_full_path

        max_key = max(path_dict, key=int)
        self.logger.info("Latest file {0} and latest path {1}".format(max_key, path_dict[max_key]))
        latest_path = path_dict[max_key]
        return latest_path

    def get_file_name_from_path(self, file_path):
        path_values = file_path.split('/')
        file_name = path_values[len(path_values) - 1]
        self.logger.debug("file_name_from_path {0}".format(file_name))
        return file_name

    def get_command_as_list(self, command):
        self.logger.debug("Command list - {0}".format(command.split(' ')))
        return command.split(' ')

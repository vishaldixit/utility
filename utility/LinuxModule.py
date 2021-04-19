from subprocess import Popen, PIPE
import subprocess


class LinuxHelper:

    def __init__(self, logger):
        self.logger = logger

    def process_command(self, command_list):
        self.logger.info("Starting the processing the command -> {0}".format(str(command_list)))
        proc = Popen(command_list, stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if out:
            self.logger.info('stdout:\n' + str(out))

        if proc.returncode != 0:
            errmsg = 'Failed to execute command "' + str(command_list) + '", return code ' + str(proc.returncode)
            self.logger.error(errmsg)
            raise ValueError("Failed to execute command")
        elif err:
            self.logger.error('stderr:\n' + str(err))
            raise ValueError("Failed to execute command")
        return out

    def get_command_status(self, command):
        result = subprocess.check_output(command, shell=True)
        self.logger.info("Shell command status {0}".format(result))
        return result

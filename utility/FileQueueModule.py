from persistqueue import Queue


class FileQueueManager:
    def __init__(self, logger):
        self.logger = logger

    def put(self, key, queue_directory_name, start_value, end_value=None):
        is_completed = False
        queue_path = "./{0}/{1}".format(queue_directory_name, key)
        file_queue = Queue(queue_path)
        self.logger.info("Persisted file queue has been created with file_queue_key = {0}".format(queue_path))
        data_value = None
        if end_value:
            data_value = "{0}-{1}".format(start_value, end_value)
        data_value = str(start_value)
        file_queue.put(data_value)
        self.logger.info("Persisted file queue has saved value as {0}".format(data_value))
        is_completed = True
        return is_completed, file_queue

    def task_done(self, file_queue: Queue):
        file_queue.task_done()

    def get(self, queue_path):
        file_queue = Queue(queue_path)
        return file_queue.get(timeout=30)


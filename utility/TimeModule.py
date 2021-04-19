from datetime import datetime
import time


class TimeHelper:
    @staticmethod
    def get_json_value_as_datetime(json_object, json_key, index=None):
        try:
            if index is not None:
                output_value = json_object[index][json_key]
            else:
                output_value = json_object[json_key]
            return datetime.strptime(output_value, '%Y-%m-%dT%H:%M:%S.%fGMT')
        except (ValueError, Exception):
            output_value = None
            return output_value

    @staticmethod
    def get_time_difference_seconds(input_datetime):
        if input_datetime is not None:
            return (datetime.utcnow() - input_datetime).total_seconds()
        else:
            return None

    @staticmethod
    def get_current_epoch_time_seconds():
        return int(time.time())

    @staticmethod
    def get_current_date_time_string():
        now = datetime.now()
        date_string = "{0}_{1}_{2}_{3}_{4}".format(now.day, now.month, now.year, now.hour, now.minute)
        return  date_string


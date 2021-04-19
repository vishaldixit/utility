from numpy import percentile


class OutlierHelper:
    def __init__(self, logger):
        self.logger = logger

    def get_outlier(self, data_list):
        q25, q75 = percentile(data_list, 25), percentile(data_list, 75)
        iqr = q75 - q25
        self.logger.info('Percentiles: 25th=%.3f, 75th=%.3f, IQR=%.3f' % (q25, q75, iqr))
        # calculate the outlier cutoff
        cut_off = iqr * 1.5
        self.logger.info("cut off - {0}".format(cut_off))
        lower, upper = q25 - cut_off, q75 + cut_off
        self.logger.info("lower - {0} upper {1}".format(lower, upper))
        # identify outliers
        outliers = [x for x in data_list if x < lower or x > upper]
        self.logger.info("outliers  - {0}".format(str(outliers)))
        self.logger.info('Identified outliers: %d' % len(outliers))
        return outliers

    def treat_outlier(self, data_list, outlier_list):
        data_list_without_outliers = [ele for ele in data_list if ele not in outlier_list]
        min_value = min(data_list_without_outliers)
        max_value = max(data_list_without_outliers)
        treated_outlier_dict = {}
        for outlier in outlier_list:
            if outlier <= min_value:
                treated_outlier_dict[outlier] = min_value
            elif outlier >= max_value:
                treated_outlier_dict[outlier] = max_value
        self.logger.info("Treated outliers - {0}".format(treated_outlier_dict))
        return treated_outlier_dict

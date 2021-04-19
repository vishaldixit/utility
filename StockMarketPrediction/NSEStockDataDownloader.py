from selenium import webdriver
import time
import random
from StockMarketPrediction.ConfigModule import ConfigManager
from datetime import datetime
import logging
from selenium.webdriver.common.keys import Keys


class StockDownloader:
    def __init__(self, logger, config_manager: ConfigManager):
        self.logger = logger
        self.config_manager = config_manager
        self.driver = webdriver.Chrome(self.config_manager.config_data['selenium']['chrome_driver_path'])

    def getRandomTime(self):
        sleep_time = round(random.uniform(2, 7), 2)
        self.logger.info(sleep_time)
        return sleep_time

    def send_key_by_element(self, element_name, keys_data, random_wait=False):
        element = self.driver.find_element_by_name(element_name)
        element.send_keys(keys_data)
        if random_wait:
            time.sleep(self.getRandomTime())

    def click_key_by_element_id(self, element_id, random_wait=False):
        element = self.driver.find_element_by_id(element_id)
        element.click()
        if random_wait:
            time.sleep(self.getRandomTime())

    def send_key_by_element_id(self, element_id, keys_data, random_wait=False):
        element = self.driver.find_element_by_id(element_id)
        element.send_keys(keys_data)
        if random_wait:
            time.sleep(self.getRandomTime())

    def button_click_by_xpath(self, xpath, random_wait=False):
        element = self.driver.find_element_by_xpath(xpath)
        element.click()
        if random_wait:
            time.sleep(self.getRandomTime())

    def load_page_url(self, url, random_wait=False):
        self.driver.get(url)
        if random_wait:
            time.sleep(self.getRandomTime())

    def apply_scrolling(self, initial_height, increment_height, stopping_height, random_wait=False):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        print(last_height)
        hight_increment = initial_height
        while True:
            self.driver.execute_script("window.scrollTo(0, {0});".format(str(hight_increment)))
            print(hight_increment)
            hight_increment = hight_increment + increment_height
            if random_wait:
                time.sleep(self.getRandomTime())
            print(hight_increment)
            if hight_increment >= last_height - stopping_height:
                print("Scroll break")
                break

    def process(self):
        # create a new Chrome session

        self.driver.get(self.config_manager.config_data['nse_page']['url'])

        # Typed User Name & Password
        for stock in self.config_manager.config_data['stock']:
            self.send_key_by_element_id(self.config_manager.config_data['nse_page']['textbox_nse_page_symbol'],
                                        stock['stock_symbol'])
            self.click_key_by_element_id(self.config_manager.config_data['nse_page']['radio_time_period_id'])

            stock_start_date = datetime.strptime(stock['stock_first_trade_date'], '%d-%m-%Y').date()
            current_year = datetime.now().year
            stock_start_date_year = stock_start_date.year
            while stock_start_date_year <= current_year:
                from_date = "01-01-{0}".format(stock_start_date_year)
                to_date = "31-12-{0}".format(stock_start_date_year)
                self.send_key_by_element_id(self.config_manager.config_data['nse_page']['datebox_fromDate'], from_date)
                webdriver.ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                self.send_key_by_element_id(self.config_manager.config_data['nse_page']['datebox_toDate'], to_date)
                webdriver.ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                #self.driver.find_element_by_xpath("//img[@class='getdata-button']").click()
                self.driver.find_element_by_class_name("getdata-button").click()
                #self.click_key_by_element_id(self.config_manager.config_data['nse_page']['submitMe'])
                stock_start_date_year = stock_start_date_year + 1


def main():
    logger = logging.getLogger(__name__)
    config_manager = ConfigManager(logger, "config.json")
    downloader = StockDownloader(logger, config_manager)
    downloader.process()


if __name__ == '__main__':
    main()

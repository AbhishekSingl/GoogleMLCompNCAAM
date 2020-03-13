"""
author: Vineeth Suhas Challagali

Scraping the ranks of various basketball teams in NCAA tournament for Kaggle competetion.
"""

import re
import time
import datetime
import argparse
from contextlib import contextmanager

from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


OUTPUT_PATH = "/Users/vineethsuhas/vineeth/handsOn/hackathons/kaggle/GoogleMLCompNCAAM/data/raw/scraped_ranks/"
BASE_URL = "https://www.masseyratings.com/ranks?s=cb&dt={0}"
HEADER_HTML_CLASS = "headrow headrepeat"
BODY_HTML_CLASS = re.compile(r'bodyrow tr[0-9]')


def get_url(dt):
    return BASE_URL.format(dt.strftime('%Y%m%d'))


def get_dates(sdt, edt):
    dates = [sdt]
    while sdt < edt:
        sdt = sdt + datetime.timedelta(days=7)
        dates.append(sdt)
    return dates


@contextmanager
def scraping_driver(headless=True, timeout=60):
    print("Initializing Driver")
    opts = Options()
    opts.headless = headless
    driver = webdriver.Firefox(options=opts)
    driver.set_page_load_timeout(timeout)
    yield driver
    print("Quitting Driver")
    driver.quit()


def get_header(soup):
    header = []
    for each in soup.find('tr', class_=HEADER_HTML_CLASS):
        header.append('{0}'.format(each.text))
    return header


def get_body(soup):
    rows = []
    tb_rows = soup.find_all("tr", class_=BODY_HTML_CLASS)
    for tb_row in tb_rows:
        row = []
        for val in tb_row.find_all('td'):
            row.append(val.text)
        rows.append(row)
    return np.array(rows)


# def scrape_website(url):
#     with scraping_driver() as sd:
#         sd.get(url)
#         page_data = sd.page_source
#         souped_data = BeautifulSoup(page_data, 'lxml')
#
#     header = get_header(souped_data)
#     body = get_body(souped_data)
#     result = pd.DataFrame(body, columns=header)
#     return result


def scrape_website(sd, url):
    sd.get(url)
    page_data = sd.page_source
    souped_data = BeautifulSoup(page_data, 'lxml')
    
    if not souped_data.find('tr', class_=HEADER_HTML_CLASS):
        import ipdb; ipdb.set_trace()
    header = get_header(souped_data)
    body = get_body(souped_data)
    result = pd.DataFrame(body, columns=header)
    return result


if __name__ == '__main__':
    import ipdb; ipdb.set_trace()
    parser = argparse.ArgumentParser()
    parser.add_argument('-s',
                        '--start-date',
                        type=str,
                        help='Start Date For Scraping',
                        required=True)
    parser.add_argument('-e',
                        '--end-date',
                        type=str,
                        help='End Date For Scraping',
                        required=True)

    args = parser.parse_args()

    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    dates = get_dates(start_date, end_date)

    with scraping_driver() as sd:

        for i, date in enumerate(dates):
            url = get_url(date)

            print("Started Scraping the Website {0}".format(url))
            result_df = scrape_website(sd, url)
            print("Done scraping the Website {0}".format(url))

            file_name = "ranks_" + str(date).replace('-', '_') + '.csv'
            file_path = OUTPUT_PATH + file_name

            print("Started writing the scraped data to {0}".format(file_name))
            result_df.to_csv(file_path)
            print("Done writing the scraped data to {0}".format(file_name))
            
            #if (i+1) % 5 == 0:
            #    print("Waiting for sometime.....")
            #    time.sleep(30)


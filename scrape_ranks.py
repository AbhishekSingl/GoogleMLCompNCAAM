"""
author: Vineeth Suhas Challagali

Scraping the ranks of various basketball teams in NCAA tournament for Kaggle competetion.

RUN: python scrape_ranks.py -s <yyyy-mm-dd> -e <yyyy-mm-dd>
"""
import os
import re
import time
import datetime
import argparse
from contextlib import contextmanager

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


DATA_DIR = '/Users/vineethsuhas/vineeth/handsOn/hackathons/kaggle/GoogleMLCompNCAAM/data/raw/'
OUTPUT_FOLDER = DATA_DIR + "scraped_ranks/"
if not os.path.exists(OUTPUT_FOLDER):
    os.mkdir(OUTPUT_FOLDER)

BASE_URL = "https://www.masseyratings.com/ranks?s=cb&dt={0}"
HEADER_HTML_CLASS = "headrow headrepeat"
BODY_HTML_CLASS = re.compile(r'bodyrow tr[0-9]')


def get_url(dt):
    return BASE_URL.format(dt.strftime('%Y%m%d'))


def get_dates(sdt, edt):
    dates = [datetime.datetime.date(sdt)]
    while sdt < edt:
        sdt = sdt + datetime.timedelta(days=7)
        dates.append(datetime.datetime.date(sdt))
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


def scrape_website(sd, url):
    print("Scraping the Website for URL .... {0}".format(url))

    tries = 0
    retry = 3
    sd.get(url)
    page_data = sd.page_source
    souped_data = BeautifulSoup(page_data, 'lxml')

    while not souped_data.find('tr', class_=HEADER_HTML_CLASS) and tries < retry:
        print("Failed!! sleeping and Retry attempt {0}".format(tries))
        time.sleep(20)
        tries += 1

        sd.get(url)
        page_data = sd.page_source
        souped_data = BeautifulSoup(page_data, 'lxml')

    header = get_header(souped_data)
    body = get_body(souped_data)
    result = pd.DataFrame(body, columns=header)
    return result


def write_to_file(df):
    file_name = "ranks_" + str(date).replace('-', '_') + '.csv'
    file_path = OUTPUT_FOLDER + file_name

    print("Writing DataFrame to file ..... {0}".format(file_name))
    df.to_csv(file_path)


if __name__ == '__main__':
    # Argument Parsing for start and end dates
    parser = argparse.ArgumentParser()
    parser.add_argument('-s',
                        '--start-date',
                        type=str,
                        help='Start Date For Scraping: yyyy-mm-dd',
                        required=True)
    parser.add_argument('-e',
                        '--end-date',
                        type=str,
                        help='End Date For Scraping: yyyy-mm-dd',
                        required=True)

    # Get the arguments
    args = parser.parse_args()
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    # Get all the dates for which the ranks has to be fetched
    dates = get_dates(start_date, end_date)

    # For each date, fetch the ranks and write to a file.
    with scraping_driver() as sd:
        for date in dates:
            url = get_url(date)
            result_df = scrape_website(sd, url)
            write_to_file(result_df)


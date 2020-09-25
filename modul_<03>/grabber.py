import os
import csv
import requests
import json

import numpy as np

from bs4 import BeautifulSoup
from multiprocessing import Pool
from pathlib import Path
from datetime import datetime

GRABBER_ROOT = './module_03/data'
CORES_NUMBER = 20
TRIP_ADVISOR_URL_TEMPLATE = 'https://www.tripadvisor.com{}'


def parse_ratings_and_reviews(node, result):
    rating_block = node.find('div').findAll('div', recursive=False)
    if len(rating_block) < 3:
        return result
    rating_block = rating_block[2].findAll('div', recursive=False)
    if len(rating_block) < 2:
        return

    ratings = rating_block[1].findAll('div')
    for rating in ratings:
        spans = rating.findAll('span', recursive=False)
        title = spans[1].text.lower()
        value = spans[2].find('span').attrs['class'][1].split('_')[1]
        result[title] = int(value)


def parse_location_and_contact(node):
    location_block = node.find('div').find('div')
    location_block = location_block.findAll('div', recursive=False)[1]
    distance_el = location_block.find('b')
    if distance_el is None:
        return np.NaN
    return float(distance_el.text.split()[0])


def parse_details_block(node, result):
    if node is None:
        return

    result['is_verified'] = 1 if node.find(
        'span', {'class': 'ui_icon verified-checkmark'}) is not None else 0
    result['has_phone_number'] = 1 if node.find(
        'a', string='+ Add phone number') is None else 0
    result['has_hours'] = 1 if node.find(
        'a', string='+ Add hours') is None else 0
    result['has_website'] = 1 if node.find(
        'a', string='+ Add website') is None else 0
    result['has_menu'] = 1 if node.find('a', string='Menu') is not None else 0


def collect_page_data(html, result):
    soup = BeautifulSoup(html, features="lxml")
    overview_tabs = soup.find('div', {'data-tab': 'TABS_OVERVIEW'})
    if overview_tabs is None:
        return

    overview_columns = overview_tabs.findAll('div', {'class': 'ui_column'})
    parse_ratings_and_reviews(overview_columns[0], result)
    parse_details_block(overview_columns[1], result)

    result['distance'] = parse_location_and_contact(overview_columns[2])
    result['has_tcAward'] = 1 if soup.find(
        'img', {'class': 'tcAward'}) is not None else 0


def grab_pages(records):
    for record in records:
        ta_url = TRIP_ADVISOR_URL_TEMPLATE.format(record['ta_url'])
        print(ta_url)
        r = requests.get(ta_url, stream=True)
        collect_page_data(r.text, record)
    return records


def parallelize_processing(records):
    pool = Pool(CORES_NUMBER)
    splitted_recs = np.array_split(records, CORES_NUMBER)
    grabbed_data = pool.map(grab_pages, splitted_recs)
    pool.close()
    pool.join()
    return np.concatenate(grabbed_data)


def read_records(filrname):
    file_path = '{}/{}/{}'.format(GRABBER_ROOT, 'urls', filename)
    records = list()
    with open(file_path) as csvfile:
        filereader = csv.reader(csvfile)
        for row in filereader:
            row_obj = {}
            row_obj['id'] = row[0]
            row_obj['ta_id'] = row[1]
            row_obj['ta_url'] = row[2]
            records.append(row_obj)
    return records


def process_file(filename):
    if not filename.endswith('.csv'):
        return

    print(filename)

    data_file_name = '{}_data.json'.format(
        filename.split('/')[-1].split('.')[0])
    data_file_path = '{}/{}'.format(GRABBER_ROOT, data_file_name)

    records = read_records(filename)
    records_data = parallelize_processing(records)

    with open(data_file_path, "w") as write_file:
        json.dump(records_data.tolist(), write_file)


for dirname, _, filenames in os.walk('{}/{}'.format(GRABBER_ROOT, 'urls')):
    for filename in filenames:
        process_file(os.path.join(dirname, filename))

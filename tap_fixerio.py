#!/usr/bin/env python3

import json
import sys
import argparse
import time
import requests
import singer
import backoff

from datetime import date, datetime, timedelta

base_url = 'https://api.fixer.io/'

logger = singer.get_logger()
session = requests.Session()

DATE_FORMAT='%Y-%m-%d'

def parse_response(r):
    flattened = r['rates']
    flattened[r['base']] = 1.0
    flattened['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], DATE_FORMAT))
    return flattened

schema = {'type': 'object',
          'properties':
          {'date': {'type': 'string',
                    'format': 'date-time'}},
          'additionalProperties': True}

def giveup(error):
    logger.error(error.response.text)
    response = error.response
    return not (response.status_code == 429 or
                response.status_code >= 500)

@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException),
                      jitter=backoff.random_jitter,
                      max_tries=5,
                      giveup=giveup,
                      interval=30)
def request(url, params):
    response = requests.get(url=url, params=params)
    response.raise_for_status()
    return response
    
def do_sync(base, start_date):
    logger.info('Replicating exchange rate data from fixer.io starting from {}'.format(start_date))
    singer.write_schema('exchange_rate', schema, 'date')

    state = {'start_date': start_date}
    next_date = start_date
    
    try:
        while True:
            response = request(base_url + next_date, {'base': base})
            payload = response.json()

            if datetime.strptime(next_date, DATE_FORMAT) > datetime.utcnow():
                break
            else:
                singer.write_records('exchange_rate', [parse_response(payload)])
                state = {'start_date': next_date}
                next_date = (datetime.strptime(next_date, DATE_FORMAT) + timedelta(days=1)).strftime(DATE_FORMAT)

    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        singer.write_state(state)
        sys.exit(-1)

    singer.write_state(state)
    logger.info('Tap exiting normally')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=False)
    parser.add_argument(
        '-s', '--state', help='State file', required=False)

    args = parser.parse_args()

    if args.config:
        with open(args.config) as file:
            config = json.load(file)
    else:
        config = {}

    if args.state:
        with open(args.state) as file:
            state = json.load(file)
    else:
        state = {}

    start_date = state.get('start_date',
                           config.get('start_date', datetime.utcnow().strftime(DATE_FORMAT)))

    do_sync(config.get('base', 'USD'), start_date)


if __name__ == '__main__':
    main()

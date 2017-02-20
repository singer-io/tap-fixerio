#!/usr/bin/env python3

import json
import sys
import argparse
import time

import requests
import singer

base_url = 'https://api.fixer.io/'

logger = singer.get_logger()

def parse_response(r):
    flattened = r['rates']
    flattened[r['base']] = 1.0
    flattened['date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(r['date'], '%Y-%m-%d'))
    return flattened

schema = {'type': 'object',
          'properties':
          {'date': {'type': 'string',
                    'format': 'date-time'}},
          'additionalProperties': True}

def do_sync(args):

    if args.config:
        with open(args.config) as file:
            config = json.load(file)
    else:
        config = {}

    base = config.get('base', 'USD')
    logger.info('Replicating the latest exchange rate data from fixer.io')
    singer.write_schema('exchange_rate', schema, 'date')

    try:
        params = {'base': base}
        response = requests.get(url=base_url + '/latest', params=params)
        singer.write_records('exchange_rate', [parse_response(response.json())])
        logger.info('Tap exiting normally')
    except requests.exceptions.RequestException as e:
        logger.fatal('Error on ' + e.request.url +
                     '; received status ' + str(e.response.status_code) +
                     ': ' + e.response.text)
        sys.exit(-1)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=False)
    parser.add_argument(
        '-s', '--state', help='State file', required=False)

    args = parser.parse_args()

    do_sync(args)


if __name__ == '__main__':
    main()

"""
    Extract and enrich quote events.

    Note: TASK comments along the code are intended to break the code in what would be roughly executed as individual
    Airflow tasks

    Example usage:
        python quotes_etl.py config.yaml
"""

import argparse
import json
import csv
from datetime import datetime
from os import path
from urllib.parse import urljoin
import logging
import pathlib
import zipfile
import asyncio
import requests

from utils.dr_utils import get_config_parameters, create_db, load_csv_to_db
from integrations.event_store import extract_events_from_stream

RESULTS_FOLDER = '../results/'

# IMPROVEMENTS
# Ideally the ingestion process should cope with new fields being added to the events schemas without required changes
# in the ingestion process. The solution of picking up a set of fields was intended to get the data quickly in
# without having to spend much time doing JSON un-nesting. If implemented in Snowflake, the file would be loaded
# the database as extracted and using the rich JSON manipulation functions available in the database engine
# the ETL would extract the required data into normalised tables.
#
# The process should also consider GDPR compliance as it is currently loading name, phone number and email that are
# considered PII. A possible solution would be to load all the data 'as is' in ingestion level, and then hash the
# PII fields before promoting it to database layers with wider access (usually the layer that the BI tool can
# access).

QUOTE_HEADERS= [
    'quoteId', 'timestamp', 'userId', 'paymentType', 'email', 'reference', 'accountReference', 'sanctionsSearchRecord',
    'source', 'has_duplicates', 'sanction_check_passed', 'safe_sic_code', 'can_access_portal', 'isFirstQuote',
    'contact_businessType', 'contact_businessName', 'contact_businessNumber', 'contact_sicCodes', 'contact_tradingName',
    'contact_turnover', 'contact_employeeCount', 'contact_ern', 'contact_ernExempt', 'contact_hasActiveInsurance',
    'contact_activeInsuranceRenewalDate', 'contact_effectiveDate', 'contact_isValidBusiness', 'contact_quoteId',
    'contact_premises', 'eventType', 'eventCreated'
]

PRODUCT_HEADERS = [
    'quote_id', 'id', 'key', 'name', 'total', 'ipt', 'agencyProduct_guid', 'agencyProduct_name'
]

log = logging.getLogger(name=__name__)
logging.basicConfig(
    filename='quotes_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger().addHandler(logging.StreamHandler())


def extract_quote_fields(event_dict):
    """
    Extract quote fields from event object

    :param event_dict: JSON object representing an event (dict)
    :return: CSV record (lst)
    """
    csv_record = []
    event_type = event_dict.get('streamMetadata').get('eventType')

    csv_record.append(event_dict.get('quoteId'))
    csv_record.append(event_dict.get('timestamp'))
    csv_record.append(event_dict.get('userId'))
    csv_record.append(event_dict.get('paymentType'))
    csv_record.append(event_dict.get('email'))
    csv_record.append(event_dict.get('reference'))
    csv_record.append(event_dict.get('accountReference'))
    csv_record.append(event_dict.get('sanctionsSearchRecord'))
    csv_record.append(event_dict.get('source'))
    csv_record.append(event_dict.get('has_duplicates'))
    csv_record.append(event_dict.get('sanction_check_passed'))
    csv_record.append(event_dict.get('safe_sic_code'))
    csv_record.append(event_dict.get('can_access_portal'))
    csv_record.append(event_dict.get('isFirstQuote'))
    if event_type == 'contact_updated':
        csv_record.append(event_dict.get('businessType'))
        csv_record.append(event_dict.get('businessName'))
        csv_record.append(event_dict.get('businessNumber'))
        csv_record.append(str(event_dict.get('sicCodes')))
        csv_record.append(event_dict.get('tradingName'))
        csv_record.append(event_dict.get('turnover'))
        csv_record.append(event_dict.get('employeeCount'))
        csv_record.append(event_dict.get('ern'))
        csv_record.append(event_dict.get('ernExempt'))
        csv_record.append(event_dict.get('hasActiveInsurance'))
        csv_record.append(event_dict.get('activeInsuranceRenewalDate'))
        csv_record.append(event_dict.get('effectiveDate'))
        csv_record.append(event_dict.get('isValidBusiness'))
        csv_record.append(event_dict.get('quoteId'))
        csv_record.append(event_dict.get('premises'))
    else:
        if event_dict.get('contact'):
            # IMPROVEMENTS
            # This exception is required because the second level get key will error if the first
            # level object do not exist. Look for a more elegant solution
            csv_record.append(event_dict.get('contact').get('businessType'))
            csv_record.append(event_dict.get('contact').get('businessName'))
            csv_record.append(event_dict.get('contact').get('businessNumber'))
            csv_record.append(str(event_dict.get('contact').get('sicCodes')))
            csv_record.append(event_dict.get('contact').get('tradingName'))
            csv_record.append(event_dict.get('contact').get('turnover'))
            csv_record.append(event_dict.get('contact').get('employeeCount'))
            csv_record.append(event_dict.get('contact').get('ern'))
            csv_record.append(event_dict.get('contact').get('ernExempt'))
            csv_record.append(event_dict.get('contact').get('hasActiveInsurance'))
            csv_record.append(event_dict.get('contact').get('activeInsuranceRenewalDate'))
            csv_record.append(event_dict.get('contact').get('effectiveDate'))
            csv_record.append(event_dict.get('contact').get('isValidBusiness'))
            csv_record.append(event_dict.get('contact').get('quoteId'))
            csv_record.append(event_dict.get('contact').get('premises'))
    csv_record.append(event_type)
    csv_record.append(event_dict.get('streamMetadata').get('eventCreated'))

    return csv_record


def extract_product_fields(quote_id, product_list):
    """
    Extract product fields from quote event object

    :param quote_id: id of the quote associated the products refer to (str)
    :param product_list: array of JSON object representing products (lst)
    :return:
    """
    csv_record_list = []
    for product in product_list:
        csv_record = [
            quote_id,
            product.get('id'),
            product.get('key'),
            product.get('name'),
            product.get('total'),
            product.get('ipt'),
            product.get('agencyProduct').get('guid'),
            product.get('agencyProduct').get('name')
        ]
        csv_record_list.append(csv_record)

    return csv_record_list


def main(config_file):
    log.info(f'Executing Quote Events extraction ETL')

    # TASK 1 - Setup temp folder for results
    execution_timestamp = datetime.strftime(datetime.now(), '%Y_%m_%d_%H_%M')
    temp_folder = path.join(RESULTS_FOLDER, 'quotes_' + execution_timestamp)
    log.info(f'Results will be saved in {temp_folder}')
    pathlib.Path(temp_folder).mkdir(parents=True, exist_ok=True)

    # TASK 2 - Extract events from Event Store
    events_file = path.join(temp_folder, 'quote_events.json')
    event_store_parameters = get_config_parameters(config_file, 'event_store')

    asyncio.run(
        extract_events_from_stream(
            connection_parameters=event_store_parameters,
            stream='quotes-data-engineering',
            output_file=events_file
        )
    )

    # TASK 3 - Processes events to extract relevant event types into quotes and quote_items files
    quotes_file = path.join(temp_folder, 'quotes.json')
    quote_items_file = path.join(temp_folder, 'quote_items.json')
    with open(events_file, 'r') as in_events, \
            open(quotes_file, 'w') as out_quotes, \
            open(quote_items_file, 'w') as out_items:

        in_data = in_events.readlines()
        quotes_writer = csv.writer(out_quotes, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        quote_items_writer = csv.writer(out_items, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        decode_errors = 0
        for line in in_data:

            try:
                line_json = json.loads(line)
            except json.decoder.JSONDecodeError:
                decode_errors += 1

            ignored_events = [
                'quote_sanctions_search_record_added',
                'statement_updated',
                'coupon_applied',
                'fee_applied'
            ]

            if line_json['streamMetadata']['eventType'] not in ignored_events:
                quote_values = extract_quote_fields(line_json)

                quotes_writer.writerow(quote_values)

                if line_json.get('products') and line_json.get('quoteId'):
                    products_values = extract_product_fields(
                        line_json.get('quoteId'),
                        line_json.get('products'),
                    )
                    quote_items_writer.writerows(products_values)

    log.info(f'File processed with {decode_errors} JSON decoding errors out of {len(in_data)} records')

    # TASK 4 - Creates SQLite database
    db_file = create_db(
        db_name='AnalyticsQuotes',
        output_folder=temp_folder,
    )

    # TASK 5 - Loads files into database
    load_csv_to_db(
        csv_file=quotes_file,
        header=QUOTE_HEADERS,
        db_file=db_file,
        table_name='quotes',
    )

    load_csv_to_db(
        csv_file=quote_items_file,
        header=PRODUCT_HEADERS,
        db_file=db_file,
        table_name='quote_items',
    )

    # TASK 6 - Downloads file from Companies House
    ch_parameters =  get_config_parameters(config_file, 'companies_house')
    file_name = 'BasicCompanyDataAsOneFile-' + datetime.strftime(datetime.today().replace(day=1), '%Y-%m-%d' + '.zip')
    ch_file = path.join(temp_folder, file_name)
    full_url = urljoin(ch_parameters['url'], file_name)
    response = requests.get(full_url)

    if response.status_code == 200:
        with open(ch_file, 'wb') as out_file:
            out_file.write(response.content)
        log.info(f'Companies House monthly file extracted from {full_url}')
    else:
        log.error(f'Companies House file is not available in {full_url}')

    if path.exists(ch_file):
        with zipfile.ZipFile(ch_file, 'r') as zip_file:
            zip_file.extractall(temp_folder)
        log.info(f'Companies House monthly file unzipped in folder {temp_folder}')

    # TASK 7 - Loads Companies House file into database
    unzipped_file = ch_file[:-4] + '.csv'
    if path.exists(unzipped_file):
        load_csv_to_db(
            csv_file=unzipped_file,
            header=None,
            db_file=db_file,
            table_name='company_data',
        )

    # TASK 8 - Enriches quotes with incorporation date -- NOT I\MPLEMENTED

    # IMPROVEMENTS
    # Data load in SQLite is very basic and would need more to ensure data is loading correctly (some columns looks
    # misaligned. Also the performance is so bad that a join was taking over 5 minutes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Executing quotes extraction ETL')
    parser.add_argument('config_file', type=str, help='YAML config file to be used in the report')

    args = parser.parse_args()
    main(**vars(args))

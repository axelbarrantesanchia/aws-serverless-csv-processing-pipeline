import csv
import json
from datetime import datetime
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")


def validate_csv(data):

    if len(data) < 2:
        logger.error("CSV file is empty or missing data rows")
        return True

    valid_product_lines = ['Object Storage', 'Firewall', 'Analytics', 'Networking', 'Compute']
    valid_currencies = ['USD', 'MXN', 'CAD']

    for row in csv.reader(data[1:], delimiter=','):
        if len(row) < 9:
            logger.error("Malformed CSV row")
            return True

        logger.info(row)
        date = row[6]
        product_line = row[4]
        currency = row[7]

        if product_line not in valid_product_lines:
            logger.error(f"Error in record {row[0]}: Unrecognized product line: {product_line}")
            return True

        if currency not in valid_currencies:
            logger.error(f"Error in record {row[0]}: Unrecognized currency: {currency}")
            return True

        try:
            datetime.strptime(date, '%Y-%m-%d')
        except:
            logger.error(f"Error in record {row[0]}: incorrect date format: {date}")
            return True
    return False

def lambda_handler(event, context):

    sqs_body = json.loads(event['Records'][0]['body'])

    message = json.loads(sqs_body['Message'])
    billing_bucket = message['bucket']
    csv_file = message['key']

    logger.info(f"Retrying {billing_bucket}/{csv_file}")
    obj = s3_client.get_object(Bucket=billing_bucket, Key=csv_file)
    data = obj['Body'].read().decode('utf-8').splitlines()
    processed_bucket = 'tech-billing-processed-data-axelbarrantes'
    error_bucket = 'tech-billing-error-data-axelbarrantes'

    error_found = validate_csv(data)

    copy_source = {
        'Bucket': billing_bucket,
        'Key': csv_file
    }

    if error_found:

        try:
            s3_client.copy_object(CopySource=copy_source, Bucket=error_bucket, Key=csv_file)
            logger.info(f"Moved erroneous file to: {error_bucket}")
            s3_client.delete_object(Bucket=billing_bucket, Key=csv_file)

        except Exception as e:
            logger.error("Error while moving file: " + str(e))
    else:
        try:
            s3_client.copy_object(CopySource=copy_source, Bucket=processed_bucket, Key=csv_file)
            logger.info(f"Moved file to: {processed_bucket}")
            s3_client.delete_object(Bucket=billing_bucket, Key=csv_file)

        except Exception as e:
            logger.error("Error while moving file: " + str(e))

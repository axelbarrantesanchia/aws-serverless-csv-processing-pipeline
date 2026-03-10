import csv
from datetime import datetime
import logging
import boto3
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")
sns = boto3.client("sns")

def simulate_failure(billing_bucket, csv_file):
    try:
        raise Exception("Simulated failure triggered")
    except Exception as e:
        sns_arn = "arn:aws:sns:us-east-1:869171546955:tech-billing-parser-alerts"

        payload = {
            "bucket": billing_bucket,
            "key": csv_file,
            "error": str(e)
        }

        sns.publish(
            TopicArn=sns_arn,
            Subject="Lambda failed to execute",
            Message=json.dumps(payload)
        )

        logger.info("SNS message successfully published")
        raise e


def validate_csv(data):
    valid_product_lines = ['Storage', 'Firewall', 'Analytics', 'Networking', 'Compute']
    valid_currencies = ['USD', 'MXN', 'CAD']

    for row in csv.reader(data[1:], delimiter=','):
        logger.info(row)
        date = row[6]
        product_line = row[4]
        currency = row[7]
        if len(row) < 9:
            logger.error("Malformed CSV row")
            return True
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
    billing_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    if "fail" in csv_file:
        simulate_failure(billing_bucket,csv_file)
    error_bucket = 'tech-billing-error-data-axelbarrantes'
    obj = s3_client.get_object(Bucket=billing_bucket, Key=csv_file)
    data = obj['Body'].read().decode('utf-8').splitlines()
    processed_bucket = 'tech-billing-processed-data-axelbarrantes'


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


#!/bin/bash

set -e

FUNCTION_NAME="tech-billing-csv-parser"
LAMBDA_DIR="../src/parser_lambda"
ZIP_NAME="parser_lambda.zip"

echo "Packaging Parser Lambda..."

cd $LAMBDA_DIR

zip -r $ZIP_NAME .

echo "Uploading to AWS Lambda..."

aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --zip-file fileb://$ZIP_NAME

echo "Deployment completed for $FUNCTION_NAME"

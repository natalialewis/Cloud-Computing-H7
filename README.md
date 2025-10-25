# Homework 6 - Build Instructions

This project uses a Python environment consumer to process widget requests from AWS S3 or DynamoDB. It creates the widget in either one, depending on what the user specified.

# Installations
1. Python
2. AWS credentials
3. AWS resources
    * A bucket to receive producer requests
    * A bucket to store created widgets
    * A DynamoDB table named widgets

# Instructions for testing
1. Run unit tests run `pytest` in the console

# Instructions for running
1. Run the consumer script for S3 or DynamoDB
    * S3 - ```python consumer.py --storage s3 --consume-bucket-name <your-request-bucket> --bucket-name <your-web-bucket>```
    * DynamoDB - ```python consumer.py --storage dynamodb --consume-bucket-name <your-request-bucket> --table-name widgets```
2. Send the requests from producer
    * ```java -jar producer.jar -rb <your-request-bucket> -mwr 10```
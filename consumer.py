import sys
import time
import click
from retriever import S3Retriever, SQSRetriever
from storage import S3Storage
from storage import DynamoDBStorage
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("consumer.log"),  # Logs to consumer.log
        logging.StreamHandler(sys.stdout)   # Logs to the console
    ]
)

# registers the function as a command line command
@click.command()
# lets the user choose where to store the widget (can be in Bucket 3 or the DynamoDB table)
@click.option("--storage", type=click.Choice(["dynamodb", "s3"]), help="Storage location for create widget requests")
# lets the user choose the SQS queue name
@click.option("--queue-name", type=str, help="SQS queue name for consuming widget requests")
# lets the user choose the bucket where the requests are consumed from
@click.option("--consume-bucket-name", type=str, help="S3 bucket name for consuming widget requests")
# lets the user choose which s3 bucket to use if they choose s3 as storage
@click.option("--bucket-name", type=str, help="S3 bucket name for storing widgets if using s3 storage")
# lets the user choose which DynamoDB table to use if they choose dynamodb as storage
@click.option("--table-name", type=str, help="DynamoDB table name for storing widgets")
def consume_requests(storage, queue_name, consume_bucket_name, bucket_name, table_name):
    # loop until the user presses Ctrl+C
    try:
        print("Starting consumer... Press Ctrl+C to stop.")

        request_retriever = None
        retriever_mode = None

        # devide which retriever to use based on user input
        if queue_name:
            # initialize the request retriever
            request_retriever = SQSRetriever(queue_name)
            retriever_mode = "sqs"
        else:
            # initialize the request retriever
            request_retriever = S3Retriever(consume_bucket_name)
            retriever_mode = "s3"

        # initialize what storage to use based on user input
        storage_handler = None
        if storage == "s3":
            storage_handler = S3Storage(bucket_name)
        elif storage == "dynamodb":
            storage_handler = DynamoDBStorage(table_name)

        while True:

            # try to get a request for s3
            if retriever_mode == "s3":
                request = request_retriever.get_request()

                # if there is a request, process the request
                if request is not None:

                    req_id = request.get("requestId", "unknown")

                    # if the request is to create a widget
                    if request.get("type") == "create":
                        try:
                            # try to create the widget
                            storage_handler.create_widget(request)
                            logging.info(f"Successfully created widget for request: {req_id}")
                        except Exception as e:
                            logging.error(f"Failed to create widget for request {req_id}: {e}")

                    # if the request is to delete a widget
                    elif request.get("type") == "delete":
                        storage_handler.delete_widget(request)
                        logging.info(f"Successfully deleted widget for request: {req_id}")

                    # if the request is to change a widget
                    elif request.get("type") == "update":
                        storage_handler.update_widget(request)
                        logging.info(f"Successfully updated widget for request: {req_id}")

                # else if there is no request, wait for 100 ms before checking again
                else:
                    time.sleep(0.1)

            # try to get a request for sqs
            elif retriever_mode == "sqs":
                message = request_retriever.get_request()

                # if there is a message, process the message
                if message is not None:
                    request_data, receipt_handle = message
                    req_id = request_data.get("requestId", "unknown")

                    try:
                        # if the request is to create a widget
                        if request_data.get("type") == "create":
                            storage_handler.create_widget(request_data)
                            logging.info(f"Successfully created widget for request: {req_id}")
                        
                        # if the request is to delete a widget
                        elif request_data.get("type") == "delete":
                            storage_handler.delete_widget(request_data)
                            logging.info(f"Successfully deleted widget for request: {req_id}")

                        # if the request is to update a widget
                        elif request_data.get("type") == "update":
                            storage_handler.update_widget(request_data)
                            logging.info(f"Successfully updated widget for request: {req_id}")

                        # If processing was successful, delete the message
                        request_retriever.delete_message(receipt_handle)
                        
                    except Exception as e:
                        logging.error(f"Failed to process SQS request {req_id}. Error: {e}. Message will not be deleted.")
    except KeyboardInterrupt:
        print("Stopping consumer...")

if __name__ == "__main__":
    consume_requests()

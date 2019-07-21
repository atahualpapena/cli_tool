import click
import json
import boto3
import sys
import os
import time
import datetime
from botocore.client import ClientError
from os.path import join, dirname


@click.command()
@click.option('--session', prompt='type your aws account ? Default: ',
              default='default', help='default aws account')
@click.option("--output", prompt="typpe output filename ? Default: ",
              default="query_results", help="filename")
@click.option("--statement", prompt="type your sql statement ",
              help="select * from table_name")
@click.option("--package", prompt="type your package filename ? Default: ",
              default="package", help="filename")
@click.option("--bucket_name", prompt="type your bucket name ? Default: ",
              default="my_bucket", help="bucket name from aws account")
def build(output, statement, package, bucket_name, session):
    client = boto3.client('s3', aws_acccess_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY,
                          aws_session_token=SESSION_TOKEN)

    account_id = client.get_caller_identity()['Account']


s3 = boto3.client('s3')
sqs = boto3.client('sqs', region_name='us-west-2')
session = boto3.session.Session(profile_name=session)
queue_url = "https://sqs.us-west-2.amazonaws.com/915526749187/dest-queue-test"
bucket = s3.list_buckets()

bucket_list = bucket['Buckets']
for i in bucket_list:
    if bucket_name in i['Name']:
        bucket = bucket_name
scheduler = input('do you want to schedule this script ?')
if scheduler == 'yes' or scheduler == 'y':
    cron = input('type your schedule, Default : cron(0/1 * * * ? *)]')
    data = {}
    data['query'] = statement
    data['output_file_prefix'] = output +
    str(datetime.datetime.now()) + '.csv'
    data['cron'] = cron
else:
    data = {}
    data['query'] = statement
    data['output_file_prefix'] = output +
    str(datetime.datetime.now()) + ".csv"
json_data = json.dumps(data, indent=4)
with open(package + ".json", "w+") as file:
    file.write(json_data)
    print("file created:")
    print(json_data)
    answer = input("would you like to upload this file ? ")
    if answer == "yes":
        print("uploading")
        s3.upload_file(package + ".json", the_bucket, package + ".json")
        print("file uploaded successfully")
        os.remove(package + ".json")

    time.sleep(20)
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'Body'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    message = response['Messages']
    parsed_msg = json.dumps(message)
    json_msg = json.loads(parsed_msg)
    url_msg = json_msg[0]['Body']
    # receipt_handle = message['ReceiptHandle']
    print("here's your download link: " + url_msg)
    # sqs.delete_message(
    # QueueUrl = queue_url,
    # ReceiptHandle = receipt_handle
    # )

    sys.exit()
    if answer == "no":
        print("your answer was no")
    else:
        print("i did not understand that")

    if __name__ == '__main__':
        build()

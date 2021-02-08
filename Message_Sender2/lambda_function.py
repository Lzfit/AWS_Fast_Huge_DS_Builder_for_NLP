# Send messages to SQS Queue using AWS Lambda

import boto3
import pandas as pd

#  list of URLs and respective countries
df = pd.read_csv('URLs_part1.csv', header=0, encoding='latin-1')

# #  accessing the sqs queue
queue_url = 'https://sqs.us-west-1.amazonaws.com/867071081797/LineOfDownloads.fifo'
sqs = boto3.client('sqs', region_name='us-west-1', endpoint_url=queue_url)

#  lambda function to send messages to sqs queue
def lambda_handler(event, context):

    Entries = []

    #  one message for each URL
    for i in range(0, len(df)):

        iD = str(i)

        Entry = {
                    'Id': iD,
                    'MessageBody': iD,
                    'MessageAttributes': {
                        'country': {
                            'DataType': 'String',
                            'StringValue': df.Country[i]
                        },
                        'state': {
                            'DataType': 'String',
                            'StringValue': df.State[i]
                        },
                        'URL': {
                            'DataType': 'String',
                            'StringValue': df.URL[i]
                        }
                    },

                    'MessageGroupId':  iD
        }

        Entries.append(Entry)

        if i % 10 == 9:
            response = sqs.send_message_batch(
                QueueUrl = queue_url,
                Entries = Entries)

            Entries = []

        elif i == (len(df)-1):
            response = sqs.send_message_batch(
                QueueUrl = queue_url,
                Entries = Entries)

    return {
        'Message Status': '200'
    }
# Poll messages from SQS Queue, download and store data in S3 buckets with AWS Lambda
import boto3
import botocore.vendored.requests.packages.urllib3 as urllib3

# lambda function to poll messages from sqs queue, download and store data in S3 buckets

def lambda_handler(event, context):

    s3=boto3.client('s3')

    #  process each message - up to 10 at same time
    records = event['Records']

    for item in records:

        #  extract information from message
        country = item['messageAttributes']['country']['stringValue']
        state = item['messageAttributes']['state']['stringValue']
        URL = item['messageAttributes']['URL']['stringValue']
        name = URL.split('/')[-1]

        #  download data from external url
        bucket = 'apify-wri-pdfs'
        key = country + "/" + state + "/" + name

        http=urllib3.PoolManager(cert_reqs = 'CERT_REQUIRED')

        try:
            s3.upload_fileobj(http.request('GET', URL,preload_content=False), bucket, key)

        except:
            continue

    return {

        'Status': '200'

    }
__version__ = '0.1.0'

import boto3
from .aws_creds import s3_bucket


def main():
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket, 'locations.json', 'locations.json')


main()

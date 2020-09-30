__version__ = '0.1.0'
import json
import boto3


class SQSQueue:
    def __init__(self, sqs_client, url, policy):
        self.client = sqs_client
        self.url = url
        self.policy = policy

    def subscribe_to_queue(self):
        return self.client.set_queue_attributes(QueueUrl=self.url, Attributes={'Policy': self.policy})

    def receive_message(self):
        return self.client.receive_message(QueueUrl=self.url, WaitTimeSeconds=5)

    def delete_message_from_queue(self, sqs_receipt_handle):
        return self.client.delete_message(QueueUrl=self.url, ReceiptHandle=sqs_receipt_handle)

    @staticmethod
    def subscribe_event_source_to_queue(sns_client, arn_topic, protocol, arn_queue):
        sns_client.subscribe(TopicArn=arn_topic, Protocol=protocol, Endpoint=arn_queue)

def main():
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

    locations_file = s3.get_object(Bucket='eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7',
                      Key='locations.json')['Body'].read().decode('utf-8')

    locations = json.loads(locations_file)

    for location in locations:
        print("{} x={} y={}".format(location['id'], location['x'], location['y']))
    print('\n')

    aws_queue = sqs.create_queue(QueueName='queue')
    queue_url = aws_queue['QueueUrl']
    arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    queue_arn = arn['Attributes']['QueueArn']

    # grant permission for sns topic to write to the sqs queue
    topic_arn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Altran-snsTopicSensorDataPart1-1HJ83JI0COKVB'

    # Abstract this out into policy class/method
    policy_document = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': f'allow-subscription-{topic_arn}',
            'Effect': 'Allow',
            'Principal': {'AWS': '*'},
            'Action': 'SQS:SendMessage',
            'Resource': f'{queue_arn}',
            'Condition': {
                'ArnEquals': {'aws:SourceArn': f'{topic_arn}'}
            }
        }]
    }
    policy_json = json.dumps(policy_document)

    sqs_queue = SQSQueue(sqs_client=sqs, url=queue_url, policy=policy_json)
    sqs_queue.subscribe_to_queue()
    SQSQueue.subscribe_event_source_to_queue(sns, topic_arn, 'sqs', queue_arn)
    messages_and_metadata = sqs_queue.receive_message()
    messages = messages_and_metadata['Messages']

    messages_to_analyse = []
    for message in messages:
        sqs_receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])
        message_content = json.loads(body['Message'])
        messages_to_analyse.append(message_content)
        sqs_queue.delete_message_from_queue(sqs_receipt_handle)
main()

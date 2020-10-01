__version__ = '0.1.0'
import json
import boto3
from pprint import pprint

class Sensor:
    def __init__(self, sensor_id, x_coord, y_coord):
        self.id = sensor_id
        self.x = x_coord
        self.y = y_coord


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


def known_sensor_locations(s3_client):
    locations_file = s3_client.get_object(Bucket='eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7',
                                   Key='locations.json')['Body'].read().decode('utf-8')
    locations = json.loads(locations_file)
    return locations


def main():
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')

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
    SQSQueue.subscribe_event_source_to_queue(sns_client=sns, arn_topic=topic_arn, protocol='sqs', arn_queue=queue_arn)

    locations = known_sensor_locations(s3_client=s3)

    sensor_data = {}
    for location in locations:
        location_id = location['id']
        sensor_data[location_id] = []

    for i in range(0, 10):
        messages_and_metadata = sqs_queue.receive_message()
        messages = messages_and_metadata['Messages']
        for message in messages:

            sqs_receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            message_content = json.loads(body['Message'])
            location_id = message_content['locationId']
            timestamp = message_content['timestamp']
            event_id = message_content['eventId']
            value = message_content['value']
            if location_id in sensor_data:
                sensor_data[location_id].append({'timestamp': timestamp, 'event_id': event_id, 'reading': value})
            sqs_queue.delete_message_from_queue(sqs_receipt_handle)
    pprint(sensor_data)

main()

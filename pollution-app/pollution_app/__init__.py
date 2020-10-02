__version__ = '0.1.0'
import json
import boto3
import time
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

    def delete_queue(self):
        self.client.delete_queue(self.url)

    @staticmethod
    def subscribe_event_source_to_queue(sns_client, arn_topic, protocol, arn_queue):
        sns_client.subscribe(TopicArn=arn_topic, Protocol=protocol, Endpoint=arn_queue)


def known_sensor_locations(s3_client):
    locations_file = s3_client.get_object(Bucket='eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7',
                                   Key='locations.json')['Body'].read().decode('utf-8')
    locations = json.loads(locations_file)
    return locations


def minutes_to_seconds(minutes):
    return minutes*60


def seconds_to_minutes(seconds):
    return seconds/60


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

    data_from_sensors = {}
    for location in locations:
        location_id = location['id']
        data_from_sensors[location_id] = []

    running_time = minutes_to_seconds(1)
    time_at_startup = time.time()
    event_identifiers = []
    while time.time() < time_at_startup + running_time:

        messages_and_metadata = sqs_queue.receive_message()
        messages = messages_and_metadata['Messages']
        for message in messages:
            sqs_receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            try:
                message_content = json.loads(body['Message'])
                location_id = message_content['locationId']
                timestamp = message_content['timestamp']
                event_id = message_content['eventId']
                value = message_content['value']
                if event_id in event_identifiers:
                    continue
                else:
                    event_identifiers.append(event_id)
                    if location_id in data_from_sensors:
                            data_from_sensors[location_id].append({'timestamp': timestamp, 'event_id': event_id, 'reading': value})
            except:
                print("Malformed message was rejected")
            sqs_queue.delete_message_from_queue(sqs_receipt_handle)
    minutes_passed = seconds_to_minutes(time.time() - time_at_startup)
    print(round(minutes_passed, 2))
    for events in data_from_sensors.values():
        events.sort(key=lambda event: event['timestamp'])
    sqs.delete_queue(QueueUrl=queue_url)


main()

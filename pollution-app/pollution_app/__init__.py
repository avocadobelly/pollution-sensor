__version__ = '0.1.0'
import json
import boto3

def main():
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')
    f = s3.get_object(Bucket='eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7', Key='locations.json')['Body'].read().decode('utf-8')

    locations = json.loads(f)

    for location in locations:
        print("{} x={} y={}".format(location['id'], location['x'], location['y']))
    print('\n')
    aws_queue = sqs.create_queue(QueueName='queue')
    queue_url = aws_queue['QueueUrl']
    arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
    queue_arn = arn['Attributes']['QueueArn']

    # grant permission for sns topic to write to the sqs queue
    topic_arn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Altran-snsTopicSensorDataPart1-1HJ83JI0COKVB'

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

    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': policy_json})

    # subscribe event source to the queue
    subscription_to_queue = sns.subscribe(TopicArn=topic_arn, Protocol='sqs', Endpoint=queue_arn)

    # receive messages from the queue
    messages_and_metadata = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
    messages = messages_and_metadata['Messages']
    for message in messages:
        body = json.loads(message['Body'])
        sns_message_id = body['MessageId']
        print(sns_message_id)
        print('\n')
        message_content = json.loads(body['Message'])
        print(message_content)
        print('\n')
main()

import dataclasses

import boto3


@dataclasses.dataclass
class SqsQueue:
    _aws_session: boto3.session.Session
    queue_name: str
    _queue_url: str

    def __init__(self, aws_session: boto3.session.Session, queue_name: str):
        self._aws_session = aws_session
        self.queue_name = queue_name
        self._queue_url = ''

        sqs_client = self._aws_session.client('sqs')
        try:
            response = sqs_client.get_queue_url(
                QueueName=self.queue_name,
            )
        except Exception as e:
            raise e

        self._queue_url = response['QueueUrl']

    def message_count(self) -> int:
        sqs_client = self._aws_session.client('sqs')
        try:
            response = sqs_client.get_queue_attributes(
                QueueUrl=self._queue_url,
                AttributeNames=['ApproximateNumberOfMessages'],
            )
        except Exception as e:
            raise e

        return int(response['Attributes']['ApproximateNumberOfMessages'])

    def send_message(self, message: str) -> None:
        sqs_session = self._aws_session.client('sqs')
        try:
            _ = sqs_session.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message,
            )
        except Exception as e:
            raise e

    def receive_messages(self, max_messages: int) -> list:
        sqs_client = self._aws_session.client('sqs')
        try:
            response = sqs_client.receive_message(
                QueueUrl=self._queue_url,
                AttributeNames=['SentTimestamp'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
            )
        except Exception as e:
            raise e

        return response['Messages']

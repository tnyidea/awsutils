import dataclasses

import botocore.exceptions

from .session import Session


@dataclasses.dataclass
class SqsQueue:
    session: Session = None
    queue_name: str = ''
    queue_arn: str = ''
    queue_url: str = ''

    def exists(self) -> bool:
        try:
            sqs_client = self.session.sqs_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_url.html
            _ = sqs_client.get_queue_url(
                QueueName=self.queue_name,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                return False
            else:
                raise e
        except Exception as e:
            raise e

        return True

    def delete(self) -> None:
        try:
            sqs_client = self.session.sqs_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_queue.html
            sqs_client.delete_queue(
                QueueUrl=self.queue_url
            )
        except Exception as e:
            raise e

        return None

    def message_count(self) -> int:
        try:
            sqs_client = self.session.sqs_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_attributes.html
            response = sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages'],
            )
        except Exception as e:
            raise e

        return int(response['Attributes']['ApproximateNumberOfMessages'])

    def send_message(self, message: str) -> None:
        try:
            sqs_client = self.session.sqs_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/send_message.html
            _ = sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message,
            )
        except Exception as e:
            raise e

    def receive_messages(self, max_messages: int) -> list:
        try:
            sqs_client = self.session.sqs_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            response = sqs_client.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=['SentTimestamp'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
            )
        except Exception as e:
            raise e

        return response['Messages']


def create_sqs_queue(aws_session: Session, queue_name: str) -> SqsQueue:
    sqs_client = aws_session.sqs_client()

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/create_queue.html
        _ = sqs_client.create_queue(
            QueueName=queue_name,
        )
    except Exception as e:
        raise e

    return new_sqs_queue(aws_session, queue_name)


# TODO: Look at how to correctly overload class constructors in python
#  external constructors offer more flexibility in return types -- unless a better way?
def new_sqs_queue(aws_session: Session, queue_name: str) -> SqsQueue:
    sqs_queue = SqsQueue()
    sqs_queue.session = aws_session
    sqs_queue.queue_name = queue_name

    sqs_client = sqs_queue.session.sqs_client()

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_url.html
        response = sqs_client.get_queue_url(
            QueueName=sqs_queue.queue_name,
        )
        sqs_queue.queue_url = response['QueueUrl']

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/get_queue_attributes.html
        response = sqs_client.get_queue_attributes(
            QueueUrl=sqs_queue.queue_url,
            AttributeNames=[
                'QueueArn',
            ]
        )
        sqs_queue.queue_arn = response['Attributes']['QueueArn']
    except Exception as e:
        raise e

    return sqs_queue

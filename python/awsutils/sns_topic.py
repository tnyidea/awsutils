import dataclasses

import botocore.exceptions

from .session import Session
from .sqs_queue import SqsQueue


@dataclasses.dataclass
class SnsTopic:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
    session: Session = None
    topic_name: str = ''
    topic_arn: str = ''

    def exists(self) -> bool:
        try:
            sns_client = self.session.sns_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/get_topic_attributes.html
            _ = sns_client.get_topic_attributes(
                TopicArn=self.topic_arn
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NotFound':
                return False
            else:
                raise e
        except Exception as e:
            raise e

        return True

    def delete(self) -> None:
        try:
            sns_client = aws_session = self.session.sns_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/delete_topic.html
            sns_client.delete_topic(
                TopicArn=self.topic_arn
            )
        except Exception as e:
            raise e

        return None

    def publish(self, subject: str, message: str) -> None:
        try:
            sns_client = self.session.sns_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
            _ = sns_client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject,
                MessageStructure='json',
            )
        except Exception as e:
            raise e

        return None

    def subscribe_sqs_queue(self, sqs_queue: SqsQueue) -> str:
        try:
            subscription_arn = self._subscribe('sqs', sqs_queue.queue_arn)
        except Exception as e:
            raise e

        return subscription_arn

    def _subscribe(self, protocol: str, endpoint: str) -> str:
        try:
            sns_client = self.session.sns_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/subscribe.html
            response = sns_client.subscribe(
                TopicArn=self.topic_arn,
                Protocol=protocol,
                Endpoint=endpoint,
                ReturnSubscriptionArn=True
            )
        except Exception as e:
            raise e

        return response['SubscriptionArn']

    def _get_topic_attributes(self) -> dict:
        try:
            sns_client = self.session.sns_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/get_topic_attributes.html
            response = sns_client.get_topic_attributes(
                TopicArn=self.topic_arn
            )
        except Exception as e:
            raise e

        return response


def create_sns_topic(aws_session: Session, topic_name: str) -> SnsTopic:
    return new_sns_topic(aws_session, topic_name)


def new_sns_topic(aws_session: Session, topic_name: str) -> SnsTopic:
    sns_topic = SnsTopic()
    sns_topic.session = aws_session
    sns_topic.topic_name = topic_name

    try:
        sns_client = aws_session.sns_client()

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/create_topic.html
        response = sns_client.create_topic(
            Name=topic_name,
        )
        sns_topic.topic_arn = response['TopicArn']

    except Exception as e:
        raise e

    return sns_topic

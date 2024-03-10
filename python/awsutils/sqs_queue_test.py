# import unittest
# import unittest.mock as mock
#
# import boto3
# import botocore.stub
#
# import awsutils
#
# ENCODING_UTF_8 = 'utf-8'
#
#
# @mock.patch('boto3.session.Session')
# class TestMockSqsQueue(unittest.TestCase):
#     def setUp(self):
#         self.test_region = 'us-east-2'
#         self.test_queue_name = 'awsutils-test-queue'
#         self.test_queue_url = 'test-queue-url'
#
#         self.mock_get_queue_attributes_request = {
#             'QueueUrl': self.test_queue_url,
#             'AttributeNames': ['ApproximateNumberOfMessages'],
#         }
#         self.mock_get_queue_attributes_response = {
#             'Attributes': {
#                 'ApproximateNumberOfMessages': '3',
#             },
#         }
#         self.mock_get_queue_url_request = {
#             'QueueName': self.test_queue_name,
#         }
#         self.mock_get_queue_url_response = {
#             'QueueUrl': self.test_queue_url
#         }
#         self.mock_receive_message_request = {
#             'QueueUrl': self.test_queue_url,
#             'AttributeNames': ['SentTimestamp'],
#             'MessageAttributeNames': ['All'],
#             'MaxNumberOfMessages': 10,
#         }
#         self.mock_receive_message_response = {
#             'Messages': [
#                 {
#                     'MessageId': 'test_message_id1',
#                     'ReceiptHandle': 'test_receipt_handle1',
#                     'Body': 'test_message1',
#                 },
#                 {
#                     'MessageId': 'test_message_id2',
#                     'ReceiptHandle': 'test_receipt_handle2',
#                     'Body': 'test_message2',
#                 },
#                 {
#                     'MessageId': 'test_message_id3',
#                     'ReceiptHandle': 'test_receipt_handle3',
#                     'Body': 'test_message3',
#                 },
#             ]
#         }
#         self.mock_send_message_request = {
#             'QueueUrl': self.test_queue_url,
#             'MessageBody': 'test_message1'
#         }
#         self.mock_send_message_response = {
#             'MD5OfMessageBody': 'test_message_body_md5',
#             'MD5OfMessageAttributes': 'test_message_attributes_md5',
#             'MD5OfMessageSystemAttributes': 'test_message_system_attributes_md5',
#             'MessageId': 'test_message_id',
#             'SequenceNumber': 'test_message_sequence_number'
#         }
#
#     def test_new_sqs_queue(self, mock_boto3_session):
#         # SQS seems to require a region where S3 does not
#         mock_boto3_client = boto3.client('sqs', self.test_region)
#         stubber = botocore.stub.Stubber(mock_boto3_client)
#         stubber.add_response('get_queue_url', self.mock_get_queue_url_response,
#                              self.mock_get_queue_url_request)
#         stubber.activate()
#
#         with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
#             sqs_queue = awsutils.SqsQueue(mock_boto3_session, self.test_queue_name)
#
#             expected = {
#                 '_aws_session': mock_boto3_session,
#                 'queue_name': self.test_queue_name,
#                 '_queue_url': self.test_queue_url,
#             }
#             self.assertEqual(expected, sqs_queue.__dict__)
#
#     def test_message_count(self, mock_boto3_session):
#         mock_boto3_client = boto3.client('sqs', self.test_region)
#         stubber = botocore.stub.Stubber(mock_boto3_client)
#         stubber.add_response('get_queue_url', self.mock_get_queue_url_response,
#                              self.mock_get_queue_url_request)
#         stubber.add_response('get_queue_attributes', self.mock_get_queue_attributes_response,
#                              self.mock_get_queue_attributes_request)
#         stubber.activate()
#
#         with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
#             sqs_queue = awsutils.SqsQueue(mock_boto3_session, self.test_queue_name)
#
#             message_count = sqs_queue.message_count()
#             expected = 3
#
#             self.assertEqual(expected, message_count)
#
#     def test_receive_messages(self, mock_boto3_session):
#         mock_boto3_client = boto3.client('sqs', self.test_region)
#         stubber = botocore.stub.Stubber(mock_boto3_client)
#         stubber.add_response('get_queue_url', self.mock_get_queue_url_response,
#                              self.mock_get_queue_url_request)
#         stubber.add_response('receive_message', self.mock_receive_message_response,
#                              self.mock_receive_message_request)
#         stubber.activate()
#
#         with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
#             sqs_queue = awsutils.SqsQueue(mock_boto3_session, self.test_queue_name)
#
#             messages = sqs_queue.receive_messages(10)
#
#             result = []
#             for message in messages:
#                 result.append(message['Body'])
#
#             expected = [
#                 'test_message1',
#                 'test_message2',
#                 'test_message3',
#             ]
#             self.assertEqual(expected, result)
#
#     def test_send_message(self, mock_boto3_session):
#         mock_boto3_client = boto3.client('sqs', self.test_region)
#         stubber = botocore.stub.Stubber(mock_boto3_client)
#         stubber.add_response('get_queue_url', self.mock_get_queue_url_response,
#                              self.mock_get_queue_url_request)
#         stubber.add_response('send_message', self.mock_send_message_response,
#                              self.mock_send_message_request)
#         stubber.activate()
#
#         with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
#             sqs_queue = awsutils.SqsQueue(mock_boto3_session, self.test_queue_name)
#
#             sqs_queue.send_message('test_message1')
#
#
# class TestSqsQueue(unittest.TestCase):
#     def setUp(self):
#         self.test_region = 'us-east-2'
#         self.test_queue = 'awsutils-test-queue'
#         self.test_message = 'test-message'
#
#     def test_new_sqs_queue(self):
#         aws_session = awsutils.new_aws_session_for_region(self.test_region)
#         try:
#             sqs_queue = awsutils.SqsQueue(aws_session, self.test_queue)
#         except Exception as e:
#             self.fail(e)
#
#         print(sqs_queue.__dict__)
#
#     def test_send_message(self):
#         aws_session = awsutils.new_aws_session_for_region(self.test_region)
#         sqs_queue = awsutils.SqsQueue(aws_session, self.test_queue)
#         try:
#             sqs_queue.send_message(self.test_message)
#         except Exception as e:
#             self.fail(e)
#
#     def test_message_count(self):
#         aws_session = awsutils.new_aws_session_for_region(self.test_region)
#         sqs_queue = awsutils.SqsQueue(aws_session, self.test_queue)
#         sqs_queue.send_message(self.test_message)
#         try:
#             message_count = sqs_queue.message_count()
#         except Exception as e:
#             self.fail(e)
#
#         self.assertEqual(message_count, 1)
#
#     def test_receive_messages(self):
#         aws_session = awsutils.new_aws_session_for_region(self.test_region)
#         sqs_queue = awsutils.SqsQueue(aws_session, self.test_queue)
#         sqs_queue.send_message(self.test_message + '1')
#         sqs_queue.send_message(self.test_message + '1')
#         try:
#             messages = sqs_queue.receive_messages(10)
#         except Exception as e:
#             self.fail(e)
#
#         self.assertEqual(len(messages), 2)
#
#
# if __name__ == '__main__':
#     unittest.main()

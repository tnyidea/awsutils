import unittest
import unittest.mock as mock

import awsutils


class TestSession(unittest.TestCase):
    @mock.patch('boto3.session')
    def test_new_aws_session_for_region(self, mock_boto3_session):
        region = 'us-east-2'
        session = awsutils.new_aws_session_for_region(region)

        mock_boto3_session.Session.called_with(region)
        self.assertIsNotNone(session)


if __name__ == '__main__':
    unittest.main()

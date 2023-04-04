"""Testing our class s3
"""

import os
import unittest
import boto3
from moto import mock_s3

from src.common.s3 import S3BucketConnector

class TestS3BucketConnector(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """
    def setUp(self):
        """Setting up the environment
        """
        # Mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY'
        self.s3_secret_key = 'AWS_SECRET_KEY'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'Key1'
        os.environ[self.s3_secret_key] = 'Key2'
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(
            service_name='s3'
        )
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'
            }
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_bucket_name
        )

    def tearDown(self) -> None:
        """Executing after unittests

        Returns:
            _type_: _description_
        """
        # mocking s3 connedion stop 
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocled s3 bucket
        """
        # Expeted results
        prefix_exp = 'prefix/'
        key1_exp = f"{prefix_exp}test1.csv"
        key2_exp = f"{prefix_exp}test2.csv"
        # Test init
        csv_content = """
            col1, col2
            val1, val2
        """
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix=prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after testing
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong(self):
        """Tests the list_files_in_prefix method in case of a wrong or not existing prefix
        """
        # Excepted results
        prefix_exp = 'prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix=prefix_exp)
        # Test after execution method 
        self.assertTrue(not list_result)

if __name__ == '__main__':
    unittest.main()

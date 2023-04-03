""" Connector and methods accessing to S3
"""
import logging
import os
from io import StringIO, BytesIO
from typing import List
from pandas.core.frame import DataFrame
import pandas as pd
from src.common.constants import S3FileType
import boto3

class S3BucketConnector():

    """Class for interacting with S3 Bucket
    """

    def __init__(self, access_key: str, secret_key: str, bucket: str):
        """Constructor for S3BucketConnector

        Args:
            access_key (str): _description_
            secret_key (str): _description_
            bucket (str): _description_
        """
        self.logger = logging.getLogger(__name__)
        self.session = boto3.Session(
            aws_access_key_id=os.getenv(access_key),
            aws_secret_access_key=os.getenv(secret_key)
        )
        self._s3 = self.session.resource(
            service_name = 's3'
        )
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix:str) -> List[str]:
        """List of file in prefix

        Args:
            prefix (str): prefix for filter data in s3 bucket

        Returns:
            List[str]: list of keys to get data from s3 bucket by key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key_object:str, encoding: str = "utf-8", sep:str = ',') -> DataFrame:
        """Read csv file from s3 bucket into a dataframe

        Args:
            key_object (str): key object in s3 bucket
            encoding (str, optional): type encoding. Defaults to "utf-8".
            sep (str, optional): sep to read csv file into df. Defaults to ','.

        Returns:
            DataFrame: return dataframe
        """
        csv_obj = self._bucket.Object(key=key_object).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data, delimiter = sep)
        return dataframe

    def write_df_to_s3(self, dataframe: DataFrame, key_object: str):
        """write dataframe to s3 bucket

        Args:
            dataframe (DataFrame): dataframe we want to save in s3 bucket
            key_object (str): name object (dataframe) in s3 bucket
        """
        out_buffer = None
        _, file_extension = os.path.splitext(key_object)
        if file_extension == S3FileType.CSV.value:
            out_buffer = StringIO()
            dataframe.to_csv(out_buffer, index=False)
        if file_extension == S3FileType.PARQUET.value:
            out_buffer = BytesIO()
            dataframe.to_parquet(out_buffer, index=False)
        if out_buffer is not None:
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key_object)
        else:
            self.logger.error("stream data frame is null")

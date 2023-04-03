""" Methods for processing the meta file
"""
from datetime import datetime, timedelta
from typing import Tuple, List
import pandas as pd
from dateutil.relativedelta import relativedelta
from src.common.s3 import S3BucketConnector
from src.common.constants import MetaProcessFormat

class MetaProcess():

    """Class for working with the meta file
    """

    @staticmethod
    def update_meta_file( s3_bucket_meta: S3BucketConnector, extract_date_list: List[str],
                        meta_key: str):
        """_summary_

        Args:
            extract_date_list (List[str]): _description_
            meta_key (str): _description_
            s3_bucket_meta (S3BucketConnector): _description_
        """
        date_extract = datetime.today().date().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        df_new = pd.DataFrame(
            data={
                    'source_date': extract_date_list,
                    'datetime_of_processing': [date_extract] * len(extract_date_list)
                }
        )
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            df_data = pd.concat([df_new, df_old], ignore_index=True)
        except Exception as error:
            s3_bucket_meta.logger.error(error)
            df_data = df_new

        s3_bucket_meta.write_df_to_s3(df_data, meta_key)

    @staticmethod
    def get_date_list(
                      s3_bucket_meta: S3BucketConnector,
                      meta_key: str,
                      first_date: str
                    ) -> Tuple[List[str], str]:
        """_summary_

        Args:
            first_date (str): _description_
            meta_key (str): _description_
            s3_bucket_meta (S3BucketConnector): _description_

        Returns:
            Tuple[List[str], str]: _description_
        """
        min_date = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        max_date = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=1)
        try:
            today = datetime.today().date() - relativedelta(years=1)
            date_list = [(min_date + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(0, (today-min_date).days + 1)]
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            diff_date_list = set(date_list) - set(df_meta.source_date)
            if diff_date_list:
                list_date = [date for date in sorted(diff_date_list) if date > min_date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)]
                min_date = min(list_date)
            else:
                min_date = datetime.datetime(22800, 1, 1)
                list_date = []
        except Exception as exp:
            s3_bucket_meta.logger.error(exp)
            list_date = [(
                (min_date + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for x in range(0, (max_date - min_date).days + 1)
            )]
            min_date = first_date
        return list_date, min_date

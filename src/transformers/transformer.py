"""ETL Componet
"""
import logging
from typing import NamedTuple, List
from datetime import datetime
import pandas as pd

from pandas.core.frame import DataFrame
from src.common.s3 import S3BucketConnector
from src.common.meta_process import MetaProcess
from src.common.constants import MetaProcessFormat


class SourceConfig(NamedTuple):
    """Class for source configuration data

    Args:
        src_file_extract_date (str): determines the date for extracting the source
        src_columns (List[str]): source columns names
        src_col_date (str): column name for date in source
        src_col_isin (str): column name for isin in source
        src_col_time (str): column name for time in source
        src_col_start_price (str): column name for starting price in source
        src_col_min_price (str): column name for minimun price in source
        src_col_max_price (str): column name for maximun in source
        src_col_traded_vol (str): column name for traded volumne in source
    """
    src_file_extract_date: str
    src_columns: List[str]
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class TargetConfig(NamedTuple):
    """Class for target configuration data

    Args:
        trg_col_date (str): column name for date in target
        trg_col_isin (str): column name for isin in target
        trg_col_op_price (str): column name for opening price in target
        trg_col_clos_price (str): column name for closing price in target
        trg_col_min_price (str): column name for minimun price in target
        trg_col_max_price (str): column name for maximun price in target
        trg_col_dail_trad_vol (str): column name for daily traded volumne in target
        trg_col_ch_prev_clos (str): column name for chnage to previous day's closing price in target
        trg_key (str): basic key of target file
        trg_key_date_format (str): date format of target file key
        trg_format (str): file format of the target file
    """

    trg_col_date: str
    trg_col_isin: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class ETL():
    """Reads the source data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                s3_bucket_trg: S3BucketConnector, meta_key: str,
                src_args: SourceConfig, trg_args: TargetConfig):
        """Constructor for ETL

        Args:
            s3_bucket_src (S3BucketConnector): _description_
            s3_bucket_trg (S3BucketConnector): _description_
            meta_key (str): _description_
            src_args (SourceConfig): _description_
            trg_args (TargetConfig): _description_
            date_list (List[str]): list of date for extracting
        """
        self.logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.date_list = MetaProcess.get_date_list(
            s3_bucket_trg, self.meta_key, self.src_args.src_file_extract_date
        )[0]
        self.logger.info(self.date_list)
    def extract(self) -> DataFrame:
        """extract data from s3 bucket by prefix date


        Returns:
            DataFrame: dataframe
        """
        files = [key for date in self.date_list for key in self.s3_bucket_src.list_files_in_prefix(date)]
        df_data = pd.concat(
            [self.s3_bucket_src.read_csv_to_df(obj) for obj in files], ignore_index=True
        )
        return df_data

    def transform_report_one(self, dataframe: DataFrame) -> DataFrame:
        """_summary_

        Args:
            dataframe (DataFrame): _description_
            first_date (str): _description_

        Returns:
            DataFrame: _description_
        """
        dataframe = dataframe[self.src_args.src_columns]
        dataframe['OpeningPrice'] = (
            dataframe
            .sort_values(by=['Time'])
            .groupby(['ISIN', 'Date'])['StartPrice']
            .transform('first')
        )
        dataframe['ClosingPrice'] = (
            dataframe
            .sort_values(by=['Time'])
            .groupby(['ISIN', 'Date'])['StartPrice']
            .transform('last')
        )

        dataframe = dataframe.groupby(["ISIN", "Date"], as_index=False)\
                .agg(
                    opening_price_eur=('OpeningPrice', 'min'),
                    closing_price_eur=('ClosingPrice', 'min'),
                    minimun_price_eur=('MinPrice', 'min'),
                    maxmun_price_eur=('MaxPrice', 'max'),
                    daily_traded_volumne=('TradedVolume', 'sum')
                )

        dataframe["prev_closing_price"] = (
            dataframe.sort_values(by=['Date'])
                .groupby('ISIN')['closing_price_eur'].shift(1)
        )

        dataframe["change_prev_closing_%"] = (
            (dataframe['closing_price_eur'] - dataframe['prev_closing_price']) / dataframe['prev_closing_price'] * 100
        )

        dataframe.drop(columns="prev_closing_price", inplace=True)
        dataframe = dataframe.round(decimals=2)
        dataframe = dataframe[dataframe['Date'] >= self.src_args.src_file_extract_date]
        return dataframe

    def load(self, dataframe: DataFrame):
        """_summary_

        Args:
            dataframe (DataFrame): _description_
            meta_key (str): _description_
        """
        str_date:str = datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        key = f"{self.trg_args.trg_key}{str_date}{MetaProcessFormat.META_DATE_FORMAT.value}"
        self.s3_bucket_trg.write_df_to_s3(dataframe, key)
        MetaProcess.update_meta_file(self.s3_bucket_trg, self.date_list,self.meta_key)

    def etl_report_one(self):
        """_summary_

        Args:
            date_list (List[str]): _description_
            first_date (str): _description_
            meta_key (str): _description_
        """
        # Extract
        dataframe = self.extract()
        # Transform
        dataframe = self.transform_report_one(dataframe)
        # Load
        self.load(dataframe)

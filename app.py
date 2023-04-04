"""Runing the ETL application
"""

import logging
import logging.config
import argparse
import yaml
from dotenv import load_dotenv


from src.common.s3 import S3BucketConnector
from src.transformers.transformer import ETL, SourceConfig, TargetConfig

def load_config():
    """To load configuration from file .env
    """
    load_dotenv()

def main():
    """Entry point to run ETL application"""
    # run load env
    load_config()
    # Parsing YAML file
    parser = argparse.ArgumentParser(description="Run the ETL")
    parser.add_argument('config', help='A configuration file YAML')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))


    # real_path = os.path.dirname(os.path.realpath('__file__'))
    # config_path = f"{real_path}/configs/report_one_config.yaml"
    # config = yaml.safe_load(open(config_path))

    # Configuration logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a sample test")

    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector for source and target bucket
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                     # endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      #endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])

    # Reading source configuration
    source_config = SourceConfig(**config['source'])
    # Reading target configuration
    target_config = TargetConfig(**config['target'])
    # reading meta file configuration
    meta_config = config['meta']['meta_key']
    # Creating ETL class
    logger.info('Xetra ETL job started')

    xetra_etl = ETL(
        s3_bucket_src=s3_bucket_src,
        s3_bucket_trg=s3_bucket_trg,
        meta_key=meta_config,
        src_args=source_config,
        trg_args=target_config
    )

    xetra_etl.etl_report_one()
    logger.info('Xetra ETL job finished')


if __name__ == '__main__':
    main()

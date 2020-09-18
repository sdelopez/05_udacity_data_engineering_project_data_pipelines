from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # Defining operators params (with defaults)
    @apply_defaults
    def __init__(self,
                 tables=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Pass a list of table to check
        # loop over table to count the number of records in each table
        for table in self.tables:
            self.log.info(f'Starting data quality check for {table} in Redshift')

            # for each table, count number of recors
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
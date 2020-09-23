from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # Defining operators params (with defaults)
    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        # passing a list of sql query for data quality check
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # check that there no empty list for sql query
        if len(self.dq_checks) == 0:
            self.log.info("There is no SQL query for Data Quality Check")
            return

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info(f'Starting data quality check in Redshift')
        # Pass a list of sql query for data quality check
        
        # Initialize counter to track quality check error
        error_count = 0
        failing_tests = []

        # loop over list of query for quality check to execute in Redshift
        for check in self.dq_checks:
            # get SQL query
            sql = check.get('check_sql')
            # get expected results 
            exp_result = check.get('expected_result')

            self.log.info(f"Running query: {sql}")
            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        # display in log the failed query
        if error_count > 0:
            self.log.info('Tests failed for the follwing query :')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info('Data Quality checks passed on all query')
    
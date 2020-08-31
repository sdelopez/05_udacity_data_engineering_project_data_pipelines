from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    # Defining operators params (with defaults)
    def __init__(self,
                table,
                redshift_conn_id = 'redshift',
                select_sql = '',
                append_data = False,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_data = False

    def execute(self, context):
        # use PostgresHook for Redshift
        redshift_hook = PostgresHook('redshift')

        # if append_data = False - delete data in existing table before inserting new data
        if self.append_data == False:
            self.log.info('Deleting data from {} fact table...'.format(self.table))
            redshift_hook.run('DELETE FROM {};'.format(self.table))
            self.log.info('Deleting data from {} fact table completed'.format(self.table))

        self.log.info('Loading data to {} fact table'.format(self.table))

        # define SQL query to inser data
        sql = """
            INSERT INTO {table}
            {select_sql};
            """.format(table=self.table, select_sql=self.select_sql)
        
        # run SQL query
        redshift_hook.run(sql)

        self.log.info('Loading data to {} fact table completed'.format(self.table))
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_stmt='',
                 expected_result='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.expected_result = expected_result
        
    def execute(self, context):
        self.log.info('Data Quality Starting')
        self.log.info('Getting credentials')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift.get_records(self.sql_stmt)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed.{records[0][0]} does not equal {self.expected_result}
                """)
        else:
            self.log.info("Data quality check passed")
            
        self.log.info('Done')
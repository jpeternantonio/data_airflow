from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_statement='',
                 table='',
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.table = table
        self.append_data = append_data
        
    def execute(self, context):
        self.log.info('LoadFactOperator Starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            self.log.info('Inserting from staging into {}'.format(self.table))
            insert_statement = 'INSERT INTO {} ({})'.format(self.table, self.sql_statement)
            redshift.run(insert_statement)
            
        else:
            delete_statement = "DELETE FROM {}".format(self.table)
            redshift.run(delete_statement)
            
            insert_statement = 'INSERT INTO {} ({})'.format(self.table, self.sql_statement)
            redshift.run(insert_statement)
      
        self.log.info('Done.')
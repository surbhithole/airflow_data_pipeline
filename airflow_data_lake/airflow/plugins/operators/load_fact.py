from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_into_table_query = """
        INSERT INTO {} 
        {}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 insert_fact_table_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_fact_table_query = insert_fact_table_query
       
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #redshift.run(SqlQueries.songplay_table_insert)
        redshift.run(LoadFactOperator.insert_into_table_query.format(self.table, self.insert_fact_table_query))
        #redshift.run()
        self.log.info("Data inserted successfully in the Fact Table {}".format(self.table))

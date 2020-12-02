from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_table_query = """
        TRUNCATE TABLE {}
        """
    
    insert_into_table_query = """
        INSERT INTO {} {}
        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 insert_dimension_table_query = "",
                 truncate_table = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_dimension_table_query = insert_dimension_table_query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info("Truncate the table {} before inserting new data".format(self.table))
            redshift.run(LoadDimensionOperator.truncate_table_query.format(self.table))
            self.log.info("Truncated the table {}, and now ready to insert the data".format(self.table))
        redshift.run(LoadDimensionOperator.insert_into_table_query.format(self.table, self.insert_dimension_table_query))
        self.log.info('LoadDimensionOperator not implemented yet')

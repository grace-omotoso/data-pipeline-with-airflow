from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                load_query="",
                truncate_records = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_query = load_query
        self.truncate_records = truncate_records

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into dimension table")
        if self.truncate_records:
            redshift.run("DELETE FROM {}".format(self.table))
        dim_sql = (f"INSERT INTO {self.table} {self.load_query}")
        redshift.run(dim_sql)
        self.log.info("Dimension table load complete")


      
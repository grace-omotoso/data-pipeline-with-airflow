from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                load_query="",
                append_records=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.load_query = load_query
        self.append_records = append_records

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into {self.table} table")
        if not self.append_records:
            redshift.run("DELETE FROM {}".format(self.table))
        fact_sql = (f"INSERT INTO {self.table} {self.load_query}")
        redshift.run(fact_sql)
        
        self.log.info('{self.table} table load completed')
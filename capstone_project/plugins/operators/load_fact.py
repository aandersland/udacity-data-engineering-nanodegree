from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 sql_statement='',
                 *args, **kwargs):
        """
        Operator loads dimension tables from prepared sql statements
        :param redshift_conn_id: Redshift connection variable
        :param aws_credentials_id: AWS credentials
        :param table: Table to write data into
        :param sql_statement: SQL statement to be executed
        :param args: Arguments from context
        :param kwargs: Keyword Arguments from context
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Aiflow operator that executes staging table data into target tables
        :param context: Airflow context information
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = self.sql_statement.format(
            self.table
        )
        self.log.info(f'Running sql statement: {formatted_sql}')
        redshift.run(formatted_sql)

        self.log.info(f'Data copied to table: {self.table}')

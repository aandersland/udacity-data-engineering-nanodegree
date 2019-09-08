from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 sql_statement='',
                 truncate_table='',
                 *args, **kwargs):
        """
        Operator loads dimension tables from prepared sql statements
        :param redshift_conn_id: Redshift connection variable
        :param aws_credentials_id: AWS credentials
        :param table: Table to write data into
        :param sql_statement: SQL statement to be executed
        :param truncate_table: Flag to truncate table
        :param args: Arguments from context
        :param kwargs: Keyword Arguments from context
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.truncate_table = truncate_table
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

        if self.truncate_table:
            self.log.info(f'Truncate table: {self.truncate_table}')
            truncate_sql = f'TRUNCATE TABLE {self.table}'
            self.log.info(f'Running sql statement: {truncate_sql}')
            redshift.run(truncate_sql)

        self.log.info(f'Running sql statement: {formatted_sql}')
        redshift.run(formatted_sql)

        self.log.info(f'Data copied to table: {self.table}')

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesInRedshiftOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 sql_statement='',
                 *args, **kwargs):
        """
        Operator performs data quality checks on tables: emtpy tables
        :param redshift_conn_id: Redshift connection variable
        :param aws_credentials_id: AWS credentials
        :param table: List of tables to perform checks on
        :param args: Arguments from context
        :param kwargs: Keyword Arguments from context
        """
        super(CreateTablesInRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Aiflow operator that executes data quality checks
        :param context: Airflow context information
        """
        checks_failed = False
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Creating table: {self.table}.')
        redshift.run(self.sql_statement)





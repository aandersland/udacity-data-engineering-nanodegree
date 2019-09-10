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
                 drop_table='',
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
        self.drop_table = drop_table
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Aiflow operator that executes data quality checks
        :param context: Airflow context information
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.drop_table:
            self.log.info(f'Drop table: {self.drop_table}')
            drop_sql = f'DROP TABLE IF EXISTS {self.table}'
            self.log.info(f'Running sql statement: {drop_sql}')
            redshift.run(drop_sql)

        self.log.info(f'Creating table: {self.table}.')
        print('sql statement', self.sql_statement)
        redshift.run(self.sql_statement)

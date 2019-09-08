from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 tables='',
                 *args, **kwargs):
        """
        Operator performs data quality checks on tables: emtpy tables
        :param redshift_conn_id: Redshift connection variable
        :param aws_credentials_id: AWS credentials
        :param tables: List of tables to perform checks on
        :param args: Arguments from context
        :param kwargs: Keyword Arguments from context
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Aiflow operator that executes data quality checks
        :param context: Airflow context information
        """
        checks_failed = False
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Getting table record counts.')

        for table in self.tables:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                checks_failed = True
                self.log.error(
                    f'Data quality check failed on table {table}, '
                    f'no records found.')
            else:
                self.log.info(
                    f'Data quality checks on table {table} '
                    f'passed with {records[0][0]} records')

        # todo add additional checks here

        if checks_failed:
            raise ValueError(
                'Data quality checks failed on 1 or more tables.')
        else:
            self.log.info('Data quality checks passed on all tables.')

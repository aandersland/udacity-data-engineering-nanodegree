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
                 years='',
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
        self.years = years
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

        for year in self.years:
            count = redshift.get_records(
                f'SELECT COUNT(t.year) '
                f'FROM f_flights f '
                f'JOIN d_flight_detail d '
                f'ON d.flight_detail_id = f.flight_detail_id '
                f'JOIN d_time t ON t.datetime = f.schdld_depart_time_id '
                f'WHERE t.year = {year} '
                )
            if len(count) < 1 or len(count[0]) < 1 or count[0][0] < 1:
                checks_failed = True
                self.log.error(
                    f'Data quality check for year {year}, no records found.'
                )
            else:
                self.log.info(
                    f'Data quality check passed for year {year} '
                    f'with {count[0][0]} records')

        if checks_failed:
            raise ValueError(
                'Data quality checks failed on 1 or more tables.')
        else:
            self.log.info('Data quality checks passed on all tables.')

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT AS '{}'
        COMPUPDATE {}
        STATUPDATE {}
        {} '{}' {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 aws_region='',
                 time_format='',
                 comp_update='',
                 stat_update='',
                 format_one='',
                 format_two='',
                 format_three='',
                 *args, **kwargs):
        """
        Initialization of an airflow operator to stage data from S3 into
        redshift
        :param redshift_conn_id: Redshift connection variable
        :param aws_credentials_id: AWS credentials
        :param table: Table to write data into
        :param s3_bucket: S3 bucket containing the data
        :param s3_key: S3 key
        :param aws_region: AWS region used
        :param time_format: Time formatting used in sql copy statement
        :param comp_update: Flag for turning on or off
        :param stat_update: Flag for turning on or off
        :param format: Format the input file for loading
        :param args: Arguments from context
        :param kwargs: Keyword Arguments from context
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region
        self.time_format = time_format
        self.comp_update = comp_update
        self.stat_update = stat_update
        self.format_one = format_one
        self.format_two = format_two
        self.format_three = format_three

    def execute(self, context):
        """
        Aiflow operator that executes staging data into redshift from S3
        :param context: Airflow context information
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clearing data from table: {self.table}')
        redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.aws_region,
            self.time_format,
            self.comp_update,
            self.stat_update,
            self.format_one,
            self.format_two,
            self.format_three
        )
        redshift.run(formatted_sql)
        self.log.info(f'Data copied to table: {self.table}')

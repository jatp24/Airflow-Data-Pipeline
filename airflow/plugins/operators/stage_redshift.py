from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    log_template = '''
    COPY {} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    format as json '{}'
    STATUPDATE ON;
    
    '''
    
    songs_template = ''' 
    COPY {} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    json 'auto'
    STATUPDATE ON;
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        logging.info('Setting up Staging to RedShift')

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        if len(path)>0:
            self.path = path
        else:
            self.path = ''


    def execute(self, context):
        logging.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logging.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
                    

        logging.info("Copying data from S3 to Redshift")
        if len(self.path) >0:
            
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            formatted_sql = StageToRedshiftOperator.log_template.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.path)
            redshift.run(formatted_sql)

           
        else:
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
            formatted_sql = StageToRedshiftOperator.songs_template.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            redshift.run(formatted_sql)
            
        logging.info("StageToRedshiftOperator has finished running.")

            
            
         
          







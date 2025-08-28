import os
import boto3
import smtplib
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from praw import Reddit

load_dotenv()

default_args = {
    'owner': 'Denzel Kinyua',
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 8, 27)
}

@dag(dag_id='reddit_dag_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False)
def reddit_dag_pipeline():
    '''
    start task, prints the job's started
    '''
    @task
    def start_task():
        print('Job starting now...')

    '''
    Connect to a Reddit instance, imported from praw
    Initialize a subreddit instance from reddit
    Return a list of columns depending on a daily limit, 100
    Return the extracted data for transformations as a list of dictionaries
    '''
    @task
    def extract_data(limit):
        data = []
        try:
            reddit = Reddit(
                client_id=os.getenv("CLIENT_ID"),
                client_secret=os.getenv("SECRET_KEY"),
                user_agent='Reddit pipeline by u/user'
            )

            subreddit = reddit.subreddit("dataengineering")
            for s in subreddit.new(limit=limit):
                data.append({
                    'id': s.id,
                    'title': s.title,
                    'text': s.selftext,
                    'score': s.score,
                    'author': s.author.name if s.author else None,
                    'author_karma': s.author.link_karma if s.author else None,
                    'url': s.url,
                    'created_at': s.created_utc
                })
            return data
        
        except Exception as e:
            print(f'Error extracting data from subreddit: {e}')
            raise

    '''
    Transform data
    Transform the created_at column into a datetime instance
    Load into csv file
    '''
    @task
    def transform_data(data):
        today = datetime.now().strftime('%Y-%m-%d')
        file_path = f'/opt/airflow/files/{today}_reddit_data.csv'
        try:
            df = pd.DataFrame(data)
            df['created_at'] = pd.to_datetime(df['created_at'].astype(float), unit='s', origin='unix')
            df.to_csv(file_path, index=False)
            return file_path
        except Exception as e:
            print(f'Error transforming data: {e}')
            raise

    '''
    Get csv file
    Initialize s3 instance
    Load into aws s3 bucket
    '''
    @task
    def load_to_s3(file, bucket):
        today = datetime.now().strftime('%Y-%m-%d')
        key = f'files/{today}_reddit_data.csv'
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                region_name='eu-north-1'
            )
            s3.upload_file(file, bucket, key)
            return f'{key} uploaded to S3 successfully!'
        except Exception as e:
            print(f'Error loading file into AWS S3 bucket: {e}')
            raise

    '''
    Alert user that dag run is complete and should check the pipeline
    '''
    @task
    def send_email(message):
        try:
            subject = 'DAG Run Complete'
            body = f"""
                    DAG Run complete: {message}
                    Please go to the Airflow UI to check the pipeline out
                """
            sender = os.getenv("SENDER")
            receiver = os.getenv("RECIPIENT") 
            password = os.getenv("EMAIL_PWD")

            msg = MIMEMultipart()
            msg["From"] = sender
            msg["To"] = receiver
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            # SSL connection (port 465)
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(sender, password)
                server.sendmail(sender, receiver, msg.as_string())

            print("Email sent successfully!")
        except Exception as e:
            print(f"Failed to send email: {e}")
            raise

    start = start_task()
    data = extract_data(100)
    file = transform_data(data)
    message = load_to_s3(file, 'reddit-data-bucket-csv')
    email = send_email(message)

    start >> data >> file >> message >> email

reddit_dag = reddit_dag_pipeline()








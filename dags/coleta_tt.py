
import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import tweepy
import re
import pandas as pd
import numpy as np
from datetime import datetime
import json
import glob2
import pymongo

url = "mongodb://127.0.0.1:27017/"
con = pymongo.MongoClient(url)

db = con.twitterdb
collection = db.tweets_famosos


path = '/usr/local/airflow/data/raw'
allFiles = glob2.glob(path + "/*.csv")

default_args = {
  'owner': 'airflow',
}



@dag(
  default_args=default_args,
  description="Desafio",
  schedule_interval=None,
  start_date=days_ago(2)
)
def desafio_ind():

  @task()
  def con_twitter():
    consumer_key = 'yhJM02AXVkeJKbZ2UY9MZx2Pk'
    consumer_secret = 'bLNfYOuu0HJsca70Em3TQxoOAdudra0jsZI8kf1Rb7L74AxAjZ'
    access_token = '164492542-o5pBhnoeyeNr4lsW1soVsGAWA6h8qH7k9E2j4I9t'
    access_token_secret = 'S5nXv7sJZaC348zfHEzTcNK4Q1BAOUsOi6KLBH6YlTM0m'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    return api

  @task()
  def get_data():
      usuario =['railanderReis','RapMais']
      #usuario =['elonmusk','BillGates']
      for i in usuario:
          try:     
              tweets = tweepy.Cursor(api.user_timeline,id=i).items(30)
              
              tweets_list = [[tweet.user.screen_name, tweet.text,tweet.created_at] for tweet in tweets]
                      
              tweets_df = pd.DataFrame(tweets_list)
              tweets_df = tweets_df.rename(columns={0: 'User', 1: 'Tweets', 2: 'Post_date'})

              tweets_df.to_csv(path+i+".csv", index = False, header=True)

          except BaseException as e:
              print('failed on_status,',str(e))
              time.sleep(3)


  @task()
  def read_data():
      list_ = []
      for file_ in allFiles:
          dfs = pd.read_csv(file_,index_col=None,header=0)
          list_.append(dfs)
          tf_data = pd.concat(list_,axis=0, ignore_index=True)

          return tf_data()

  @task()
  def transform_data():
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'https?:\/\/.*[\r\n]*', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'#\S+', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'@\S+', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'\$\w*', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'\n', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'\""', value='', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].replace(to_replace=r'  ', value=' ', regex=True)
    tf_data['Tweets'] = tf_data['Tweets'].str.replace('[^\w\s#@/:%.,_-]', '', flags=re.UNICODE)
    tf_data['Tweets'] = tf_data['Tweets'].str.upper().str.lstrip().str.strip()

                    
    tf_data['Post_date'] = pd.to_datetime(tf_data['Post_date'], format='%d-%m-Y',infer_datetime_format=True) 
    tf_data['Post_date'] = tf_data['Post_date'].dt.strftime('%Y-%m-%d')

    tf_data.insert(3, "Upload_date",datetime.today().strftime('%Y-%m-%d'), allow_duplicates=False)    

    return tf_data

  @task()
  def upload_data():
    ttf_data.reset_index(inplace=True)
    data_dict = tf_data.to_dict("records")
    db.tweets_famosos.insert_many(data_dict)  

  con_tt = con_twitter()
  data = get_data(con_tt)
  task_read_dt = read_data(data)
  task_tf_data = transform_data(task_read_dt)
  task_up_dt = upload_data(task_tf_data)


coleta_tw = desafio_ind()  
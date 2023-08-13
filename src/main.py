from reddit import RedditLoader
from storage import GoogleStorageLoader
from data_warehouse import BigQueryTable
from datetime import date, timedelta
import pandas as pd
import fastparquet as fp

# make time limits - we need only yesterday's posts
START_DATE = date.today() - timedelta(days = 1)
FILENAME = f'reddit-comments-{START_DATE}.parquet'
FILEPATH = f'../content/{FILENAME}'
BUCKET_NAME = "reddit-api"
DATASET_NAME='reddit_api_raw_data'
TABLE_NAME='raw_data'

# get subreddits name from a local txt file
with open('../content/subreddits.txt') as f:
    content = f.read()
    subreddit_names = content.split('\n')
            
# prepare list for a future dataframe
#data = []
RedditClient = RedditLoader()

for i, subreddit_name in enumerate(subreddit_names):
    data = RedditClient.collect_comments(subreddit_name=subreddit_name, start_date=START_DATE)

    # create new parquet file if not exists or append to existing
    if i == 0:
        fp.write(FILEPATH, pd.DataFrame(data), compression = 'GZIP')
    else:
        fp.write(FILEPATH, pd.DataFrame(data), compression = 'GZIP', append=True)
        
    print(f'Subreddit {subreddit_name} is done!')
        
#full_data = pd.DataFrame(data)
# save to parquet file in a format 'reddit-comments-YYYY-MM-DD'
#full_data.to_parquet('../content/' + FILENAME)

# upload to the storage
GoogleStorage = GoogleStorageLoader()
GoogleStorage.upload_to_bucket(bucket_name=BUCKET_NAME,
                               destination=FILENAME,
                               source_file=FILEPATH)

# upload from storage parquet file to the BigQuery table
BigQueryTableLoader = BigQueryTable(dataset_name=DATASET_NAME, table_name=TABLE_NAME)
# check if table exists
BigQueryTableLoader.create_table_if_not_exists(config_path='../schemas.yml')
# load file from cloud storage
BigQueryTableLoader.load_from_cloud_storage(uri=f'gs://{BUCKET_NAME}/{FILENAME}')
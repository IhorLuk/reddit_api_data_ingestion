from reddit import RedditLoader
from storage import GoogleStorageLoader
from datetime import date, timedelta
import pandas as pd

# make time limits - we need only yesterday's posts
START_DATE = date.today() - timedelta(days = 1)
END_DATE = date.today()
FILENAME = f'reddit-comments-{START_DATE}.parquet'
BUCKET_NAME = "reddit-api"
DESTINATION = "reddit-comments-2023-08-11.parquet"

# get subreddits name from a local txt file
with open('../content/subreddits.txt') as f:
    content = f.read()
    subreddit_names = content.split('\n')
            
# prepare list for a future dataframe
data = []
RedditClient = RedditLoader()

for subreddit_name in subreddit_names:
    data.extend(RedditClient.collect_comments(subreddit_name=subreddit_name, start_date=START_DATE))
  
full_data = pd.DataFrame(data)
# save to parquet file in a format 'reddit-comments-YYYY-MM-DD'
full_data.to_parquet('../content/' + FILENAME)

# upload to the storage
GoogleStorage = GoogleStorageLoader()
GoogleStorage.upload_to_bucket(bucket_name=BUCKET_NAME,
                               destination=DESTINATION,
                               source_file='../content/' + FILENAME)
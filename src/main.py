from praw import Reddit
from praw.models import Subreddit, Submission
import os
from dotenv import load_dotenv
from reddit import RedditLoader
from datetime import datetime, date, timedelta
import pandas as pd

# make time limits - we need only yesterday's posts
START_DATE = date.today() - timedelta(days = 1)
END_DATE = date.today()

# get subreddits name from a local txt file
with open('../content/subreddits.txt') as f:
    content = f.read()
    subreddit_names = content.split('\n')
            
# prepare list for a future dataframe
data = []
RedditLoader = RedditLoader()

for subreddit_name in subreddit_names:
    data.extend(RedditLoader.collect_comments(subreddit_name=subreddit_name, start_date=START_DATE))
  
full_data = pd.DataFrame(data)
# save to parquet file in a format 'reddit-comments-YYYY-MM-DD'
full_data.to_parquet(f'reddit-comments-{START_DATE}')
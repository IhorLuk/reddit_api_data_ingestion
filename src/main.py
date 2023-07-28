from praw import Reddit
from praw.models import Subreddit, Submission
import os
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
import pandas as pd

# connect to reddit API service
load_dotenv()
reddit = Reddit(client_id=os.getenv('CLIENT_ID'),
                client_secret=os.getenv('CLIENT_SECRET'),
                redirect_url='http://localhost:a8080',
                user_agent=os.getenv('USER_AGENT'))

# make time limits - we need only yesterday's posts
START_DATE = date.today() - timedelta(days = 1)
END_DATE = date.today()

# get subreddits name from a local txt file
with open('../content/subreddits.txt') as f:
    content = f.read()
    subreddit_names = content.split('\n')

# prepare list for a future dataframe
data = []

# loop on getting all the data
for subreddit_name in subreddit_names:
    subreddit = Subreddit(reddit=reddit, display_name=subreddit_name)
    
    for submission_id in subreddit.new(limit=100):
        submission = Submission(reddit=reddit, id=submission_id)
        
        # we don't want to see data for stickied (or pinned) submissions
        if not submission.stickied:
            # convert timestamp to a datetime format
            created_time = datetime.fromtimestamp(int(submission.created_utc))
            # check if submission was created yesterday
            if created_time.date() == START_DATE:
                
                submission.comments.replace_more(limit=None)
                comment_queue = submission.comments[:]  # Seed with top-level
                while comment_queue:
                    comment = comment_queue.pop(0)
                    
                    # not all comments have available author name
                    try:
                        author_name = comment.author.name
                    except AttributeError:
                        author_name = None
                        
                    # prepare row for dataframe
                    row = {
                        'comment_id': comment.id,
                        'comment_author': author_name,
                        'comment_created_time': datetime.fromtimestamp(int(comment.created_utc)),
                        'commentnum_upvotes': comment.score,
                        'comment_text': comment.body,
                        'submission_id': submission.id,
                        'submission_author': submission.author.name,
                        'submission_title': submission.title,
                        'submission_created_time': created_time,
                        'submission_name': submission.name,
                        'submission_num_comments': submission.num_comments,
                        'submission_num_upvotes': submission.score,
                        'submission_text': submission.selftext,
                        'submission_url': submission.url,
                        'subreddit_full_name': subreddit.fullname,
                        'subreddit_display_name': subreddit.display_name,
                        'subreddit_title': subreddit.title,
                        'subreddit_public_description': subreddit.public_description,
                        'subreddit_created_utc': datetime.fromtimestamp(int(subreddit.created_utc)),
                        'subreddit_subscribers': subreddit.subscribers,
                        'subreddit_lang': subreddit.lang,
                        'subreddit_subreddit_type': subreddit.lang,
                        'subreddit_over18': subreddit.over18
                        }
                    
                    data.append(row)
                    comment_queue.extend(comment.replies)
  
full_data = pd.DataFrame(data)
# save to parquet file in a format 'reddit-comments-YYYY-MM-DD'
full_data.to_parquet(f'reddit-comments-{START_DATE}')
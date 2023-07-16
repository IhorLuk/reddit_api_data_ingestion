from praw import Reddit
from praw.models import Subreddit
from dotenv import dotenv_values

config = dotenv_values(".env")

reddit = Reddit(client_id=config['CLIENT_ID'],
                client_secret=config['CLIENT_SECRET'],
                redirect_url='http://localhost:a8080',
                user_agent=config['USER_AGENT'])

with open('../content/subreddits.txt') as f:
    content = f.read()
    subreddit_names = content.split('\n')
    
data = []

for subreddit_name in subreddit_names:
    subreddit = Subreddit(reddit=reddit, display_name=subreddit_name)
    
    subreddit_dict = {'full_name': subreddit.fullname,
                      'display_name': subreddit.display_name,
                      'title': subreddit.title,
                      'public_description': subreddit.public_description,
                      'created_utc': int(subreddit.created_utc),
                      'subscribers': subreddit.subscribers,
                      'lang': subreddit.lang,
                      'subreddit_type': subreddit.lang,
                      'over18': subreddit.over18}
    
    data.append(subreddit_dict)
  
# posts = reddit.subreddit(subredit_name).hot(limit=10)

# title = {subredit_name + '_' + str(i+1): post.title for i, post in enumerate(posts)}


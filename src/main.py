import praw
from dotenv import dotenv_values

config = dotenv_values(".env")

reddit = praw.Reddit(client_id=config['CLIENT_ID'],
                     client_secret=config['CLIENT_SECRET'],
                     redirect_url='http://localhost:a8080',
                     user_agent=config['USER_AGENT'])

subredit_name = 'Switzerland'
posts = reddit.subreddit(subredit_name).hot(limit=10)

title = {}
for i, posts in enumerate(posts):
    title[subredit_name + '_' + str(i+1)] = posts.title
    
print(title)



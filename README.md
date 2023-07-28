# Reddit API - data ingestion using Python praw library

1. Create python script to get data we need from Reddit using API and put it in some object.
Add interested subreddits in txt file - `subreddits.txt`. Currently it takes all comments to all submissions and stores it in a local parquet file - `reddit-comments-YYYY-MM-DD` for previos day. Credentials to your Reddit API should be provided.
2. Loading parquet to and object storage (in progress).
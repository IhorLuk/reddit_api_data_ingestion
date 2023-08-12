from praw import Reddit
from praw.models import Subreddit, Submission
import os
from dotenv import load_dotenv
from datetime import datetime, date, timedelta

class RedditLoader():
    def __init__(self) -> None:
        """Loads credentials from environment and starts reddit client
        """
        # connect to reddit API service
        load_dotenv()
        self.reddit = Reddit(client_id=os.getenv('CLIENT_ID'),
                        client_secret=os.getenv('CLIENT_SECRET'),
                        redirect_url='http://localhost:a8080',
                        user_agent=os.getenv('USER_AGENT'))
        

            
    def get_subreddit(self, subreddit_name):
        """Creates subreddit instance to get submissions from it

        Args:
            subreddit_name (str): name of subreddit
        """
        self.subreddit = Subreddit(reddit=self.reddit, display_name=subreddit_name)
        
    def get_submission(self, submission_id):
        """Creates a submission instance, used for getting comments.

        Args:
            submission_id (str): the id of submission
        """
        self.submission = Submission(reddit=self.reddit, id=submission_id)
        
    def get_comments(self, start_date):
        """Collects all comments for a submission and returns list of dicts

        Args:
            start_date (date): date to collect comments for

        Returns:
            list: list of dicts, where each dict is a row for a comments of submission
        """
        submission_data = []
        if not self.submission.stickied:
            # convert timestamp to a datetime format
            created_time = datetime.fromtimestamp(int(self.submission.created_utc))
            # check if submission was created yesterday
            if created_time.date() == start_date:
                
                self.submission.comments.replace_more(limit=None)
                comment_queue = self.submission.comments[:]  # Seed with top-level
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
                        'submission_id': self.submission.id,
                        'submission_author': self.submission.author.name,
                        'submission_title': self.submission.title,
                        'submission_created_time': created_time,
                        'submission_name': self.submission.name,
                        'submission_num_comments': self.submission.num_comments,
                        'submission_num_upvotes': self.submission.score,
                        'submission_text': self.submission.selftext,
                        'submission_url': self.submission.url,
                        'subreddit_full_name': self.subreddit.fullname,
                        'subreddit_display_name': self.subreddit.display_name,
                        'subreddit_title': self.subreddit.title,
                        'subreddit_public_description': self.subreddit.public_description,
                        'subreddit_created_utc': datetime.fromtimestamp(int(self.subreddit.created_utc)),
                        'subreddit_subscribers': self.subreddit.subscribers,
                        'subreddit_lang': self.subreddit.lang,
                        'subreddit_subreddit_type': self.subreddit.lang,
                        'subreddit_over18': self.subreddit.over18
                        }
                    submission_data.append(row)
                    comment_queue.extend(comment.replies)
        return submission_data
                    
    def collect_comments(self, subreddit_name, start_date):
        """The main function, that collections all comments for selected date (ideally it should be yesterday) and returns list of dicts.

        Args:
            subreddit_name (str): the name of subreddit.
            start_date (date): for which date to collect comments

        Returns:
            list: list of dictionaries, each dictionary is a row of data. Could be used to create a dataframe.
        """
        subreddit_data = []
        self.get_subreddit(subreddit_name)
        
        for submission_id in self.subreddit.new(limit=100):
            self.get_submission(submission_id)
            submission_time = datetime.fromtimestamp(int(self.submission.created_utc))
            if submission_time.date() < start_date:
                break
            
            new_data = self.get_comments(start_date)
            
            if new_data is not None:
                subreddit_data.extend(new_data)
            
        return subreddit_data
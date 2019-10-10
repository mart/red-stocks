import os
from datetime import datetime
from time import sleep
import praw
import logging

from analyze import sentiment
from analyze import tickers
from db.models import add_posts, add_comments, Post, Comment
from scrape import reddit
from scrape import scores

# TODO move to db config table, with row for each config
config = {'subreddits': ["investing", "RobinHood", "wallstreetbets",
                         "options", "stocks", "weedstocks", "TheCannalysts",
                         "SecurityAnalysis", "StockMarket", "InvestmentClub",
                         "Stock_Picks", "ValueInvesting", "CanadianInvestor",
                         "UKInvesting", "pennystocks", "M1Finance"],
          'earliest_content': 1483228818,
          'update_frequency': 86400,
          'update_buffer': 240,
          'min_loop_seconds': 240,
          'client_id': os.environ['SCRIPT_ID'],
          'client_secret': os.environ['SECRET'],
          'user_agent': os.environ['APPNAME'],
          'username': os.environ['USERNAME'],
          'password': os.environ['PASSWORD']}
log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ['LOG_LEVEL'])


def start_reddit(configuration):
    """Initializes the PRAW object

    Returns
    -------
    Reddit
        A reddit object connected using the reddit API

    """
    return praw.Reddit(client_id=configuration['client_id'],
                       client_secret=configuration['client_secret'],
                       user_agent=configuration['user_agent'],
                       username=configuration['username'],
                       password=configuration['password'])


class Worker:
    """A worker spawned by the worker process; controls data scraping, processing, storage

    Attributes are either configuration or data attributes
    Methods control data scraping, processing, and storage
    """

    def __init__(self, configuration):
        # config
        self.subreddits = configuration['subreddits']
        self.earliest_content = configuration['earliest_content']
        self.update_frequency = configuration['update_frequency']
        self.update_buffer = configuration['update_buffer']
        self.min_loop_seconds = configuration['min_loop_seconds']
        self.start_day = datetime.today().date()
        self.loop_start = int(datetime.today().timestamp())
        self.reddit = start_reddit(configuration)
        # data
        self.comments = None
        self.posts = None
        self.comment_scores = None
        self.post_scores = None
        self.post_labels = None
        self.comment_labels = None
        self.ticker_posts = None
        self.ticker_comments = None
        self.post_sentiment = None
        self.comment_sentiment = None

    def scrape_content(self):
        self.comments = reddit.scrape_content(self.subreddits, Comment, self.earliest_content)
        self.posts = reddit.scrape_content(self.subreddits, Post, self.earliest_content)
        return self.comments or self.posts

    def content_to_db(self):
        add_comments(self.comments)
        self.comments = None
        add_posts(self.posts)
        self.posts = None

    def scrape_scores(self):
        self.comment_scores = scores.scrape_update(self.reddit, Comment, self.update_frequency, self.update_buffer)
        self.post_scores = scores.scrape_update(self.reddit, Post, self.update_frequency, self.update_buffer)
        return any(self.comment_scores) or any(self.post_scores)

    def scores_to_db(self):
        scores.update_content(Comment, self.comment_scores[0], self.comment_scores[1])
        self.comment_scores = None
        scores.update_content(Post, self.post_scores[0], self.post_scores[1])
        self.post_scores = None

    def label_content(self):
        self.post_labels = tickers.label_content(Post)
        self.comment_labels = tickers.label_content(Comment)
        count = 0 if self.post_labels is None else len(self.post_labels)
        count += 0 if self.comment_labels is None else len(self.comment_labels)
        log.info("Labeled " + str(count) + " content items")
        return self.comment_labels or self.post_labels

    def labels_to_db(self):
        tickers.write_content_labels(Post, self.post_labels)
        self.post_labels = None
        tickers.write_content_labels(Comment, self.comment_labels)
        self.comment_labels = None

    def label_tickers(self):
        self.ticker_comments = tickers.label_tickers(Comment)
        self.ticker_posts = tickers.label_tickers(Post)
        count = 0 if self.ticker_comments is None else len(self.ticker_comments[1])
        count += 0 if self.ticker_posts is None else len(self.ticker_posts[1])
        log.info("Labeled " + str(count) + " ticker items")
        return any(self.ticker_comments) or any(self.ticker_comments)

    def tickers_to_db(self):
        tickers.write_ticker_labels(Comment, self.ticker_comments[0], self.ticker_comments[1])
        self.ticker_comments = None
        tickers.write_ticker_labels(Post, self.ticker_posts[0], self.ticker_posts[1])
        self.ticker_posts = None

    def sentiment_analysis(self):
        self.post_sentiment = sentiment.sentiment(Post)
        self.comment_sentiment = sentiment.sentiment(Comment)
        count = 0 if self.post_sentiment is None else len(self.post_sentiment)
        count += 0 if self.comment_sentiment is None else len(self.comment_sentiment)
        log.info("Analyzed " + str(count) + " content items")
        return self.post_sentiment or self.comment_sentiment

    def sentiment_to_db(self):
        sentiment.write_sentiment(Post, self.post_sentiment)
        self.post_sentiment = None
        sentiment.write_sentiment(Comment, self.comment_sentiment)
        self.comment_sentiment = None

    def delay(self):
        now = int(datetime.today().timestamp())
        if now - self.loop_start < self.min_loop_seconds:
            log.info("Delaying "
                     + str(self.min_loop_seconds - (now - self.loop_start))
                     + " seconds")
            sleep(self.min_loop_seconds - (now - self.loop_start))
        self.loop_start = int(datetime.today().timestamp())

    def tickers(self):
        if self.start_day != datetime.today().date():
            log.info("Daily ticker update ")
            tickers.update_tickers()
            self.start_day = datetime.today().date()


if __name__ == "__main__":
    worker = Worker(config)
    log.info("Startup ticker update ")
    tickers.update_tickers()
    while True:
        while worker.scrape_content():
            worker.content_to_db()

        while worker.scrape_scores():
            worker.scores_to_db()

        while worker.label_content():
            worker.labels_to_db()

        while worker.label_tickers():
            worker.tickers_to_db()

        while worker.sentiment_analysis():
            worker.sentiment_to_db()

        worker.tickers()
        worker.delay()  # enforces minimum loop time

import os

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column, Integer, String, Boolean, ARRAY, Float
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine(os.environ['DATABASE_URL'], use_batch_mode=True)
Session = sessionmaker(bind=engine)


class Comment(Base):
    __tablename__ = 'comments'

    body = Column(String)
    author = Column(String)
    author_flair_text = Column(String)
    created_utc = Column(Integer)
    subreddit_id = Column(String)
    link_id = Column(String)
    parent_id = Column(String)
    score = Column(String)
    retrieved_on = Column(Integer)
    gilded = Column(String)
    id = Column(String, primary_key=True)
    subreddit = Column(String)
    author_flair_css_class = Column(String)
    update_age = Column(Integer)
    deleted = Column(Boolean)
    processed = Column(Boolean)
    labels = Column(ARRAY(String))
    parent_labels = Column(ARRAY(String))
    sentiment = Column(Float)


class Post(Base):
    __tablename__ = 'posts'

    created_utc = Column(Integer)
    subreddit = Column(String)
    author = Column(String)
    domain = Column(String)
    url = Column(String)
    num_comments = Column(String)
    score = Column(String)
    title = Column(String)
    selftext = Column(String)
    id = Column(String, primary_key=True)
    gilded = Column(String)
    stickied = Column(String)
    retrieved_on = Column(Integer)
    over_18 = Column(String)
    thumbnail = Column(String)
    subreddit_id = Column(String)
    author_flair_css_class = Column(String)
    is_self = Column(String)
    permalink = Column(String)
    author_flair_text = Column(String)
    update_age = Column(Integer)
    deleted = Column(Boolean)
    labels = Column(ARRAY(String))
    processed = Column(Boolean)
    sentiment = Column(Float)


class Ticker(Base):
    __tablename__ = 'tickers'

    symbol = Column(String, primary_key=True)
    name = Column(String)
    content_ids = Column(ARRAY(String))
    price_data = Column(JSONB)
    last_update = Column(Integer)


def add_posts(posts):
    session = Session()
    session.add_all([
        Post(
            created_utc=post.created_utc,
            subreddit=post.subreddit,
            author=post.author,
            domain=post.domain,
            url=post.url,
            num_comments=post.num_comments,
            score=post.score,
            title=post.title,
            selftext=getattr(post, 'selftext', ''),
            id=post.id,
            gilded=sum(getattr(post, 'gildings', {}).values()),
            stickied=post.stickied,
            retrieved_on=post.retrieved_on,
            over_18=post.over_18,
            thumbnail=post.thumbnail,
            subreddit_id=post.subreddit_id,
            author_flair_css_class=post.author_flair_css_class,
            is_self=post.is_self,
            permalink=post.permalink,
            author_flair_text=post.author_flair_text,
            update_age=(post.retrieved_on - post.created_utc),
            deleted=False,
            processed=False)
        for post in posts])
    session.commit()


def add_comments(comments):
    session = Session()
    session.add_all([
        Comment(
            body=comment.body,
            author=comment.author,
            author_flair_text=comment.author_flair_text,
            created_utc=comment.created_utc,
            subreddit_id=comment.subreddit_id,
            link_id=comment.link_id,
            parent_id=comment.parent_id,
            score=comment.score,
            retrieved_on=getattr(comment, 'retrieved_on', comment.created_utc),
            gilded=sum(getattr(comment, 'gildings', {}).values()),
            id=comment.id,
            subreddit=comment.subreddit,
            author_flair_css_class=comment.author_flair_css_class,
            update_age=(getattr(comment, 'retrieved_on', comment.created_utc) - comment.created_utc),
            deleted=False,
            processed=False)
        for comment in comments])
    session.commit()

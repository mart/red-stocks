import os

import psycopg2
from psycopg2 import extras
from psycopg2 import sql
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

"""
Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
"""


def analyze_sentiment(content, analyzer):
    """Produce a sentiment score using the VADER model

    Returns
    -------
    dict(str: float)
        mapping of content id to sentiment score

    """
    output = {}
    for id, text in content.items():
        output[id] = analyzer.polarity_scores(text)['compound']
    return output


def connect():
    """Connect to database

    """
    return psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


def should_analayze(db, table):
    """Determines if there is content in the database that needs analysis

    Returns
    -------
    bool
        True if at least one item is unprocessed in the table

    """
    query = sql.SQL("SELECT id FROM {}"
                    " WHERE sentiment is NULL"
                    " LIMIT 1").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query)
    output = cur.fetchone()
    cur.close()
    return output is not None


def read_posts(db):
    """Queries a batch of reddit posts from the database with no sentiment analysis

    Returns
    -------
    dict(str: str)
        A dictionary with ID as keys and text content as values

    """
    query = sql.SQL("SELECT id, title, selftext FROM posts"
                    " WHERE sentiment is NULL"
                    " LIMIT 10000")
    cur = db.cursor()
    cur.execute(query)
    output = {content[0]: content[1] + " " + content[2] for content in cur.fetchall()}
    cur.close()
    return output


def read_comments(db):
    """Queries a batch of reddit comments from the database with no sentiment analysis

    Returns
    -------
    dict(str: str)
        A dictionary with ID as keys and text content as values
    """
    query = sql.SQL("SELECT id, body FROM comments"
                    " WHERE sentiment is NULL"
                    " LIMIT 10000")
    cur = db.cursor()
    cur.execute(query)
    output = {content[0]: content[1] for content in cur.fetchall()}
    cur.close()
    return output


def write_batch(db, table, content):
    """Add content sentiment to the database

    """
    update = [(score, id) for id, score in content.items()]
    query = sql.SQL("UPDATE {} SET sentiment=%s WHERE id=%s").format(sql.Identifier(table))
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, update)
    db.commit()
    cur.close()


def sentiment():
    """Sentiment analysis of reddit content, update database with sentiment scores

    """
    db = connect()
    analyzer = SentimentIntensityAnalyzer()
    while should_analayze(db, 'posts'):
        post_batch = analyze_sentiment(read_posts(db), analyzer)
        write_batch(db, 'posts', post_batch)
    while should_analayze(db, 'comments'):
        post_batch = analyze_sentiment(read_comments(db), analyzer)
        write_batch(db, 'comments', post_batch)

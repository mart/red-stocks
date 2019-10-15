from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
"""
Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
"""
from datetime import datetime
from db.models import Session, Ticker, Post, Comment
import pandas as pd
import logging
from os import environ

MAX_BATCH = 20000
log = logging.getLogger(__name__)
logging.basicConfig(level=environ['LOG_LEVEL'])


def write_sentiment(table, update):
    """Add content sentiment to the database using bulk update

    """
    session = Session()
    session.bulk_update_mappings(table, update)
    session.commit()


def sentiment(table):
    """Sentiment analysis of reddit content

    Returns
    -------
    list(dict(str: float))
        A list of dictionaries with id and sentiment keys, values
    """
    session = Session()
    analyzer = SentimentIntensityAnalyzer()

    if session.query(table.id).filter(table.sentiment.is_(None)).first() is not None:  # check for item w/o sentiment
        if table.__tablename__ == 'posts':
            query = session.query(table.id, table.title, table.selftext). \
                filter(table.sentiment.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.title + " " + item.selftext for item in query.all()}
        else:
            query = session.query(table.id, table.body). \
                filter(table.sentiment.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.body for item in query.all()}

        output = []
        for id, text in content.items():
            output.append({'id': id, 'sentiment': analyzer.polarity_scores(text)['compound']})
        return output
    return []


def add_sentiment_to_symbol_data(tickers=None):
    session = Session()
    if tickers is None:
        tickers = {row.symbol for row in session.query(Ticker.symbol).all()}

    for symbol in tickers:
        ticker = session.query(Ticker).filter(Ticker.symbol == symbol).first()
        if ticker.price_data is not None:
            update_symbol_data(symbol, ticker, session)


def update_symbol_data(symbol, ticker, session):
    log.info("Updating sentiment data for " + symbol)
    df = pd.DataFrame(ticker.price_data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True, drop=False)
    df['positive_count'] = 0
    df['negative_count'] = 0
    df['sentiment_sum'] = 0
    df['scaled_sentiment'] = 0

    sentiment_data = get_sentiment_data(symbol, session)
    nonzero_content = [item for item in sentiment_data if item[0] != 0]

    for content in nonzero_content:
        score = content[0]
        create_date = datetime.fromtimestamp(content[1])
        closest_after = df[create_date:].first_valid_index()
        if closest_after is not None:  # ignore content with no future index
            if score < 0:
                df.loc[closest_after, "negative_count"] += 1
            elif score > 0:
                df.loc[closest_after, "positive_count"] += 1
            df.loc[closest_after, "sentiment_sum"] += score

    for index, row in df.iterrows():
        num_scores = row["negative_count"] + row["positive_count"]
        if num_scores > 0:
            df.loc[index, "scaled_sentiment"] = row["sentiment_sum"] / num_scores

    df["date"] = df["date"].astype(str)  # so that we can serialize as json
    ticker.price_data = df.to_dict('records')
    session.commit()


def get_sentiment_data(symbol, session):
    """Obtains sentiment data from the database

    Returns
    -------
    list(tuple(float, int))
        A list of tuples representing sentiment data with:
        A float sentiment score
        An int representing the epoch time the comment or post was created
    """
    content_ids = session.query(Ticker.content_ids) \
        .filter(Ticker.symbol == symbol).scalar()
    sentiment_data = []
    if content_ids is not None:
        posts = [item[3:] for item in content_ids if item[0:3] == "t3_"]
        comments = [item[3:] for item in content_ids if item[0:3] == "t1_"]
        sentiment_data = session.query(Post.sentiment, Post.created_utc) \
            .filter(Post.id.in_(posts)).all()
        sentiment_data += session.query(Comment.sentiment, Comment.created_utc) \
            .filter(Comment.id.in_(comments)).all()
    return sentiment_data

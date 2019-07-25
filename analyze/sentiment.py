from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
"""
Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for
Sentiment Analysis of Social Media Text. Eighth International Conference on
Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
"""
from db.models import Session

MAX_BATCH = 20000


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
                filter(table.labels.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.title + " " + item.selftext for item in query.all()}
        else:
            query = session.query(table.id, table.body). \
                filter(table.labels.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.body for item in query.all()}

        output = []
        for id, text in content.items():
            output.append({'id': id, 'sentiment': analyzer.polarity_scores(text)['compound']})
        return output

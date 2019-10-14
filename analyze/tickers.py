import csv
import os.path
import re
from ftplib import FTP
from datetime import datetime

import psycopg2
from psycopg2 import extras
from psycopg2 import sql

from db.models import Ticker, Session

MARKETS = ['nasdaq', 'other']
TOO_MANY_LABELS = 5
POST_PREFIX = 't3_'
COMMENT_PREFIX = 't1_'
MAX_BATCH = 20000
# Use this to mark a processed set with no tickers found
UNKNOWN_TICKER_STRING = 'UNKNOWN'


def download_tickers():
    """Downloads a csv stock tickers and companies from the NASDAQ website and makes a map

    Returns
    -------
    dict(str: str)
        A map of stock ticker to company name

    """
    csv.register_dialect("pipe", delimiter='|', quoting=csv.QUOTE_NONE)
    today = datetime.today().strftime('%Y%m%d')
    if not os.path.exists(today + MARKETS[0] + '.text'):
        for market in MARKETS:
            ftp = FTP("ftp.nasdaqtrader.com")
            ftp.login("anonymous", "anonymous@anonymous.com")
            ftp.cwd("/symboldirectory")
            with open(today + market + '.txt', 'wb') as file:
                ftp.retrbinary("RETR " + "{}listed.txt".format(market), file.write)
    tickers = {}
    for market in MARKETS:
        with open(today + market + '.txt', 'r', encoding='utf-8') as file:
            reader = csv.reader(file, dialect="pipe")
            next(reader)
            for row in reader:
                tickers[row[0]] = row[1].replace(",", "")
    return tickers


def update_tickers():
    """Downloads ticker lists and adds new tickers to the database

    """
    session = Session()
    new_tickers = download_tickers()
    stored_tickers = set([row.symbol for row in session.query(Ticker.symbol).all()])
    update = []
    for symbol in set(new_tickers.keys()).difference(stored_tickers):
        update.append({'symbol': symbol, 'name': new_tickers[symbol]})
    session.bulk_update_mappings(Ticker, update)


def write_content_labels(table, content):
    """Updates database content with given labels

    """
    session = Session()
    update = [{'id': id, 'labels': list(labels)} for id, labels in content.items()]
    session.bulk_update_mappings(table, update)
    session.commit()


def find_tickers(tickers, content):
    """Finds ticker symbols within reddit text content

    Returns
    -------
    dict(str: set(str))
        A dictionary with ID as keys and a set of found ticker symbols as values

    """
    output = {}
    for id, body in content.items():
        # find strings of capital letters e.g. '$AAPL', 'AAPL' and trim
        trimmed = [ticker.strip().replace("$", "")
                   for ticker in re.findall("(?:^|\s)\$?[A-Z]+(?:\s|$)", body)]
        output[id] = set(trimmed).intersection(tickers)
        if len(output[id]) > TOO_MANY_LABELS or len(output[id]) == 0:
            output[id] = {UNKNOWN_TICKER_STRING}
    return output


def label_content(table):
    """Labels reddit content with subject ticker symbols

    Returns
    -------
    dict(str: list(str))
        A dictionary with id as keys and a list of associated tickers as values

    """
    session = Session()
    tickers = set([row.symbol for row in session.query(Ticker.symbol).all()])
    if session.query(table.id).filter(table.labels.is_(None)).first() is not None:  # check for unlabeled content
        if table.__tablename__ == 'posts':
            query = session.query(table.id, table.title, table.selftext). \
                filter(table.labels.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.title + " " + item.selftext for item in query.all()}
        else:
            query = session.query(table.id, table.body). \
                filter(table.labels.is_(None)).limit(MAX_BATCH)
            content = {item.id: item.body for item in query.all()}
        return find_tickers(tickers, content)
    return {}


def write_ticker_labels(table, ticker_labels, content_ids):
    """Adds the assigned labels from a batch of content to the tickers table and marks content as processed

    """
    # TODO use SQLAlchemy, refactor to better match many-to-many relation
    # https://docs.sqlalchemy.org/en/13/orm/tutorial.html#building-a-many-to-many-relationship
    db = psycopg2.connect(os.environ['DATABASE_STRING'])
    update = [(item['labels'], item['symbol']) for item in ticker_labels]
    query = sql.SQL("UPDATE tickers SET content_ids = content_ids || %s WHERE symbol = %s")
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, update)

    query = sql.SQL("UPDATE {} SET processed=true WHERE id=%s").format(sql.Identifier(table.__tablename__))
    cur = db.cursor()
    processed = [(id,) for id in content_ids]
    psycopg2.extras.execute_batch(cur, query, processed)
    db.commit()
    cur.close()


def invert_labels(prefix, labels):
    """Inverts the ID: list of ticker symbols (convert to ticker symbol: list of IDs)
        Outputs a list of dictionaries for a SQLAlchemy batch update

    Returns
    -------
    list(dict(str: list(str)))
        A list of dictionaries with ticker symbols as keys and a list of content ids as values
    """
    symbol_map = {}
    for id, list in labels.items():
        for ticker in list:
            if ticker in symbol_map:
                symbol_map[ticker].append(prefix + id)
            else:
                symbol_map[ticker] = [prefix + id]
    symbol_map.pop(UNKNOWN_TICKER_STRING, None)
    return [{'symbol': symbol, 'labels': labels} for symbol, labels in symbol_map.items()]


def label_tickers(table):
    """Labels tickers with a list of associated content ids

    Returns
    -------
    tuple(list(dict(str: list(str))), list(str))
        A tuple with:
        A list of dictionaries with ticker symbols as keys and a list of content ids as values
        A list of content ids that were processed to generate the other list in the tuple
    """
    session = Session()
    if table.__tablename__ == 'posts':
        prefix = POST_PREFIX
    else:
        prefix = COMMENT_PREFIX

    if session.query(table.id).filter(table.processed.is_(False)).first() is not None:
        query = session.query(table.id, table.labels).filter(table.processed.is_(False)).limit(MAX_BATCH)
        content_labels = {content[0]: content[1] for content in query.all()}
        content_ids = [id for id in content_labels.keys()]
        return invert_labels(prefix, content_labels), content_ids
    return [], []

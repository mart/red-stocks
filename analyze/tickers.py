import requests
from datetime import datetime
import os.path
import csv
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import re

MARKETS = ['nasdaq', 'nyse', 'amex']
TOO_MANY_LABELS = 5
POST_PREFIX = 't3_'
COMMENT_PREFIX = 't1_'


def connect():
    """Connect to database

    """
    return psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


def download_tickers():
    """Downloads a csv stock tickers and companies from the NASDAQ website and makes a map

    Returns
    -------
    dict(str: str)
        A map of stock ticker to company name

    """
    today = datetime.today().strftime('%Y%m%d')
    if not os.path.exists(today + MARKETS[0] + '.csv'):
        for market in MARKETS:
            url = f"https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange={market}&render=download"
            response = requests.get(url, allow_redirects=True)
            with open(today + market + '.csv', 'wb+') as file:
                file.write(response.content)
    tickers = {}
    for market in MARKETS:
        with open(today + market + '.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            next(reader)
            for row in reader:
                tickers[row[0]] = row[1].replace(",", "")
    return tickers


def write_tickers(db, tickers):
    """Adds the given tickers to the tickers database table

    """
    query = sql.SQL("INSERT INTO tickers VALUES %s")
    cur = db.cursor()
    psycopg2.extras.execute_values(cur, query.as_string(cur), tickers)
    db.commit()
    cur.close()


def read_tickers(db):
    """Queries stored ticker symbols from the database and returns a set

    Returns
    -------
    set(str)
        A set of all the stock ticker symbols from the database

    """
    query = sql.SQL("SELECT symbol FROM tickers")
    cur = db.cursor()
    cur.execute(query)
    output = [symbol[0] for symbol in cur.fetchall()]
    cur.close()
    return set(output)


def add_tickers():
    """Downloads ticker lists and adds new tickers to the database

    """
    db = connect()
    tickers = download_tickers()
    new_tickers = []
    for symbol in set(tickers.keys()).difference(read_tickers(db)):
        new_tickers.append((symbol, tickers[symbol]))
    write_tickers(db, new_tickers)


def unlabeled_content(db, table):
    """Determines whether or not there is content that needs labeling

    Returns
    -------
    bool
        True if at least one content item has not yet been labeled

    """
    query = sql.SQL("SELECT id FROM {}"
                    " WHERE labels is NULL"
                    " LIMIT 1").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query)
    output = cur.fetchone()
    cur.close()
    return output is not None


def read_posts(db):
    """Queries a batch of unlabeled reddit posts from the database

    Returns
    -------
    dict(str: str)
        A dictionary with ID as keys and text content as values

    """
    query = sql.SQL("SELECT id, title, selftext FROM posts"
                    " WHERE labels is NULL"
                    " LIMIT 10000")
    cur = db.cursor()
    cur.execute(query)
    output = {content[0]: content[1] + " " + content[2] for content in cur.fetchall()}
    cur.close()
    return output


def read_comments(db):
    """Queries a batch of unlabeled reddit comments from the database

    Returns
    -------
    dict(str: str)
        A dictionary with ID as keys and text content as values
    """
    query = sql.SQL("SELECT id, body FROM comments"
                    " WHERE labels is NULL"
                    " LIMIT 10000")
    cur = db.cursor()
    cur.execute(query)
    output = {content[0]: content[1] for content in cur.fetchall()}
    cur.close()
    return output


def find_tickers(tickers, content):
    """Finds ticker symbols within reddit text content

    Returns
    -------
    dict(str: set(str))
        A dictionary with ID as keys and a set of found ticker symbols as values

    """
    output = {}
    for id, body in content.items():
        # remove capital letters when used normally e.g. 'The squirrel' -> ' e squirrel', then intersect
        trimmed = re.sub("\s[A-Z][^A-Z]", " ", re.sub("^[A-Z][^A-Z]", " ", body))
        output[id] = set(re.findall("[A-Z]+", trimmed)).intersection(tickers)
        if len(output[id]) > TOO_MANY_LABELS or len(output[id]) == 0:
            output[id] = {'UNKNOWN'}
    return output


def write_content_labels(db, table, content):
    """Add content labels to the database

    """
    update = [(list(labels), id) for id, labels in content.items()]
    query = sql.SQL("UPDATE {} SET labels=%s WHERE id=%s").format(sql.Identifier(table))
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, update)
    db.commit()
    cur.close()


def label_content():
    """Labels reddit content with possible subject ticker symbols, adds to database

    """
    db = connect()
    tickers = read_tickers(db)
    while unlabeled_content(db, 'posts'):
        posts = find_tickers(tickers, read_posts(db))
        write_content_labels(db, 'posts', posts)
    while unlabeled_content(db, 'comments'):
        comments = find_tickers(tickers, read_comments(db))
        write_content_labels(db, 'comments', comments)


def read_content_labels(db, table):
    """Queries a batch of unprocessed reddit content from the database

    Returns
    -------
    dict(str: str)
        A dictionary with ID as keys and text content as values
    """
    query = sql.SQL("SELECT id, labels FROM {}"
                    " WHERE processed = false"
                    " LIMIT 10000").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query)
    output = {content[0]: content[1] for content in cur.fetchall()}
    cur.close()
    return output


def invert_labels(prefix, labels):
    """Inverts the ID: list of ticker symbols (convert to ticker symbol: list of IDs)

    Returns
    -------
    dict(str: list(str))
        A dictionary with ticker symbols as keys and a list of content ids as values
    """
    output = {}
    for id, list in labels.items():
        for label in list:
            if label in output:
                output[label].append(prefix + id)
            else:
                output[label] = [prefix + id]
    output.pop('UNKNOWN', None)
    return output


def write_ticker_labels(db, table, ticker_labels, content_ids):
    """Adds the assigned labels from a batch of content to the tickers table and marks content as processed

    """
    update = [(list(ids), ticker) for ticker, ids in ticker_labels.items()]
    query = sql.SQL("UPDATE tickers SET content_ids = content_ids || %s WHERE symbol = %s")
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, update)
    db.commit()

    query = sql.SQL("UPDATE {} SET processed=true WHERE id=%s").format(sql.Identifier(table))
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, content_ids)
    db.commit()
    cur.close()


def unprocessed_content(db, table):
    """Determines if there is unprocessed content in the database

    Returns
    -------
    bool
        True if at least one item is unprocessed in the table
        
    """
    query = sql.SQL("SELECT id FROM {}"
                    " WHERE processed = false"
                    " LIMIT 1").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query)
    output = cur.fetchone()
    cur.close()
    return output is not None


def label_tickers():
    """Labels tickers with a list of associated content ids, adds to database, marks content processed

    """
    db = connect()
    while unprocessed_content(db, 'posts'):
        content_labels = read_content_labels(db, 'posts')
        content_ids = [(id,) for id in content_labels.keys()]
        ticker_labels = invert_labels(POST_PREFIX, content_labels)
        write_ticker_labels(db, 'posts', ticker_labels, content_ids)
    while unprocessed_content(db, 'comments'):
        content_labels = read_content_labels(db, 'comments')
        content_ids = [(id,) for id in content_labels.keys()]
        ticker_labels = invert_labels(COMMENT_PREFIX, content_labels)
        write_ticker_labels(db, 'comments', ticker_labels, content_ids)


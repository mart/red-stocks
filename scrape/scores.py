import os
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import praw
from prawcore import exceptions
import datetime
from time import sleep

# update when an item is more than 24 hours old
UPDATE_TIME = 86400
# update this many more seconds past the UPDATE_TIME
UPDATE_BUFFER = 1200


def connect():
    """Connect to database

    """
    return psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


def now():
    """The current time

    Returns
    -------
    int
        The current time in UNIX timestamp format

    """
    return int(datetime.datetime.today().timestamp())


def start_reddit():
    """Initializes the PRAW object

    Returns
    -------
    Reddit
        A reddit object connected using the reddit API

    """
    return praw.Reddit(client_id=os.environ['SCRIPT_ID'],
                       client_secret=os.environ['SECRET'],
                       user_agent=os.environ['APPNAME'],
                       username=os.environ['USERNAME'],
                       password=os.environ['PASSWORD'])


def should_update(db, table):
    """Queries the table to find out if last update was not recent

    Returns
    -------
    boolean
        True if at least one item found that needs an update

    """
    query = sql.SQL("SELECT update_age FROM {}"
                    " WHERE (created_utc < (select extract(epoch from now()) - %s)"
                    " AND update_age < %s)"
                    " LIMIT 1").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query, [UPDATE_TIME + UPDATE_BUFFER, UPDATE_TIME])
    output = cur.fetchone()
    cur.close()
    return output is not None


def update():
    """Updates the scores for items that are more than UPDATE_TIME old

    """
    reddit = start_reddit()
    db = connect()
    while should_update(db, "comments"):
        print("Updating comment batch...")
        update_items(db, "comments", reddit)
    while should_update(db, "posts"):
        print("Updating post batch...")
        update_items(db, "posts", reddit)
    db.close()


def update_items(db, table, reddit):
    """Updates a batch of items that are more than UPDATE_TIME old

    """
    items = read_items(db, table)
    updated, deleted = scrape_content(items, table, reddit)

    if deleted:
        query = sql.SQL("UPDATE {} SET retrieved_on=%s, update_age=%s - created_utc, deleted=true "
                        "WHERE id=%s").format(sql.Identifier(table))
        cur = db.cursor()
        psycopg2.extras.execute_batch(cur, query, deleted)
        db.commit()

    query = sql.SQL("UPDATE {} SET score=%s, retrieved_on=%s, update_age=%s - created_utc WHERE id=%s")\
        .format(sql.Identifier(table))
    cur = db.cursor()
    psycopg2.extras.execute_batch(cur, query, updated)
    db.commit()
    cur.close()


def read_items(db, table):
    """Finds a batch of items to update

    Returns
    -------
    list
        A list of reddit item IDs that need score updates

    """
    query = sql.SQL("SELECT id FROM {}"
                    " WHERE (created_utc < (select extract(epoch from now()) - %s) AND update_age < %s)"
                    " LIMIT 2000").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query, [UPDATE_TIME, UPDATE_TIME])
    output = [item[0] for item in cur.fetchall()]
    cur.close()
    return output


def scrape_content(result_ids, table, reddit):
    """Finds updated scores and deleted items

    Returns
    -------
    list(tuple)
        A list of tuples with (score, now(), now(), id) representing items that were found
    list(tuple)
        A list of tuples with (now(), now(), id) representing items that could not be found
    """
    if table == "comments":
        prefix = "t1_"
    elif table == "posts":
        prefix = "t3_"
    else:
        raise Exception('table cannot be {}, can only get reddit posts or comments'.format(table))

    scrape_list = [prefix + reddit_id for reddit_id in result_ids]
    output = praw_scrape(reddit, scrape_list)
    deleted = []

    if len(output) != len(result_ids):
        retrieved = now()
        found_ids = set([item[3] for item in output])
        deleted = [(retrieved, retrieved, item) for item in result_ids if item not in found_ids]

    return output, deleted


def praw_scrape(reddit, scrape_list):
    """Uses PRAW to get new scores for items

    Returns
    -------
    list(tuple)
        A list of tuples with (score, now(), now(), id) representing items that were found
    """
    gen = reddit.info(fullnames=scrape_list)
    try:
        output = []
        retrieved = now()
        for c in gen:
            output.append((c.score, retrieved, retrieved, c.id))
    except exceptions.ResponseException as e:
        print(str(e) + " at " + str(now()))
        return [("0", "0")]
    except exceptions.RequestException as e:
        print(str(e) + " at " + str(now()))
        sleep(60)
        return [("0", "0")]
    return output

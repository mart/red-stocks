import os
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
from psaw import PushshiftAPI
import datetime

# update
UPDATE_FREQUENCY = 300
# don't go too far back in time (1483228818 is Jan 1 2017)
EARLIEST_CREATED = 1483228818


def connect():
    """Connect to database

    """
    return psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


def gilded(gildings):
    """Converts the guildings into a simple total count

    Returns
    -------
    int
        number of gildings
    """
    return sum(gildings.values())


def get_items(table, subreddit, latest):
    """Gets a batch of reddit content using pushshift.io and starting from latest

    Returns
    -------
    list(list)
        list of PSAW items cast into a list format
    """
    api = PushshiftAPI()
    if table == "posts":
        gen = api.search_submissions(after=latest, subreddit=subreddit, limit=1000, sort="asc")
    elif table == "comments":
        gen = api.search_comments(after=latest, subreddit=subreddit, limit=1000, sort="asc")
    else:
        raise Exception('table cannot be {}, can only get reddit posts or comments'.format(table))
    max_response_cache = 1000
    cache = []
    for c in gen:
        cache.append(c)
        if len(cache) >= max_response_cache:
            break
    update = prepare_items(table, cache)
    return update


def latest_item(db, table, subreddit):
    """Finds the created time of the latest content in the database from the specified subreddit

    Returns
    -------
    int
        The UNIX timestamp of the latest reddit content from the specified subreddit

    """
    query = sql.SQL("SELECT MAX(created_utc) FROM {} WHERE subreddit=%s").format(sql.Identifier(table))
    cur = db.cursor()
    cur.execute(query, [subreddit])
    output = cur.fetchone()[0]
    cur.close()

    if output is None or output < EARLIEST_CREATED:
        output = EARLIEST_CREATED
    return output


def should_scrape(db, table, subreddit, previous_item):
    """Queries the table to find out if last scrape was not recent

    Returns
    -------
    boolean
        True if posts were added last time and should scrape again

    """
    latest = latest_item(db, table, subreddit)
    return previous_item != latest and latest < int(datetime.datetime.today().timestamp()) - UPDATE_FREQUENCY


def scrape(subreddits):
    """Scrapes reddit content via pushshift.io and adds new content to the database

    """
    db = connect()
    for sub in subreddits:
        while True:
            latest_comment = latest_item(db, "comments", sub)
            print("adding: " + "comments for " + sub + " from " + str(latest_comment))
            add_items(db, "comments", sub, latest_comment)
            if not should_scrape(db, "comments", sub, latest_comment):
                break
        while True:
            latest_post = latest_item(db, "posts", sub)
            print("adding: " + "posts for " + sub + " from " + str(latest_post))
            add_items(db, "posts", sub, latest_post)
            if not should_scrape(db, "posts", sub, latest_post):
                break
    db.close()


def add_items(db, table, subreddit, latest_entry):
    """Adds new items to the database

    """
    update = get_items(table, subreddit, latest_entry)
    query = sql.SQL("INSERT INTO {} VALUES %s").format(sql.Identifier(table))
    cur = db.cursor()
    psycopg2.extras.execute_values(cur, query.as_string(cur), update)
    db.commit()
    cur.close()


def prepare_items(table, items):
    """Takes a list of items and turns them into a psycopg2-friendly list format

    Returns
    -------
    list(list)
        list of PSAW items cast into a list format

    """
    output = []
    if table == "comments":
        for comment in items:
            # attributes can be missing from psaw output
            try:
                gilds = gilded(comment.gildings)
            except AttributeError:
                gilds = 0
            try:
                retrieved_on = comment.retrieved_on
            except AttributeError:
                retrieved_on = comment.created_utc
            output.append([comment.body,
                           comment.author,
                           comment.author_flair_text,
                           comment.created_utc,
                           comment.subreddit_id,
                           comment.link_id,
                           comment.parent_id,
                           comment.score,
                           retrieved_on,
                           gilds,
                           comment.id,
                           comment.subreddit,
                           comment.author_flair_css_class,
                           retrieved_on - comment.created_utc])
    if table == "posts":
        for post in items:
            # attributes can be missing from psaw output
            try:
                gilds = gilded(post.gildings)
            except AttributeError:
                gilds = 0
            try:
                selftext = post.selftext
            except AttributeError:
                selftext = ""
            output.append([post.created_utc,
                           post.subreddit,
                           post.author,
                           post.domain,
                           post.url,
                           post.num_comments,
                           post.score,
                           post.title,
                           selftext,
                           post.id,
                           gilds,
                           post.stickied,
                           post.retrieved_on,
                           post.over_18,
                           post.thumbnail,
                           post.subreddit_id,
                           post.author_flair_css_class,
                           post.is_self,
                           post.permalink,
                           post.author_flair_text,
                           post.retrieved_on - post.created_utc])
    return output

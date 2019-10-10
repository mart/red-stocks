from psaw import PushshiftAPI
from sqlalchemy import func
import logging
import os
from db.models import Session

MAX_BATCH = 10000
log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ['LOG_LEVEL'])


def get_items(table, subreddit, latest):
    """Gets a batch of reddit content using pushshift.io, starting from latest

    Returns
    -------
    list(object)
        list of PSAW items (similar to PRAW objects)
    """
    api = PushshiftAPI()
    if table.__tablename__ == "posts":
        gen = api.search_submissions(after=latest, subreddit=subreddit, limit=1000, sort="asc")
    elif table.__tablename__ == "comments":
        gen = api.search_comments(after=latest, subreddit=subreddit, limit=1000, sort="asc")
    else:
        raise Exception('table cannot be {}, can only get reddit posts or comments'.format(table))
    max_response_cache = MAX_BATCH
    cache = []
    for c in gen:
        cache.append(c)
        if len(cache) >= max_response_cache:
            break
    return cache


def latest_item(session, table, subreddit, earliest_content):
    """Finds the created timestamp of the latest content in the database from the specified subreddit

    Returns
    -------
    int
        The UNIX timestamp of the latest reddit content from the specified subreddit

    """
    latest = session.query(func.max(table.created_utc)).filter_by(subreddit=subreddit).scalar()
    if latest is None or latest < earliest_content:
        latest = earliest_content
    return latest


def scrape_content(subreddit, table, earliest_content):
    """Scrapes reddit comments via pushshift.io from subreddits

    Returns
    -------
    list(object)
        list of PSAW items similar to PRAW comment objects
    """
    session = Session()
    content = []
    last_update = -1

    while len(content) < MAX_BATCH:
        latest_content = latest_item(session, table, subreddit, earliest_content)
        if latest_content == last_update:
            break
        content += get_items(table, subreddit, latest_content)
        log.info("Scraped " + subreddit + " " + table.__tablename__ + " from " + str(latest_content))
        last_update = latest_content
    return content

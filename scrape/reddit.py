from psaw import PushshiftAPI
from sqlalchemy import func

from db.models import Session

MAX_BATCH = 10000


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


def scrape_content(subreddits, table, earliest_content):
    """Scrapes reddit comments via pushshift.io from subreddits

    Returns
    -------
    list(object)
        list of PSAW items similar to PRAW comment objects
    """
    session = Session()
    content = []

    for subreddit in subreddits:
        latest_content = latest_item(session, table, subreddit, earliest_content)
        content += get_items(table, subreddit, latest_content)
        if len(content) > MAX_BATCH:
            break

    return content

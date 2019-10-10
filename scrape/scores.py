from datetime import datetime
from time import sleep
import logging
import os
from prawcore import exceptions

from db.models import Session

MAX_BATCH = 10000
log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ['LOG_LEVEL'])


def update_content(table, updated, deleted):
    """Updates database with new scores if not deleted, marks all given content as updated

    """
    session = Session()
    deleted_batch = [{'id': item[0], 'retrieved_on': item[1], 'update_age': item[1], 'deleted': True}
                     for item in deleted]
    updated_batch = [{'id': item[0], 'retrieved_on': item[1], 'update_age': (item[1] - item[2]), 'score': item[3]}
                     for item in updated]

    session.bulk_update_mappings(table, deleted_batch)
    session.bulk_update_mappings(table, updated_batch)
    session.commit()


def praw_scrape(reddit, scrape_list):
    """Uses PRAW to get new scores for items

    Returns
    -------
    list(tuple)
        A list of tuples with (id, timestamp, created_utc, score) representing items that were found
    """
    gen = reddit.info(fullnames=scrape_list)
    try:
        output = []
        retrieved = int(datetime.today().timestamp())
        for c in gen:
            output.append((c.id, retrieved, c.created_utc, c.score))
    except exceptions.ResponseException as e:
        log.warning(str(e) + " at " + str(int(datetime.today().timestamp())))
        return []
    except exceptions.RequestException as e:
        log.warning(str(e) + " at " + str(int(datetime.today().timestamp())))
        sleep(60)
        return []
    return output


def needs_update(session, table, update_frequency, update_buffer):
    """Queries the table to find out if a score update is needed

    Finds an item that hasn't been updated at least uopdate_time after creation
     and is older than update_time + update_buffer

    Returns
    -------
    boolean
        True if at least one item found that needs an update

    """
    update_cutoff = int(datetime.today().timestamp()) - (update_frequency + update_buffer)
    query = session.query(table.update_age). \
        filter(table.created_utc < update_cutoff, table.update_age < update_frequency).first()
    return query is not None


def scrape_update(reddit, table, update_frequency, update_buffer):
    """Finds and gets updates for a batch of items that are more than update_frequency old

    Returns
    -------
    tuple(list(tuple), list(tuple))
        A tuple with:
        A list of tuples with (id, timestamp, created_utc, score) representing items that were found
        A list of tuples with (id, timestamp) representing items that could not be found
    """
    session = Session()
    if needs_update(session, table, update_frequency, update_buffer):
        update_cutoff = int(datetime.today().timestamp()) - update_frequency
        items = session.query(table). \
            filter(table.created_utc < update_cutoff, table.update_age < update_frequency).limit(MAX_BATCH)

        if table.__tablename__ == "comments":
            prefix = "t1_"
        else:
            prefix = "t3_"

        scrape_list = [prefix + item.id for item in items]
        output = praw_scrape(reddit, scrape_list)
        deleted = []

        if len(output) != len(scrape_list):
            retrieved_on = int(datetime.today().timestamp())
            found_ids = set([item[3] for item in output])
            deleted = [(item, retrieved_on) for item in items if item.id not in found_ids]

        log.info("Scraped " + table.__tablename__ + " scores to " + str(update_cutoff))
        return output, deleted
    return [], []

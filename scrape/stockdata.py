from os import environ
from db.models import Ticker, Session
import requests
import logging
from datetime import datetime

DATA_API_URL = 'https://api.tiingo.com/tiingo/daily/'
API_KEY = environ['TIINGO_API']
log = logging.getLogger(__name__)
logging.basicConfig(level=environ['LOG_LEVEL'])


class TiingoAPIError(ConnectionError):
    pass


def update_stock_data(start_date, tickers=None):
    """Obtains stock data for all available tickers from start_date to present

    """
    session = Session()
    now = int(datetime.today().timestamp())
    skipped = 0

    if tickers is None:
        tickers = {row.symbol for row in session.query(Ticker.symbol).all()}

    for symbol in tickers:
        ticker = session.query(Ticker)\
            .filter(Ticker.symbol == symbol).first()
        params = {'token': API_KEY, 'startDate': start_date}
        try:
            new_data = data_request(params, symbol)
        except TiingoAPIError as err:
            log.warning(err)
            # Error logged, skip this ticker
            skipped += 1
            continue
        ticker.price_data = new_data
        ticker.last_update = now
        session.commit()
    log.info("Skipped " + str(skipped) + "symbols")
    log.info("Updated " + str((len(tickers)) - skipped) + "symbols")


def data_request(params, ticker):
    """Obtains stock data for the ticker

    """
    url = DATA_API_URL + ticker + "/prices"
    raw_data = requests.get(url, params=params).json()
    log.debug("Stock data for " + ticker)

    if isinstance(raw_data, list) and len(raw_data) < 2:
        raise TiingoAPIError("Error with " + ticker + " request: " + raw_data[0][:20]
                             if len(raw_data) > 0
                             else "Unknown response for " + ticker + ": " + str(raw_data))

    if isinstance(raw_data, dict) and raw_data.get("detail") is not None:
        raise TiingoAPIError("Error with " + ticker + " request: " + raw_data.get("detail"))

    return raw_data

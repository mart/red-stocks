from datetime import datetime
from time import sleep

from analyze.sentiment import sentiment
from analyze.tickers import add_tickers, label_content, label_tickers
from scrape.reddit import scrape
from scrape.scores import update, now

if __name__ == "__main__":
    subreddits = ["investing", "RobinHood", "wallstreetbets",
                  "options", "stocks", "weedstocks", "TheCannalysts",
                  "SecurityAnalysis", "StockMarket", "InvestmentClub",
                  "Stock_Picks", "ValueInvesting", "CanadianInvestor",
                  "UKInvesting", "pennystocks", "M1Finance"]
    worker_start = datetime.today().date()
    while True:
        start = now()
        sentiment()
        scrape(subreddits)
        update()
        label_content()
        label_tickers()
        if now() - start < 60:
            sleep(now() - start)
        if worker_start != datetime.today().date():
            add_tickers()
            worker_start = datetime.today().date()

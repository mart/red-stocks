from scrape.reddit import scrape
from scrape.scores import update, now
from analyze.tickers import add_tickers, label_content, label_tickers
from datetime import datetime
from time import sleep

if __name__ == "__main__":
    subreddits = ["investing", "RobinHood", "wallstreetbets",
                  "options", "stocks", "weedstocks", "TheCannalysts",
                  "SecurityAnalysis", "StockMarket", "InvestmentClub",
                  "Stock_Picks", "ValueInvesting", "CanadianInvestor",
                  "UKInvesting", "pennystocks", "M1Finance"]
    worker_start = datetime.today().date()
    while True:
        start = now()
        scrape(subreddits)
        update()
        label_content()
        label_tickers()
        if now() - start < 30:
            sleep(now() - start)
        if worker_start != datetime.today().date():
            add_tickers()
            worker_start = datetime.today().date()

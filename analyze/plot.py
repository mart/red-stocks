import pandas as pd
import seaborn
from db.models import Session, Ticker
import matplotlib.pyplot as plt
from matplotlib.patches import ArrowStyle
import logging
from os import environ

log = logging.getLogger(__name__)
logging.basicConfig(level=environ['LOG_LEVEL'])


def plot_sentiment_by_item(symbol, num_days):
    """Creates a plot with price data and sentiment markers

    Annotates this plot with arrows representing content with negative (red) or
    positive (green) sentiment scores. If the plot is too crowded to display all
    scores on record, it is trimmed to show only the past 35 days and labels
    arrows with the number of negative or positive items.
    """
    session = Session()
    log.debug("Plotting " + symbol)
    seaborn.set()

    data = session.query(Ticker.price_data) \
        .filter(Ticker.symbol == symbol).scalar()
    df = pd.DataFrame(data)
    df = df.iloc[-num_days:]
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True, drop=False)

    fig, ax1 = plt.subplots()
    seaborn.lineplot(x="date", y="adjOpen", data=df, ax=ax1)
    fig.autofmt_xdate()

    if 'sentiment_sum' in df.columns:   # if true, this has sentiment data
        annotate_plot(df)

    plt.savefig("test.svg")


def annotate_plot(df):
    """Annotates this plot with arrows representing sentiment scores

     If labels are to be shown, the arrow opacity is maximized, otherwise the
     opacity is scaled to represent the magnitude of the sentiment score.
     Content that cannot be matched to data is skipped.
    """
    arrow_length = (df["adjOpen"].max() - df["adjOpen"].min()) / 15
    arrow_style = ArrowStyle("Fancy", head_length=.25, head_width=.25, tail_width=.15)
    max_sentiment = max(df['scaled_sentiment'].max(),
                        abs(df['scaled_sentiment'].min()))

    for index, row in df.iterrows():
        alpha = max(abs(row['scaled_sentiment'] / max_sentiment), 0.2)
        price_level = row['adjOpen']
        ytext = price_level
        color = 'k'
        if row['scaled_sentiment'] < 0:
            color = 'r'
            ytext = price_level + arrow_length
        elif row['scaled_sentiment'] > 0:
            color = 'g'
            ytext = price_level - arrow_length

        plt.annotate("",
                     xy=(index, price_level),
                     xycoords='data',
                     xytext=(index, ytext),
                     textcoords='data',
                     arrowprops=dict(arrowstyle=arrow_style, color=color, linewidth=0.5, alpha=alpha),
                     horizontalalignment='center')

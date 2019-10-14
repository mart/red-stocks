import pandas as pd
import seaborn
from db.models import Session, Ticker, Post, Comment
import matplotlib.pyplot as plt
from matplotlib.patches import ArrowStyle
from datetime import datetime
import logging
from os import environ
from time import mktime

log = logging.getLogger(__name__)
logging.basicConfig(level=environ['LOG_LEVEL'])
pd.options.mode.chained_assignment = None


def plot_sentiment(symbol):
    """Creates a plot with price data and sentiment markers

    Annotates this plot with arrows representing content with negative (red) or
    positive (green) sentiment scores. If the plot is too crowded to display all
    scores on record, it is trimmed to show only the past 35 days and labels
    arrows with the number of negative or positive items.
    """
    log.debug("Plotting " + symbol)
    session = Session()

    data = session.query(Ticker.price_data) \
        .filter(Ticker.symbol == symbol).scalar()
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True, drop=False)
    df['positive_count'] = 0
    df['negative_count'] = 0

    sentiment = get_sentiment_data(symbol, session)

    # filter for data with nonzero sentiment
    nonzero_content = [item for item in sentiment if item[0] != 0]
    show_labels = False
    if len(nonzero_content) > len(df):  # trim data if plot is crowded
        df = df[-35:]
        first_time = int(mktime(df["date"][0].timetuple()))
        sentiment = [item for item in nonzero_content if item[1] >= first_time]
        show_labels = True  # show counts because many arrows will overlap

    seaborn.set()
    plot = seaborn.relplot(x="date", y="adjClose", kind="line", data=df)
    plot.fig.autofmt_xdate()
    annotate(df, sentiment, show_labels)
    plt.savefig("test.svg")


def annotate(df, sentiment, show_labels):
    """Annotates this plot with arrows representing sentiment scores

     If labels are to be shown, the arrow opacity is maximized, otherwise the
     opacity is scaled to represent the magnitude of the sentiment score.
     Content that cannot be matched to data is skipped.
    """
    arrow_length = (df["adjClose"].max() - df["adjClose"].min()) / 15
    arrow_style = ArrowStyle("Fancy", head_length=.25, head_width=.25, tail_width=.15)
    for content in sentiment:
        create_date = datetime.fromtimestamp(content[1])
        closest_after = df[create_date:].first_valid_index()
        try:
            price_level = df.loc[closest_after]['adjClose']
        except TypeError:  # no matching date in the dataset
            log.debug("Skipping annotation for " + create_date.strftime("%Y-%m-%d"))
            continue

        alpha = abs(content[0])
        if show_labels:
            alpha = 1.0
        color = 'k'
        xtext = price_level
        if content[0] < 0:
            color = 'r'
            xtext = price_level + arrow_length
            df['negative_count'][closest_after] += 1
        elif content[0] > 0:
            color = 'g'
            xtext = price_level - arrow_length
            df['positive_count'][closest_after] += 1

        plt.annotate("",
                     xy=(closest_after, price_level),
                     xycoords='data',
                     xytext=(closest_after, xtext),
                     textcoords='data',
                     arrowprops=dict(arrowstyle=arrow_style, color=color, linewidth=0.5, alpha=alpha),
                     horizontalalignment='center')

    if show_labels:
        for index, row in df.iterrows():
            if row['negative_count'] > 0:
                plt.annotate(str(row['negative_count']),
                             xy=(row['date'], row['adjClose']),
                             xycoords='data',
                             xytext=(row['date'], row['adjClose'] + arrow_length),
                             textcoords='data',
                             arrowprops=dict(arrowstyle='-', color='r', alpha=0),
                             horizontalalignment='center',
                             fontsize=5)
            if row['positive_count'] > 0:
                plt.annotate(str(row['positive_count']),
                             xy=(row['date'], row['adjClose']),
                             xycoords='data',
                             xytext=(row['date'], row['adjClose'] - arrow_length),
                             textcoords='data',
                             arrowprops=dict(arrowstyle='-', color='g', alpha=0),
                             horizontalalignment='center',
                             verticalalignment='top',
                             fontsize=5)


def get_sentiment_data(symbol, session):
    """Obtains sentiment data from the database

    Returns
    -------
    list(tuple(float, int))
        A list of tuples representing sentiment data with:
        A float sentiment score
        An int representing the epoch time the comment or post was created
    """
    content_ids = session.query(Ticker.content_ids) \
        .filter(Ticker.symbol == symbol).scalar()
    posts = [item[3:] for item in content_ids if item[0:3] == "t3_"]
    comments = [item[3:] for item in content_ids if item[0:3] == "t1_"]
    sentiment = session.query(Post.sentiment, Post.created_utc) \
        .filter(Post.id.in_(posts)).all()
    sentiment += session.query(Comment.sentiment, Comment.created_utc) \
        .filter(Comment.id.in_(comments)).all()
    return sentiment

import json
import logging
import sys
from collections.abc import Iterator
from datetime import date, datetime, timedelta

import boto3
import newspaper
from gnews import GNews

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%m/%d/%Y %I:%M:%S %p",
)
logger = logging.getLogger(__name__)

DATE_FMT_STR = "%Y-%m-%d"

config = {}


def s3_put_object(
    art_date: str, art_topic: str, art_title: str, art_dict: dict
):
    s3 = boto3.resource("s3")
    s3_path = f"{art_date}/{art_topic}/{art_title}"
    s3.Bucket(config["S3_BUCKET"]).put_object(
        Key=s3_path,
        Body=json.dumps(art_dict),
    )
    logging.info(s3_path)


def get_full_article(url: str):
    """
    Download an article from the specified URL, parse it, and return an article
    object.
     :param url: The URL of the article you wish to summarize.
     :return: An `Article` object returned by the `newspaper` library.
    """
    # Check if the `newspaper` library is available
    if "newspaper" not in (
        sys.modules.keys() & globals()
    ):  # Top import failed since it's not installed
        print("\nget_full_article() requires the `newspaper` library.")
        print(
            """You can install it by running `python3 -m pip install
            newspaper3k` in your shell.\n"""
        )
        return None
    try:
        article = newspaper.Article(url="%s" % url, language="en")
        article.download()
        article.parse()
    except Exception as error:
        logger.error(error.args[0])
        return None
    return article


def daterange(start_date: date, end_date: date) -> Iterator[date]:
    for n in range(int((end_date - start_date).days)):
        yield (start_date + timedelta(n)).strftime(DATE_FMT_STR)


def clean_article_title(title: str) -> str:
    return title.strip().replace("/", "-") + '.json'

def get_articles_for_date_range(date_range: Iterator[date]):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """

    google_news = GNews()

    google_news.exclude_websites = [
        "thehill.com",
        "investors.com",
        "si.com",
        "newsweek.com",
        "wsj.com",
    ]

    topics = [
        "WORLD",
        "NATION",
        "BUSINESS",
        "TECHNOLOGY",
        "ENTERTAINMENT",
        "SCIENCE",
        "HEALTH",
    ]

    for _date in date_range:
        logging.info("Date to gather: %s", _date)

        for topic in topics:
            _articles = google_news.get_news_by_topic(topic)

            for article in _articles:
                date_str = article["published date"]
                format_str = "%a, %d %b %Y %H:%M:%S %Z"
                date_obj = datetime.strptime(date_str, format_str)
                article["published date"] = date_obj.strftime(DATE_FMT_STR)

                # (03/29/2023) limitation of the GNews library is that
                # it cannot filter by date for the "get_news_by_topic" method.
                # This is a workaround to filter by date.

                if article["published date"] != _date:
                    logging.warning(
                        f"Article date is not in date range: date expected \
                            {_date}, date for article \
                                {article['published date']}"
                    )
                    continue

                _full_article = get_full_article(article["url"])
                if _full_article and hasattr(_full_article, "text"):
                    article["text"] = _full_article.text
                    article["topic"] = topic
                    article["publisher"] = dict(article["publisher"])
                    article["title"] = clean_article_title(article["title"])
                    s3_put_object(
                        article["published date"],
                        article["topic"],
                        article["title"],
                        article,
                    )

    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")


def main():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)
    date_range = daterange(start_date, end_date)

    # TODO: Breakout period into event option once testing is complete
    get_articles_for_date_range(date_range)


def lambda_handler(event, context):
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    config["S3_BUCKET"] = event["S3_BUCKET"]
    main()

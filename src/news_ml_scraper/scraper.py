import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import click
import newspaper
from dotenv import find_dotenv, load_dotenv
from gnews import GNews

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%m/%d/%Y %I:%M:%S %p",
)
logger = logging.getLogger(__name__)


def s3_put_object(article_date, article_topic, article_title, article_json):
    s3 = boto3.resource("s3")
    s3.Bucket("liamwazherealso-news-ml").put_object(
        Key=f"""{article_date}/{article_topic}/
                                    {article_title}""",
        Body=json.dumps(article_json),
    )


def get_full_article(url):
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


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


@click.command()
def main():
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """

    dates = []

    start_date = date(2016, 1, 1)
    # non inclusive
    end_date = date(2022, 6, 2)
    for single_date in daterange(start_date, end_date):
        dates.append((single_date.year, single_date.month, single_date.day))

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

    for i in range(len(dates[:-1])):
        google_news.start_date = dates[i]
        google_news.end_date = dates[i + 1]

        for topic in topics:
            _articles = google_news.get_news_by_topic(topic)

            for article in _articles:
                _full_article = get_full_article(article["url"])
                if _full_article and hasattr(_full_article, "text"):
                    article["text"] = _full_article.text
                    article["topic"] = topic
                    article["publisher"] = dict(article["publisher"])
                    date_str = article["published date"]
                    format_str = "%a, %d %b %Y %H:%M:%S %Z"
                    date_obj = datetime.strptime(date_str, format_str)
                    article["published date"] = date_obj.strftime("%Y-%m-%d")
                    s3_put_object(
                        article["published date"],
                        article["topic"],
                        article["title"],
                        article,
                    )

    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    main()

from gnews import GNews
import newspaper
import sys
from tinydb import TinyDB, Query

# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import logging


from datetime import date, timedelta

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO,
                    datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)


def get_full_article(url):
    """
    Download an article from the specified URL, parse it, and return an article object.
     :param url: The URL of the article you wish to summarize.
     :return: An `Article` object returned by the `newspaper` library.
    """
    # Check if the `newspaper` library is available
    if 'newspaper' not in (sys.modules.keys() & globals()):  # Top import failed since it's not installed
        print("\nget_full_article() requires the `newspaper` library.")
        print("You can install it by running `python3 -m pip install newspaper3k` in your shell.\n")
        return None
    try:
        article = newspaper.Article(url="%s" % url, language='en')
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
@click.argument('output_filepath', type=click.Path())
def main(output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    dates = []
    
    db = TinyDB(output_filepath)

    start_date = date(2016, 1, 1)
    # non inclusive
    end_date = date(2022, 6, 2)
    for single_date in daterange(start_date, end_date):
        dates.append((single_date.year, single_date.month, single_date.day))
        
    google_news = GNews()
    
    
    google_news.exclude_websites = ['thehill.com', 'investors.com', 'si.com', 'newsweek.com', 'wsj.com']
    
    topics = ['WORLD', 'NATION', 'BUSINESS', 'TECHNOLOGY', 'ENTERTAINMENT', 'SCIENCE', 'HEALTH']
    for i in range(len(dates[:-1])):
        google_news.start_date = dates[i]
        google_news.end_date = dates[i+1]
        
        for topic in topics:
            _articles = google_news.get_news_by_topic(topic)
        
            for article in _articles:
                _full_article = get_full_article(article['url'])
                if _full_article:
                    article['text'] = _full_article.text
                    article['topic'] = topic
                    db.insert(article)

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

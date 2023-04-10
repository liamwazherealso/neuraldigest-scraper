run:
	poetry run python src/news_ml_scraper/scraper.py
lambda:
	rm -rf dist/lambda dist/lambda.zip
	pip install -t dist/lambda .
	cd dist/lambda && zip -x '*.pyc' -r ../lambda.zip .
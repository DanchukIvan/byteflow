# **Yass - Yet Another Simple Scraper**

Yass is a microframework that makes it easier to retrieve information from APIs and regular websites.

Yass, unlike complex projects like Scrapy or simple libraries like BeautifulSoup, is extremely easy to use due to the unification of the information extraction process and at the same time has quite a wide range of functionality.

## **Why use Yass?**

* :rocket: Yass is built on top of asyncio and asynchronous libraries, which significantly speeds up your code in the context of I/O operations.

* :repeat: With Yass, there is no need to continuously customize the data scraping process. From project to project, you will have a single, transparent architecture.

* ![s3](docs/assets/amazons3.svg)![kafka](docs/assets/apachekafka.svg)![psql](docs/assets/postgresql.svg)![clickhouse](docs/assets/clickhouse.svg) Yass allows you to route data to any backend: s3-like storage, database, network file system, broker/message bus, etc.

* :gear: Yass allows the user to choose what to do with the data: hold it in memory until a certain critical value accumulates, or immediately send it to the backend, perform pre-processing, or leave it as is.

## **Installation**

Installation is as simple as:

`
pip install yass
`

## **Dependencies**

>The list of core Yass dependencies is represented by the following libraries:
>
> * aiohttp
> * aioitertools
> * fsspec
> * more-itertools
> * regex
> * uvloop (for Unix platforms)
> * yarl
> * dateparser

## **More information about the project**

You can learn more about Yass in the [project documentation](https://danchukivan.github.io/yass/), including the API and Tutorial sections. Changes can be monitored in the Changelog section.

## **Project status**

Yass is currently a deep alpha project with an unstable API and limited functionality. Its use in production is **strictly not recommended**.

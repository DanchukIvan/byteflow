
<figure align="center">
    <img src="https://raw.githubusercontent.com/DanchukIvan/byteflows/main/docs/icons/logo.png" alt="byteflows" width="200px">
    <figcaption>
    <a href="https://www.freepik.com/icon/big-data_5316461#fromView=search&page=3&position=30&uuid=d40e1557-77ae-4504-8465-264e0e731318">Icon by Parzival’ 1997</a>
    </figcaption>
</figure>

# **Simple data workflows**

Byteflows is a microframework that makes it easier to retrieve information from APIs and regular websites.

Byteflows, unlike complex projects like Scrapy or simple libraries like BeautifulSoup, is extremely easy to use due to the unification of the information extraction process and at the same time has quite a wide range of functionality.

## **Why use Byteflows?**

* 🚀 Byteflows is built on top of asyncio and asynchronous libraries, which significantly speeds up your code in the context of I/O operations.

* 🔁 With Byteflows, there is no need to continuously customize the data scraping process. From project to project, you will have a single, transparent architecture.

* ![s3](https://raw.githubusercontent.com/DanchukIvan/byteflows/main/docs/img/amazons3.svg) ![kafka](https://raw.githubusercontent.com/DanchukIvan/byteflows/main/docs/img/apachekafka.svg) ![psql](https://raw.githubusercontent.com/DanchukIvan/byteflows/main/docs/img/postgresql.svg) ![clickhouse](https://raw.githubusercontent.com/DanchukIvan/byteflows/main/docs/img/clickhouse.svg) Byteflows allows you to route data to any backend: s3-like storage, database, network file system, broker/message bus, etc.

* ⚙️ Byteflows allows the user to choose what to do with the data: hold it in memory until a certain critical value accumulates, or immediately send it to the backend, perform pre-processing, or leave it as is.

## **Installation**

Installation is as simple as:

`
pip install byteflows
`

## **Dependencies**

>The list of core Byteflows dependencies is represented by the following libraries:
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

You can learn more about Byteflows in the [project documentation](https://danchukivan.github.io/byteflows/), including the API and Tutorial sections. Changes can be monitored in the Changelog section.

## **Project status**

Byteflows is currently a deep alpha project with an unstable API and limited functionality. Its use in production is **strictly not recommended**.

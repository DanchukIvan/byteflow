# **Simple scrape as SELECT * FROM ANYTHING in network**

---

Byteflows is a microframework that makes it easier to retrieve information from APIs and regular websites.

Byteflows, unlike complex projects like Scrapy or simple libraries like BeautifulSoup, is extremely easy to use due to the unification of the information extraction process and at the same time has quite a wide range of functionality.

!!! info "Unstable API"

    Currently, Byteflows is at the beginning of its development - there may still be undetected bugs and frequent changes in interfaces. In this regard, it is highly not recommended to use it in production.

## **Why use Byteflows?**

<div class="grid cards" markdown>

- :material-lightning-bolt:{ .lg .middle } **Asynchronous execution**

    ---

    Byteflows is built on top of asyncio and asynchronous libraries, which significantly speeds up your code in the context of I/O operations.

- :material-content-copy:{ .lg .middle } **Unified architecture**

    ---

    With Byteflows, there is no need to continuously customize the data scraping process. From project to project, you will have a single, transparent architecture.

- :material-hub-outline:{ .lg .middle } **Multiple backend support**

    ---

    Byteflows allows you to route data to any backend: s3-like storage, database, network file system, broker/message bus, etc.

- :fontawesome-solid-gears:{ .lg .middle } **Flexible configuration options**

    ---

    Byteflows allows the user to choose what to do with the data: hold it in memory until a certain critical value accumulates, or immediately send it to the backend, perform pre-processing, or leave it as is.

</div>

## **Installation**

Installation is as simple as:

`
pip install byteflows
`

??? abstract "Dependencies"

    The list of core Byteflows dependencies is represented by the following libraries:

        - aiohttp
        - aioitertools
        - fsspec
        - more-itertools
        - regex
        - uvloop (for Unix platforms)
        - yarl
        - dateparser
        - beartype

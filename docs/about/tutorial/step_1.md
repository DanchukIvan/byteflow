[](){#tutorial-section}

# **Сreate resource**

Let's assume that we are interested in information about income and expenses, balance sheets for a small list of companies. Having visited the sections we need on the FMP website and studied the pattern of link formation, we will configure the resource and requests to it.

## **Create core class**

Declaring the necessary constants:

``` py
from os import getenv
from byteflows.resources.api import ApiResource, EndpointPath, SimpleEORTrigger
from byteflows import EntryPoint

API_KEY: str | None = getenv("FMP_API")
API_URL = "https://financialmodelingprep.com/api/v3"

TICKERS: list[str] = ["AAPL", "MSFT", "AMZN", "NVDA", "TSLA","MA", "PG"]
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0", "Accept-Encoding": "gzip, deflate, br, zstd"}

```

Creating an entry point into the application and register the resource:

??? info "About example"
    You can test Byteflows on a different API and with different parameters of all classes, not limited to what is presented in the Tutorial.

``` py
app = EntryPoint()
fmp_api: ApiResource = app.define_resource(resource_type='api', url=API_URL)
fmp_api.request_timeout = 60
fmp_api = fmp_api.configure(extra_headers=HEADERS, max_batch=2, delay=5, eor_triggers=[SimpleEORTrigger(12)])

```

What did we just do? We declared an API resource, set a maximum timeout for requests associated with this resource, delay and also enriched requests to this resource with the headers we needed.

Let's go through a few key parameters.

**Max_batch** parameter is responsible for how many requests can be sent to a resource at the same time. In this case, this limit is divided between all working data collectors. If we needed to process three parts of the FMP API at the same time, one of the data collectors would be idle until the other one finished its work, since the quota of two requests was used up by the first two fastest data collectors.

And vice versa, if only one request to a resource were created with such a limit, then the only data collector would scoop up the entire available quota and send two requests at a time to the resource.

When setting this parameter, we suggest taking into account the rate limit and restrictions on parallel requests for a specific resource.

The **eor_trigger** parameter is nothing more than an object that detects the end of payload in a resource or the moment at which it is necessary to stop processing the resource even if there is a payload.

At the moment, several trigger options are implemented for the resource API. Sometimes we don’t know how much data we can get from a resource, or we are limited by the number of requests per day, as in our case, so it is possible to combine different triggers. When combining triggers, a pessimistic approach applies - resource processing will stop as soon as at least one trigger fires.

In our example, we use a simple trigger that limits work with a resource by the number of requests to it. Moreover, we mean precisely the number of requests, and not the cycles of sending requests/receiving responses.

Trigger classes do not store information about previous test results. Therefore, if there is some kind of global restriction on a resource, as in our case (limit of 250 requests per day), it needs to be implemented through the totality of all available Byteflows limiting objects (for example, start collecting data only once a day for the entire limit or split the total available amount requests by number of starts and requests).

For our example, we set a limit of 12 requests (1) per run.
{ .annotate }

1. :octicons-alert-16: This configuration is provided for informational purposes only and is in no way calculated through available resource quotas.

## **Registering endpoints**

Let's continue preparing our application and let’s designate the endpoints that we will process in the API.

``` py
income_endpoint: EndpointPath = fmp_api.add_endpoint('income')
income_endpoint.add_fix_part("income-statement", 0)
income_endpoint.add_mutable_parts(TICKERS)

balance_endpoint: EndpointPath = fmp_api.add_endpoint('balance')
balance_endpoint.add_fix_part('balance-sheet-statement')
balance_endpoint.add_mutable_parts(TICKERS)

```

Endpoint objects allow us to form sort of “subsections” of a resource, like chapters in books, and are needed to correctly expand the base url of a resource. Subsequently, a request object will be created for each such endpoint.

As you can see from the names of the methods, we have added a fixed part of the expanded url - it will always be the same when generating links - and a variable part, which will change after the query filed is exhausted for each variable part.

The placement of link parts in Endpoint objects can be controlled using the optional prior parameter. If we need the part of the link to be the very first, we will set its prior to one.

You can see how many sections are registered in the endpoint using the last_prior attribute.

``` py
balance_endpoint.add_mutable_parts(TICKERS, prior=1)
print(balance_endpoint.last_prior) # Display 1.

```

It looks like we are done with the formation of the resource, now let’s move on to an equally important part, namely the formation of the I/O environment.

*[FMP]: Financialmodelingprep

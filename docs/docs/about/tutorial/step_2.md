# **Input and output**

Since we are working with a resource to extract data, it would not hurt us to formulate answers to several questions:

- what types of data we process;
- how we process data;
- where will we store them and whether we will store them at all;
- will we somehow additionally process the data.

And the answers to most of the questions are provided by the **contentio** module and the concept of I/O contexts. In the practical part, let’s not get into the details of the Yass architecture and let’s get down to business.

## **Registering data formats**

To process any data, we need to define its format and handler functions, which we will do now. We know that the FMP API gives us data in json format. We will save the data in csv format.

``` py
from yass import contentio
import polars as pl

contentio.create_datatype(
    format_name="json",
    input_func=pl.read_json,
    output_func=pl.DataFrame.write_json,
)

contentio.create_datatype(
    format_name="csv",
    input_func=pl.read_csv,
    output_func=pl.DataFrame.write_csv,
)

```

Using the create_datatype module-level function, we declared two data types - csv and json - and also assigned handler functions to them using the wonderful Polars library. Thanks to Polars, we can seamlessly convert json data to csv, since both data types are schema-dependent and can be described in terms of a dataframe.

!!! info "About data types and their handlers"
    **The system of data types and their processing is the weakest part of the Yass API**. At the moment, it is necessary to explicitly declare types and handlers assigned to them, but this creates the risk of impossibility of converting structured and semi-structured data from one format to another, despite the fact that they are all schema-dependent and have explicit polymorphism, and also requires the introduction of adapter objects between all compatible data types.

    The most likely solution will be to create data type presets in Yass and use universal libraries like Polars for structured data and Pillow type for blob-like data. Subsequently, the user will not have to manage the composition of data types and their handlers, which will also relieve him of responsibility for the backward compatibility of handlers.

## **Registration of storage**

Now we will determine where we will store the data. We will choose S3 object storage. Let's cook it.

``` py

S3_KWARGS = {
    "endpoint_url": getenv("S3_ENDPOINT"),
    "key": getenv("S3_KEY"),
    "secret": getenv("S3_SECRET"),
    "client_kwargs": {
      'region_name': getenv("S3_REGION")
   }
}

storage: FsBlobStorage = app.define_storage(storage_type="blob")
storage = storage.configure(
    engine_proto="s3",
    engine_params=S3_KWARGS,
    bufferize=True,
    limit_type="count",
    limit_capacity=3,
)
```

In this piece of code, we received the parameters we needed to initialize the storage and configured it. Let's also go over some parameters.

**Engine_proto** is the key under which this or that engine of a certain storage class is registered. Under the hood of the storage classes, a factory is triggered that returns a storage engine object, through which a connection is established with the actual backend and all sorts of manipulations are carried out with it. If the engine is not registered, we will receive the corresponding error.

*[backend]: By backend we mean anything that can process data: databases like Postgres, MySQL, Clickhouse, caches like Redis, KV DB or message buses like Kafka, RabbitMQ, NATS.
*[engine]:
    To find out which engine implementations are available, you can call the registred_types property on storage classes.

**Bufferize** is a switch - if True, then all data will be pre-buffered in memory and unloaded only when the set limit is exceeded. Otherwise, the data will be immediately transferred to the backend, regardless of whether some kind of limit is set or not.

**The Limit Type** is nothing more than the key by which one or another Limit class is registered. These are special triggers that determine the moment of uploading data to the backend according to some criterion, for example, the number of elements in the data collector buffer, memory occupied by the data. Again, a factory is triggered under the hood, which, having detected the required implementation using the key, returns an object.

??? note "About polymorphism in Yass"
    At the moment, users cannot register their own limit types and storage engines; this is an internal Yass implementation detail and will most likely not be available in public in the future. If your practice has some special use case and the current capabilities of Yass are not enough, you can open an issue for its creation.

## **Creating context objects**

Finally, we can define I/O contexts for each scheduled query.

``` py

income_context: contentio.IOContext = contentio.create_io_context(
    in_format="json", out_format="csv", storage=storage
)

balance_context: contentio.IOContext = contentio.create_io_context(
    in_format="json", out_format="csv", storage=storage
)

```

Theoretically, if we are:

- not going to transform the original data;
- keys to be stored in memory for all requests to a resource will be formed according to the same pattern;
- data will be uploaded to the same backend

then we don't need to register multiple contexts. However, our history will develop according to a different scenario.

## **Augmenting the context object**

IOContext objects are key during the data processing phase. In addition to the fact that with their help we record what we are going to receive, what to return and where to store it, they also allow us to formulate a pattern for forming a path for storing data (or, for example, table names if we are going to enter data into the database) and enrich/ modify data before it is stored in memory or flushed to the backend.

As a matter of fact, we are now mapping out the path formation pattern behind each context, and also designing a pipeline for one of the contexts.

### Path generator

First, we need to create path generator objects. These objects represent a kind of collection that processes PathSegment objects. These segments are nothing more than parts of what we will end up with.

!!! warning
    When forming paths/tables, etc. you need to consider the storage structure of the data destination. Registering redundant segments may result in a level of nesting that is not supported by the end backend.

Well, just do it.

``` py

i_pathgenerator: contentio.PathTemplate = income_context.attache_pathgenerator()
i_pathgenerator.add_segment("_", 1, ["fmp_api"])
i_pathgenerator.add_segment("_", 2, ["income", date.today, time])
i_pathgenerator.add_segment("_", 3, ["test"])

b_pathgenerator: contentio.PathTemplate = balance_context.attache_pathgenerator()
b_pathgenerator.add_segment("_", 1, ["fmp-api"])
b_pathgenerator.add_segment("_", 2, ["balance", date.today, time])
b_pathgenerator.add_segment("_", 3, ["test"])

```

So what's going on here:

- we attached a path generator to each context object (if it goes down one level - keys for mapping);
- to each path generator we add segments: the first is the name of the resource, the second is the short name of the endpoint, the current date and timestamp, the third is the string “test”.

As you may have noticed, the number of elements in one segment is not limited and an element can be a callable object.

- the dash in the very first argument is a concatenator. It is this symbol that will be used to combine elements of one segment;
- we also use the second argument to indicate the order in which the segments will be arranged.

!!! note
    In theory, the order of segments can be fixed simply by adding segments in the desired order. However, if we first generate temporary keys, and then modify the path generator on the fly in the pipeline to obtain the final storage path, then the priority argument allows us to make such an operation much more flexible and faster using existing tools without writing additional logic.

    This note applies to all Yass entities whose constructors and methods take as an argument a priority value or something similar to that value. Probably in future releases in some cases such arguments will be excluded or the functionality using these arguments will be changed.

You can check which line will be rendered with the given parameters as follows:

``` py

i_pathgenerator.render_path()

```

### Charging pipelines

Suppose among the information related to company income in the FMP API, we only need identifying fields, coefficients, and we also need to add our own custom indicator coefficient. And additionally, we will add the record creation date for each row. The tasks of data conversion and object modification are solving in Yass using pipelines.

The essence of how pipelines work is simple - it registers a handler function and applies it to incoming data, which is in serialized form before uploading to the backend. All handlers run in a separate thread and do not block the Yass execution thread.

??? info "IO bound and CPU bound tasks"
    Yass's asynchronous capabilities and Python's concurrency model are great for I/O and small CPU workloads. However, if you need to implement CPU-heavy processing of tasks, you should use a non-standard solution in your pipelines, which can be guaranteed to execute the task on a separate core without connection with the GIL. A large number of CPU tasks alone will significantly reduce the performance of Yass only due to the nature of Python working in a multiprocessor environment.

We implement everything previously said in simple code.

``` py

from polars import DataFrame

income_pipeline: contentio.IOBoundPipeline = income_context.attache_pipeline()

@income_pipeline.step(1)
def append_interest_rate_ratio(data: DataFrame) -> DataFrame:
    new_frame = data.with_columns(
        (pl.col("interestExpense") / pl.col("revenue") * 100).round(2).alias("InterestRate")
        )
    return new_frame

@income_pipeline.step(2)
def get_only_income_ratio(data: DataFrame) -> DataFrame:
    needed_fields = ["date", "symbol", "fillingDate", "calendarYear", "period", "grossProfitRatio", "operatingIncomeRatio", "netIncomeRatio", "InterestRate"]
    new_frame = data.select(needed_fields)
    return new_frame

@income_pipeline.step(3)
def insert_metadata(data: DataFrame) -> DataFrame:
    created_at = pl.Series(
        "created_at", [datetime.now().date()] * data.shape[0], pl.Date
    )
    new_frame = data.insert_column(data.shape[1], created_at)
    return new_frame

income_pipeline.show_pipeline()

```

Our pipeline is ready. In pipelines we can carry out any operations, including compressing data into one object, completely changing the structure of a data object (for example, parsing a downloaded doc file, wrapping the necessary data in json and putting this json in the buffer instead of the original object), receiving additional data from other services and merge them with the original object. Everything is limited only by your imagination, needs and serialization capabilities of the functions and tools you use.

??? warning "Mmm, why are there steps here?"
    Unfortunately, pipelines are also a weak part of the Yass API. There are several problems at once: this is access to global objects if you need to interact with them, and the risk of a large overproduction of threads, and there is no state storage. In the future, it is planned to eliminate these shortcomings, including the ability to save the state of the pipeline when errors occur and reload it from where it stopped in runtime.

    This is part of a lot of work to come and after hammering away at the simpler components of Yass, the code and functionality of the pipelines will be polished. The step method is a tiny piece of the future API.

Now the final part remains. Let's get to it.

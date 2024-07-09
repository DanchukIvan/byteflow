# **Yass Components**

---

This page provides brief definitions of Yass components to provide a better understanding of exactly how Yass works under the hood.

Although Yass is made up of many parts—main classes, service classes, utilities, and functions—only a few of them form the foundation of a working application.More detailed information about each component can be found in the [Tutorials](../about/tutorial/step_1.md) section or in the [API description](../api/about_section.md#api-section-start) of each module.

## **Resources**

Key component. A [resource](../api/api_resources.md#implementations) is nothing more than a data source located on the network. This could be a third-party API or an API owned by your organization, or simply a website that hosts the information you need.

??? warning "Stay ethical"
    The Yass developer does not encourage or condone attempts to borrow information from a resource against the will of the resource owner. Therefore, if in the process of scraping data you encounter powerful resistance from the source, **I strongly recommend that you think about whether you are doing something that is acceptable to the owner of the resource and whether it is worth the potential damage for both parties**.
!!! info "Functionality limitation"
    Currently only working with the [API](../api/api_resources.md#working-with-api) is available. More uses of Yass will become available in the future.

As a matter of fact, if you do not declare a Resource in the code, then no work will begin :) The resource, as a central entity, performs the following functions:

- fixes the entry point to the data source and stores authorization information;
- allows you to create queries to a data source and stores information about them;
- defines restrictions on the data source, such as the number of requests per second, the minimum delay between requests, the maximum time to wait for a response;
- stores triggers that allow you to interrupt resource processing based on specified criteria.

## **Requests**

A lower level component that is directly involved in the information retrieval process. If we generalize the functionality of the [Request](../api/api_resources.md#implementations) classes, we can say that at the level of each specific request the following is determined:

- how to generate links to send a network request, thereby defining the data source section that Yass will work with;
- what input data we expect to receive and in what format we plan to upload it;
- where we want to store the data obtained as a result of the execution of this request;
- whether additional processing of this request is needed.

Thus, top-level information about how to work with a data source is recorded in the Resource class, and more applied details, without which it is impossible to correctly extract data from the source, are defined in Request.

Several entities are also associated with Requests (I/O context, pipelines), which are service entities in relation to it, and therefore more detailed information about them is provided in other sections of the documentation.

*[documentation]: Tutorials or API reference

## **Data collectors**

[Data collectors](../api/api_data_collectors.md#implementations) are the workhorses that pull links from Requests, retrieve data from the source, wait for the data to be converted, and pass it on for storage/further transmission. For each request, one data collector is created, which operates concurrently in the event loop.

The data collector's work begins by waiting for the current time to be scheduled to work with the resource. Once the data collector has started, it repeats the 'wait-receive' loop endlessly until an unexpected error occurs or the user cancels the corresponding instance of the class (1).
{ .annotate }

1. :material-wrench-clock-outline: There is currently no way to stop data collectors unless an unexpected error occurs. The first stable version of Yass will feature a cli interface that allows you to selectively stop the selected data collector.

Typically, you won't have to instantiate data collectors yourself, since they are generated automatically by the application class when the application starts. However, in the future it is planned to add an interface with the ability to receive an instance of the created data collector.

## **Storages**

The repository concludes the overview of Yass core components.

The [storage](../api/blob_storage.md#implementations) ensures the transfer of data to the selected backend, and the backend can be anything - persistent storage or some kind of message broker, data processing platform. The main role of this entity is to summarize information about registered backends and provide unified interfaces for working with them. Inextricably linked with storage is the concept of an engine - this is the object through which Yass establishes a connection with the backend and transmits data. The essence of the engine is that it transfers a similar object well to SqlAlchemy.

In other words, Storage is an interface to storage deployed outside the Yass loop.

Although this family of classes has public interfaces, you don't actually have to use them when using Yass - they are all baked into template methods of other classes involved in data scraping, and are applied "automatically". These interfaces can be useful if you plan to combine Yass with other solutions within your architecture.

## **What's next?**

Once you are familiar with the key components, it is best to study the Study Guide. It clearly shows the process of preparing an application to work with the **[Financialmodelingprep API](https://site.financialmodelingprep.com/)**. Thanks to them for the opportunity to provide a more meaningful example to Yass users[^1]!

!!! info "Something else about working with FMP"
    FMP also has its own SDK for working with their API. In case you are interested in working with this resource, it is [here](https://pypi.org/project/fmpsdk/).

*[FMP]: Financialmodelingprep

[^1]: Financialmodelingprep provides financial information about public companies via an API that is free up to 250 calls per day.

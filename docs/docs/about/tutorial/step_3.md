# **Creating and run requests to a resource**

In general, all that’s left to do is create requests and schedule them. Let's get started.

``` py
from yass.scheduling import TimeCondition

time_interval_income: ActionCondition = TimeCondition(
    period=1, start_time="11:22", end_time="22:00", frequency=0
)
time_interval_balance: ActionCondition = TimeCondition(
    period=1, start_time="11:22", end_time="22:00", frequency=0
)
```

We have just declared two timers. Timers belong to the trigger family that are used in the active part of Yass.
Let's talk about what will happen based on the current timer parameters: our activity starts at 11.22, every day (since we transmitted one) and once a day (since the frequency parameter is set to zero); the time after which the waiting period begins is 22.00. If we launch our application after 22.00, it will wait until the next day to start crawling the resource.

If we wanted scheduling to be carried out by days of the week, then we would need to pass a tuple with the required days of the week in ISO format (numbers from 1 to 7).

??? info "Expected API changes"
    In future releases, it is planned that scheduling will be set at the request class level through the appropriate method, without the need to import the class from the module. This is also the part of the Yass API that will be expanded - there will be new active triggers that allow you to flexibly organize the launch of data collectors not only by time and dates, but also by other criteria.

Actually, we are all ready to create request objects and start collecting data.

``` py
report_params = {"period": "annual", "limit":5, "apikey": API_KEY}

income_query: ApiRequest = fmp_api.make_query(
    "income_statement",
    "income",
    fix_params=report_params,
    collect_interval=time_interval_income,
    io_context=income_context,
    has_pages=False,
)

balance_query: ApiRequest = fmp_api.make_query(
    "balance_sheet",
    balance_endpoint,
    fix_params=report_params,
    collect_interval=time_interval_balance,
    io_context=balance_context,
    has_pages=False,
)

app.run()

```

After processing requests is complete, we can see what we have in storage using module-level functions.

``` py
from yass.storages import blob

content_list = blob.ls_storage(storage.engine, "fmp-api")
print(content_list)
content = blob.read(storage.engine, content_list[0])
print(content)

```

That's all for now. I hope everything worked out for you ;)

from io import BytesIO

from pandas import DataFrame, read_csv

io_mapping = {}


# TODO: нужна валидация что это обычная функция
# TODO: нужно описать важные дополнительные параметры (сжатие, движки, маппинг) в сигнатуре функции
# TODO: нужно понять как оптимизировать систему записи файлов - некоторые форматы являются текстовыми
# и нужен универсальный метод избежать сложной логики при отсутствии потери производительности
# FIXME: заголовки файлов почему-то пишутся в середине файла - нужно посмотреть что за хуйня
def dispatch_io(*output_fmt):
    def io_func(func):
        for fmt in output_fmt:
            io_mapping[fmt] = func
        return func

    return io_func


@dispatch_io("csv")
def csv(data):
    headers = None
    for item in data:
        headers = list(item.keys())
        break
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_csv(buf, header=headers, index=False, encoding="utf-8")
    buf.seek(0)
    return set(buf)


@dispatch_io("xlsx", "xls")
def excel(data):
    headers = None
    for item in data:
        headers = list(item.keys())
        break
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_excel(buf, index=False, header=headers)
    buf.seek(0)
    return set(buf)


@dispatch_io("json")
def json(data):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_json(buf)
    buf.seek(0)
    return set(buf)


# TODO: посмотреть какие есть сторадж опшены для извлечения


@dispatch_io("parquet")
def parquet(data, partitions=None):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_parquet(buf, partition_cols=partitions)
    buf.seek(0)
    return set(buf)


@dispatch_io("feather")
def feather(data):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_feather(buf)
    buf.seek(0)
    return set(buf)


if __name__ == "__main__":
    data = [{"data": 10500, "name": "price", "location": "Ukhta"}]
    data_2 = [
        {"data": 10500, "name": "price", "location": "Ukhta"},
        {"data": 20500, "name": "price", "location": "Novgorod"},
    ]

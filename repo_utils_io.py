from pandas import DataFrame, read_csv
from io import BytesIO
from difflib import SequenceMatcher

io_mapping = {}


# TODO: нужна валидация что это обычная функция
# TODO: нужно описать важные дополнительные параметры (сжатие, движки, маппинг) в сигнатуре функции
def dispatch_io(*output_fmt):
    def io_func(func):
        for fmt in output_fmt:
            io_mapping[fmt] = func
        return func
    return io_func


@dispatch_io('csv')
def csv(data):
    headers = None
    for item in data:
        headers = list(item.keys())
        break
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_csv(buf, header=headers, columns=headers, index=False)
    buf.seek(0)
    return set(buf)


@dispatch_io('xlsx', 'xls')
def excel(data):
    headers = None
    for item in data:
        headers = list(item.keys())
        break
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_excel(buf, index=False, header=headers, columns=headers)
    buf.seek(0)
    return set(buf)


@dispatch_io('json')
def json(data):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_json(buf)
    buf.seek(0)
    return set(buf)


# TODO: посмотреть какие есть сторадж опшены для извлечения

@dispatch_io('parquet')
def parquet(data, partitions=None):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_parquet(buf, partition_cols=partitions)
    buf.seek(0)
    return set(buf)


@dispatch_io('feather')
def feather(data):
    df = DataFrame.from_records(data)
    buf = BytesIO()
    df.to_feather(buf)
    buf.seek(0)
    return set(buf)


data = [{'data': 10500, 'name': 'price', 'location': 'Ukhta'}]
data_2 = [{'data': 10500, 'name': 'price', 'location': 'Ukhta'},
          {'data': 20500, 'name': 'price', 'location': 'Novgorod'}]

buf_1 = BytesIO()
buf_1.writelines(csv(data))
buf_1.seek(0)
buf_2 = BytesIO()
buf_2.writelines(csv(data_2))
buf_2.seek(0)
mnogestvo = set(buf_2)
delta = mnogestvo.symmetric_difference(set(buf_1))
print(delta)
# buf_3 = BytesIO()
# buf_3.writelines(new_setup)
# print(buf_3.getvalue())

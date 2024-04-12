from __future__ import annotations

from ast import literal_eval
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import jmespath
import pandas as pd
from polars import DataFrame
from yass import EntryPoint, contentio
from yass.resources import MaxPageEORTrigger
from yass.scheduling import TimeCondition

if TYPE_CHECKING:
    from yass.resources import ApiRequest, ApiResource
    from yass.scheduling.base import ActionCondition
    from yass.storages.blob import FsBlobStorage


headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Bearer APPLQOF89F6EC73OPSF8P75TB39VA0PNP624S3DO42MT8298OLM4E5991N06A1OC",
}
url = "https://api.hh.ru/vacancies"
s3_kwargs = {
    "endpoint_url": "https://storage.yandexcloud.net/",
    "key": "YCAJE2ch9FookEYy7zu5No3Us",
    "secret": "YCP96xrQlCajcbliwyGR1d2wnkae1Z6W-gs4aDO6",
}


def prepare_area_lst(slicer: tuple[int, int] | None = None) -> list[int]:
    if Path("areas.txt").exists():
        with open("areas.txt") as file:
            txt = file.read()
            return literal_eval(txt)
    population_df = pd.read_csv(
        "https://raw.githubusercontent.com/hflabs/city/master/city.csv"
    )
    # Нужно будет сделать более гибкие параметры выбора населения.
    # Хотя все "улучшения" делать нужно все таки после первой MVP версии.
    population_df = population_df[population_df["population"] > 800000]
    population_df = population_df[["address"]]
    population_df["address"] = population_df["address"].str.extract(
        r"(?<=г )([\w\- ]+)"
    )
    fmt_lst = [
        f"@.name == '{v}'"
        for v in population_df["address"]
        if isinstance(v, str)
    ]
    pattern_area_ids = "||".join(fmt_lst)
    area_pattern = jmespath.compile(
        f"[0].areas[*].areas[?{pattern_area_ids}].id[]"
    )
    area_pattern2 = jmespath.compile(f"[0].areas[?{pattern_area_ids}].id[]")
    with httpx.Client() as client:
        res = client.get("https://api.hh.ru/areas", headers=headers)
        areas: list[int] = area_pattern.search(res.json())
        areas2 = area_pattern2.search(res.json())
        areas.extend(areas2)
        with open("areas.txt", "+w", encoding="utf-8") as file:
            file.write(str(areas))
        if slicer is not None:
            return areas[slice(*slicer)]
        return areas


schema = dict()
schema["url"] = "alternate_url"
schema["city"] = "area.name"
schema["salary"] = "salary.from"
schema["published_at"] = "published_at"
schema["req_skills"] = "snippet.requirement"
schema["experience"] = "experience.name"

from datetime import datetime

import orjson
import polars as pl
from regex import regex

# TODO: нужно сделать обработку сигналов ОС, в том числе отмену с клавы.
app = EntryPoint()

# TODO: все таки нужны хэндлеры для интервалов времени, чтобы можно было вводить дату и время в строковом виде.
# TODO: поля нужно сделать kw, иначе можно запутаться что и как. Плюс поле (one_run) скрыть из сигнатуры.
# TODO: нужно сделать стратегии на выбор - edger_start (запускаемся сразу несмотря на лаг), in_time (выравниваем время и ждем следующего запуска)
# TODO: некорректно работает сдвиг лага - нужно посмотреть возможно снова ошибка в логике. Сейчас вроде считает, но делает смещение больше или меньше нужного интервала.
time_interval_f: ActionCondition = TimeCondition(
    period=(1, 3, 5, 7), start_time="11:22", end_time="22:00", frequency=2
)
time_interval_s: ActionCondition = TimeCondition(
    period=1, start_time="11:23", end_time="22:00", frequency=2
)
eor = MaxPageEORTrigger(
    search_area="content", current_page_field="page", max_page_field="pages"
)


@eor.set_content_handler
def get_fields(response):
    jsoned: dict = orjson.loads(response)
    return jsoned


# TODO: нужно сделать так, чтобы Репо отстреливало старт если нет ни одного контекста. То есть нужно сделать свою версию defaultdict, которая в режиме редактирования
# позволяет создавать произвольные ключи. Или просто возвращать МаппингПрокси, который запрещает редактирование экземпляров.
resource: ApiResource = app.define_resource(resource_type="api", url=url)
resource = resource.configure(
    extra_headers=headers, max_batch=3, delay=1, eor_triggers=[eor]
)
# TODO: нужно предусмотреть стратегии сброса данных - по циклу или по памяти или сразу. Также нужно дать возможность создавать резервную копию данных на диске на фоне.
# TODO: нужно предусмотреть стратегию что делать с совпадающими файлами - перезаписывать их, добавлять новый файл с префиксом или аппендить. Для SQL движков это работать не должно.
# TODO: нужно сделать возможность синхронного и асинхронного чтения, записи и т.д. из функций уровня модуля. Не всегда нужно будет асинхронное взаимодействие.
storage: FsBlobStorage = app.define_storage(storage_type="blob")
storage = storage.configure(
    engine_proto="s3",
    engine_params=s3_kwargs,
    bufferize=True,
    limit_type="count",
    limit_capacity=10,
)
areas_map: dict[str, list[int]] = {"area": prepare_area_lst()}
search_string: dict[str, str] = {"text": "data+engineer"}
# TODO: сделать make_part служебной функцией
# TODO: сложно че-то заполнять, нужно автоматом генерить имена шаблонов, а закидывать списки чего-то в качестве значений аттрибутов

# TODO: нужно однозначно делать пресеты - замучаешься фигачить все типы данных нужные
# TODO: исключить оборачивание в DataProxy - так проще будет работать с данными - но оставить классы, чтобы потом изучить как можно
# при работающей проверке типов эффективно работать с прокси-объектами.
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
# TODO: нужно сделать проверку на поле привязки - все таки будем привязывать только к запросам. Можно ещё к ресурсам, но тогда нужно думать
# про диспетчеризацию объектов.
context: contentio.IOContext = contentio.create_io_context(
    in_format="json", out_format="csv", storage=storage
)

query_f: ApiRequest = resource.make_query(
    "hh1.ru",
    persist_fields=search_string,
    mutable_fields=areas_map,
    collect_interval=time_interval_f,
    io_context=context,
    has_pages=True,
)
query_s: ApiRequest = resource.make_query(
    "hh2.ru",
    persist_fields=search_string,
    mutable_fields=areas_map,
    collect_interval=time_interval_s,
    io_context=context,
    has_pages=True,
)
pipeline: contentio.IOBoundPipeline = context.attache_pipline()


# TODO: нужно проверять сигнатуру функции на совместимость в типом возврата в функции десереализации в контексте
# TODO: нужно логировать старт и окончание пайплайна
# TODO: нужно сделать проверку на то, что номер шага минимум - единица
# TODO: нужно сделать __setitem__ для поиска среди функций и их редактирования (например, замены функции на другую) или метод replace
# TODO: нужно автоматически присваивать шагу имя функции для целей логирования.
# TODO: нужно дать возможность навешивать эксепшн хэндлеры на пайплайны
@pipeline.content_filter
def empty_content(data: DataFrame) -> bool:
    try:
        data = data.select(pl.col("items").explode()).unnest("items")
    except Exception:
        return False
    else:
        return True


@pipeline.step(1)
def parse_schema(data: DataFrame) -> DataFrame:
    print(f"Start pipeline {parse_schema.__name__}")
    data = data.select(pl.col("items").explode()).unnest("items")
    condition_filter = regex.compile(r"\b(?=\[)\b|\b(?=\.).\b")
    for alias, json_path in schema.items():
        json_path: str
        key_path = condition_filter.split(json_path, 0)
        if len(key_path) < 2:
            key, path = key_path[0], key_path[0]
        else:
            key, path = key_path[0], key_path[1]
        if data.schema[key] == pl.Struct:
            data = data.with_columns(
                pl.col(key).struct.json_encode().alias(alias)
            )
            data = data.with_columns(
                pl.col(alias).str.json_path_match(f"$.{path}")
            )
        else:
            data = data.with_columns(pl.col(key).alias(alias))
    data = data.select(pl.col(schema.keys()))
    data = data.with_columns(
        pl.col("published_at").str.to_datetime().cast(pl.Date),
        pl.col("salary").cast(pl.Float64),
    )
    data = data.fill_null(0)
    print(f"End pipeline {parse_schema.__name__}")
    print(data.head(3))
    return data


@pipeline.step(2)
def insert_metadata(data: DataFrame) -> DataFrame:
    created_at = pl.Series(
        "created_at", [datetime.now().date()] * data.shape[0]
    )
    data = data.insert_column(data.shape[1], created_at)
    return data


from yass.storages import read

__all__ = [
    "empty_content",
    "get_fields",
    "insert_metadata",
    "parse_schema",
    "prepare_area_lst",
]

data = read(
    storage.engine,
    r"api.hh.ru/hh1.ru/2024-04-11_hh1.ru_1712845920.6408794.csv",
)
print(data)

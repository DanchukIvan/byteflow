from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

import httpx
import jmespath
import pandas as pd
from polars import DataFrame, Series, Time

from exceptions import EndOfResource
from yass import Yass

if TYPE_CHECKING:
    from resources import ApiResource, ResourceRequest
    from scrapers import ApiScraper

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

from datetime import datetime, time

import polars as pl
from regex import Pattern, regex

import contentio
from repo import RepoManager
from resources import ApiResource, ResourceRequest
from scrapers import ApiScraper

# TODO: нужно сделать обработку сигналов ОС, в том числе отмену с клавы.
app = Yass()
# TODO: нужно сделать так, чтобы Репо отстреливало старт если нет ни одного контекста. То есть нужно сделать свою версию defaultdict, которая в режиме редактирования
# позволяет создавать произвольные ключи. Или просто возвращать МаппингПрокси, который запрещает редактирование экземпляров.
scraper: ApiScraper = app.make_crawl(url, crawl_type="api")
scraper.extra_headers = headers
# TODO: нужно предусмотреть стратегии сброса данных - по циклу или по памяти или сразу. Также нужно дать возможность создавать резервную копию данных на диске на фоне.
# TODO: нужно предусмотреть стратегию что делать с совпадающими файлами - перезаписывать их, добавлять новый файл с префиксом или аппендить. Для SQL движков это работать не должно.
# TODO: нужно сделать возможность синхронного и асинхронного чтения, записи и т.д. из функций уровня модуля. Не всегда нужно будет асинхронное взаимодействие.
storage: RepoManager = scraper.storage
resource: ApiResource = scraper.resource
rquery: ResourceRequest = resource.get_request(name="de_vacancy")
areas_map = {"area": prepare_area_lst()}
rquery.set_mutable_field(areas_map)
search_string = {"text": "data+engineer"}
rquery.set_persist_field(search_string)
# TODO: сделать make_part служебной функцией
# TODO: сложно че-то заполнять, нужно автоматом генерить имена шаблонов, а закидывать списки чего-то в качестве значений аттрибутов
filepart = {"part": rquery.part_name}
folderpart = {"part1": datetime.now().date}
rootpart = {"part2": "test-new-prog"}

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
context = contentio.create_io_context(
    target=rquery,
    in_format="json",
    out_format="csv",
    storage_proto="s3",
    storage_kwargs=s3_kwargs,
)
context.path_temp = contentio.PathTemplate(
    filepart, folder_part=folderpart, root_part=rootpart, delim="-"
)


# TODO: нужно проверять сигнатуру функции на совместимость в типом возврата в функции десереализации в контексте
# TODO: нужно логировать старт и окончание пайплайна
# TODO: нужно сделать проверку на то, что номер шага минимум - единица
# TODO: нужно сделать __setitem__ для поиска среди функций и их редактирования (например, замены функции на другую) или метод replace
# TODO: нужно автоматически присваивать шагу имя функции для целей логирования.
# TODO: нужно дать возможность навешивать эксепшн хэндлеры на пайплайны
@context.wrap_pipeline.step(1)
def parse_schema(data: DataFrame) -> DataFrame:
    print(f"Start pipeline {parse_schema.__name__}")
    data = data.select(pl.col("items").explode()).unnest("items")
    condition_filter: Pattern[str] = regex.compile(r"\b(?=\[)\b|\b(?=\.).\b")
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
                pl.col(alias).str.json_path_match(f"$.{path}").alias(alias)
            )
        else:
            data = data.with_columns(pl.col(key).alias(alias))
    data = data.select(pl.col(schema.keys()))
    data = data.with_columns(
        pl.col("published_at").str.to_datetime().cast(pl.Date),
        pl.col("salary").cast(pl.Int16),
    )
    data = data.fill_null(0)
    print(f"End pipeline {parse_schema.__name__}")
    return data


@context.wrap_pipeline.step(2)
def insert_metadata(data: DataFrame) -> DataFrame:
    created_at = pl.Series(
        "created_at", [datetime.now().date()] * data.shape[0]
    )
    data = data.insert_column(data.shape[1], created_at)
    return data


@context.wrap_pipeline.step(3)
def merge(data: DataFrame) -> DataFrame:
    # TODO: доступ к очередям нужно закрыть потоковым локом
    queue: dict[str, DataFrame] = scraper.storage.queques[id(context)]
    new_data: DataFrame = data
    if queue:
        for path, dataobj in queue.items():
            if sorted(data.columns) == sorted(dataobj.columns):
                new_data = new_data.extend(dataobj)
    final_data: DataFrame = new_data.filter(data.is_unique())
    return final_data


# TODO: нужно тестить функцию на предмет наличия возврата EOR
@context.wrap_pipeline.eorsignal
def next_url(data: DataFrame) -> DataFrame:
    try:
        target_data: Series = data["items"]
        has_data = target_data.explode().struct.unnest()
        return data
    except Exception:
        raise EndOfResource


from test_new_timecond import TimeCondition

# TODO: все таки нужны хэндлеры для интервалов времени, чтобы можно было вводить дату и время в строковом виде.
# TODO: поля нужно сделать kw, иначе можно запутаться что и как. Плюс поле (one_run) скрыть из сигнатуры.
# TODO: нужно сделать стратегии на выбор - edger_start (запускаемся сразу несмотря на лаг), in_time (выравниваем время и ждем следующего запуска)
# TODO: некорректно работает сдвиг лага - нужно посмотреть возможно снова ошибка в логике. Сейчас вроде считает, но делает смещение больше или меньше нужного интервала.
time_interval = TimeCondition(1, time(8, 0), time(20, 00), frequency=4)
# TODO: нужно немного изменить логику триггеров - нужно ввести класс EdgerStart, который сразу запускает функцию на исполнение и будет классом по умолчанию.
# Плюс регистрировать триггеры возможно как-то иначе, чтобы регилось не конкретное условие, а сам дескриптор.
scraper.scrape.setup_trigger(time_interval)
# TODO: нужно предусмотреть тестовый старт приложения - чтобы обойти разок ресурс и проверить работоспособность. Может даже отчет какой-то сформировать, а потом и превью.

if __name__ == "__main__":
    app.run()

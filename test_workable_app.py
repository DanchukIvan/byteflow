import httpx
import jmespath
import pandas as pd

from repo import NetworkRepo
from resources import ApiResource, QueryString
from schemas import JsonField, JsonSchema
from scrapers import ApiScraper
from yass import Yass

# TODO: нужно понять как перехватывать ошибки чтобы они не уводили прогу в рекурсию
# иначе получается очень топорно

url = "https://api.hh.ru/vacancies"
s3_kwargs = {
    "endpoint_url": "https://storage.yandexcloud.net/",
    "key": "YCAJE2ch9FookEYy7zu5No3Us",
    "secret": "YCP96xrQlCajcbliwyGR1d2wnkae1Z6W-gs4aDO6",
}
n = NetworkRepo("csv", "s3", engine_kwargs=s3_kwargs, url=url)

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Bearer APPLQOF89F6EC73OPSF8P75TB39VA0PNP624S3DO42MT8298OLM4E5991N06A1OC",
}
scr = ApiScraper(extra_headers=headers, url=url)

r = ApiResource("https://api.hh.ru/vacancies", delay=2)

q = QueryString(
    "data_engineer",
    persist_fields={
        "text": "data+engineer+AND+python",
        "search_period": 5,
        "per_page": 100,
    },
    url=url,
)


def prepare_area_lst(slicer: tuple[int, int] = None) -> list[int]:
    population_df = pd.read_csv(
        "https://raw.githubusercontent.com/hflabs/city/master/city.csv"
    )
    # Нужно будет сделать более гибкие параметры выбора населения.
    # Хотя все "улучшения" делать нужно все таки после первой MVP версии.
    population_df = population_df[population_df["population"] > 800000]
    population_df = population_df[["city"]]
    fmt_lst = [
        f"@.name == '{v}'"
        for v in population_df["city"].to_list()
        if isinstance(v, str)
    ]
    pattern_area_ids = "||".join(fmt_lst)
    area_pattern = jmespath.compile(
        f"[0].areas[*].areas[?({pattern_area_ids})].id[]"
    )
    with httpx.Client() as client:
        res = client.get("https://api.hh.ru/areas")
        areas: list[int] = area_pattern.search(res.json())
        if slicer:
            return areas[slice(*slicer)]
        return areas


# TODO: перед запуском приложение должно попробовать сбиндить все аргументы, так как некоторые параметры
# могут быть дополнены без инициализации новых классов. Вообще стоит подумать нужно ли вызывать биндинг в метаклассе.
# TODO: нужно чтобы каждый раз каждый класс напоминал какие обязательные аттрибуты осталось установить чтобы приложение запустилось.
# Это должен быть общий объект, наверное лучше всего реализовать это в контексте.
areas = prepare_area_lst()
q.set_mutable_field({"area": areas})
data_pattern = jmespath.compile(
    "items[].{req_skills: snippet.requirement, expirience: experience.name}"
)
# TODO: Нужно регистрировать поля по квери объекту или имени квери, чтобы сразу в него добавлялся объект
# url_f = JsonField('url', 'alternate_url', 'key', url=url)
# vacancy_name = JsonField('vacancy', 'area.name', 'dict.key', url=url)
# salary = JsonField('salary', 'salary.from', 'dict.key', url=url)
# publish_date = JsonField('publish_at', 'published_at', 'key', url=url)
# archived = JsonField('archived', 'archived', 'key', url=url)
# req_skills = JsonField(
#     'req_skills', 'snippet.requirement', 'dict.key', url=url)
# experience = JsonField('expirience', 'experience.name', 'dict.key', url=url)

# TODO: нужно понять как мапить списки. Скорее всего через прокси-типы.
# TODO: подумать над тем, чтобы искать аннотацию аттрибута среди mro других объектов,
# зарегистрированных за определенным url - так можно закрыть сложности с наследованием и вероятностью запутаться.
# TODO: нужно создавать регистры в модулях, где описываются классы, а потом импортировать в отдельный модуль регистры

with JsonSchema("vacancy_de", "items", url=url) as schema:
    schema["url"] = "alternate_url"
    schema["city"] = "area.name"
    schema["salary"] = "salary.from"
    schema["published_at"] = "published_at"
    schema["archived"] = "archived"
    schema["req_skills"] = "snippet.requirement"
    schema["expirience"] = "experience.name"


scr.scrape.setup_trigger(period=1, start_time="9:10")
r.set_correct_query()  # FIXME: следствие монки патчинга, это нужно полюбому как-то разрешать.
app = Yass()
app.run()

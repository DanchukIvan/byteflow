import httpx
import jmespath
import pandas as pd

from resources import QueryString
from schemas import JsonSchema
from yass import Yass

app = Yass()
builder = app.create_context()

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Bearer APPLQOF89F6EC73OPSF8P75TB39VA0PNP624S3DO42MT8298OLM4E5991N06A1OC",
}

scraper = builder.create_scraper("api", extra_headers=headers)

url = "https://api.hh.ru/vacancies"
s3_kwargs = {
    "endpoint_url": "https://storage.yandexcloud.net/",
    "key": "YCAJE2ch9FookEYy7zu5No3Us",
    "secret": "YCP96xrQlCajcbliwyGR1d2wnkae1Z6W-gs4aDO6",
}

resource = builder.create_resource("api", url, 1)
repo = builder.create_repo("network", "csv", engine_kwargs=s3_kwargs)

q = QueryString(
    "data_engineer_2",
    persist_fields={
        "text": "data+engineer+AND+python",
        "search_period": 5,
        "per_page": 100,
    },
)


def prepare_area_lst(slicer: tuple[int, int] = None) -> list[int]:
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
    print(pattern_area_ids)
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


areas = prepare_area_lst()
q.set_mutable_field({"area": areas})
resource.put_query(q)

with (json_s := JsonSchema("json", "items", resource.context)) as schema:
    schema["url"] = "alternate_url"
    schema["city"] = "area.name"
    schema["salary"] = "salary.from"
    schema["published_at"] = "published_at"
    schema["archived"] = "archived"
    schema["req_skills"] = "snippet.requirement"
    schema["expirience"] = "experience.name"

q.schema = json_s

builder.check_ready()
scraper.scrape.setup_trigger(
    period=1, start_time="08:00", end_time="23:00", frequency=3
)
app.run()

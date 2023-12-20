from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import Any

import jmespath
import polars as pl
import pyarrow as pa
from attrs import define, field, validators
from jmespath.parser import ParsedResult
from regex import regex

import base
from registies import schemas_registry


@define(slots=False)
class BaseTextSchema(ABC, base.YassAttr):
    schema_name: str = field()
    item_path: str = field()
    context: dict = field()
    item_fields: dict[str, str] = field(factory=dict)
    search_pattern: Any = field(init=False)
    metadata: dict = field(factory=dict)

    def __enter__(self):
        return self.item_fields

    def __exit__(self, exc_type, exc_value, traceback):
        pass
        # self.create_pattern()

    @abstractmethod
    def transform(self):
        """Метод, который ищет в объекте данных по паттерну нужные поля и сохраняет их в инстанс схемы"""

    @abstractmethod
    def create_pattern(self):
        """Метод, который создает паттерн и кэширует его в атрибуте класса (инстанса класса)"""

    def __init_subclass__(cls, schema_name):
        if schema_name is None:
            raise ValueError(
                "У подклассов BaseSchema обязательно нужно имя для регистрации в фабрике классов"
            )

        schemas_registry[schema_name] = cls

    def __hash__(self) -> int:
        return hash(id(self))


@define(slots=False)
class JsonSchema(BaseTextSchema, schema_name="json"):
    search_pattern: ParsedResult = field(init=False)

    def __attrs_post_init__(self):
        extension = self.schema_name
        self.settings.io.input_fmt = extension  # TODO: вот это вот должен устанавливать объект запроса или ресурс, но для целей теста пока оставлю так
        self.settings.io.schema_registry[extension] = self

    def print_json_paths(self):
        columns = {}
        for name, path in self.item_fields.items():
            column = f"$.{self.item_path}[*].{path}"
            columns.update({name: column})
        return columns

    def transform(self, raw_dataframe: "pl.DataFrame"):
        dataframe: pl.DataFrame = pl.concat(
            raw_dataframe[self.item_path]
        ).to_frame(self.item_path)
        print(dataframe.head())
        if dataframe[self.item_path].dtype is pl.Null:
            print(
                f"Here is nothing. {self.__class__.__name__} return sentinel"
            )
            return False, True
        columns = []
        for name, path in self.item_fields.items():
            column = (
                pl.col("json").str.json_path_match(f"$.{path}").alias(name)
            )
            columns.append(column)
        struct_frame = dataframe.with_columns(
            pl.select(
                json=pl.lit(
                    dataframe.select(self.item_path)
                    .unnest(self.item_path)
                    .write_ndjson()
                )
                .str.strip("\n")
                .str.split("\n")
            ).explode("json")
        )
        print(struct_frame.head())
        final_dataframe = struct_frame.with_columns(columns)
        final_dataframe = final_dataframe.drop([self.item_path, "json"])
        print(final_dataframe.head())
        final_dataframe = final_dataframe.rechunk()
        return final_dataframe, False

    @property
    def schema_metadata(self):
        return self.metadata

    def __check_pattern(self):
        self.create_pattern()

    def create_pattern(self):
        string = self.create_field_string()
        compiled = jmespath.compile(string)
        self.search_pattern = compiled

    def create_field_string(self):
        to_join_string = []
        for name, path in self.item_fields.items():
            to_join_string.append(f"{name}: {path}")
        fields_string = ", ".join(to_join_string)
        full_pattern_string = f"$.{self.item_path}[*].{fields_string}"
        return full_pattern_string

    def set_item_field(self, field_obj):
        if isinstance(field_obj, dict):
            self.item_fields.update(field_obj)
            return
        raise ValueError(
            "Коллекция полей элементов может состоять только из подклассов класса DataField"
        )

    def __getattribute__(self, __name: str):
        if __name == "take_data":
            self.__check_pattern()
        return super().__getattribute__(__name)

    def __hash__(self) -> int:
        return super().__hash__()


def validate_path_or_pattern(instance, attribute, path_or_pattern):
    allowed_path = {"list", "key", "dict"}
    form_validator = regex.compile(r"([\w+]\.?){1,}")
    res = form_validator.search(path_or_pattern).captures()[0]
    print(res)
    if len(res) == len(path_or_pattern) and attribute.name == "path":
        return
    lst = set(res.split("."))
    res = lst - allowed_path
    if len(res) == 0:
        return
    raise ValueError(
        f'В пути или паттерне есть значения, не подходящие под допустимый шаблон. Допустимые значения элементов паттерна {allowed_path}, допустимый шаблон пути "path_name.path_name.path_name..."'
    )


def validate_index(instance, attribute, index):
    def check_stop_index(index):
        stop = index.stop
        start = index.start
        if not stop or stop < start:
            raise ValueError(
                "Конец объекта среза может быть только положительным числом и больше начальной позиции. Для доступа к элементу по индексу без среза используйте целые числа"
            )

    for i in index:
        if isinstance(i, (int, slice)):
            if type(i) == slice:
                check_stop_index(i)

        else:
            raise ValueError(
                "Индекс может быть только целым числом или объектом среза"
            )


class DataField(base.YassAttr):
    ...


string_validator = validators.instance_of(str)


@define(slots=False)
class JsonField(DataField):
    name: str = field(validator=string_validator)
    path: str = field(validator=validate_path_or_pattern)
    path_pattern: str = field(
        validator=[string_validator, validate_path_or_pattern]
    )
    index: list[int | slice] = field(
        validator=[validators.instance_of(list), validate_index], default=[]
    )
    string: str = field(default="", init=False)

    def set_path(self, path, path_pattern):
        self.path = path
        self.path_pattern = path_pattern

    def set_index(self, index):
        self.index.append(index)

    def build_string(self):
        splitted_path = self.path.split(".")
        splitted_pattern = self.path_pattern.split(".")
        if "list" in splitted_pattern:
            index = iter(self.index)
        zipped_path = zip(splitted_path, splitted_pattern)
        result = list()
        if not self.string:
            for path, pattern in zipped_path:
                if pattern == "list":
                    idx = next(index)
                    print(idx)
                    # TODO: нужно подумать как тут элегантно проверить на корректно заполненные слайсы индекс
                    idx_string = (
                        f"{path}[{idx}]"
                        if isinstance(idx, int)
                        else f"{path}[{idx.start if idx.start else 0}:{idx.stop}]"
                    )
                    print(idx_string)
                    result.append(idx_string)
                else:
                    result.append(f"{path}")
            field_path = ".".join(result)
            self.string = f"{self.name}: {field_path}"

        return self.string

    def __str__(self):
        if not self:
            self.build_string()
        return self.string

    def __bool__(self):
        return bool(self.string)


if __name__ == "__main__":
    url = "https"
    with (j := JsonSchema("vacancy_de", "items", url=url)) as schema:
        schema["url"] = "alternate_url"
        schema["city"] = "area.name"
        schema["salary"] = "salary.from"
        schema["published_at"] = "published_at"
        schema["archived"] = "archived"
        schema["req_skills"] = "snippet.requirement"
        schema["expirience"] = "experience.name"

    print(j.create_field_string())

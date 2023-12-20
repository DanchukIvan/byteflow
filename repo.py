import importlib
import os.path
import platform
from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from itertools import filterfalse
from typing import ClassVar, NewType, TypedDict

import pandas as pd
import polars as pl
import pyarrow as pa
from attrs import asdict, define, field, fields, validators
from fsspec import AbstractFileSystem, available_protocols, filesystem
from fsspec.caching import caches
from fsspec.implementations.cached import SimpleCacheFileSystem
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from sqlalchemy import Engine, create_engine, text

import base
from helpers import to_async
from repo_utils_io import ReadHandler, WriteHandler


def set_path_module():
    if platform.system() == "Windows":
        # from pathlib import WindowsPath
        pathlib = importlib.import_module("pathlib", "WindowsPath")
        path_class = getattr(pathlib, "WindowsPath", None)
    elif platform.system() == "Linux":
        # from pathlib import PosixPath
        pathlib = importlib.import_module("pathlib")
        path_class = getattr(pathlib, "PosixPath", None)
    return path_class


@define(slots=False)
class Repo(ABC, base.YassService):
    output_format: str = field()

    @abstractmethod
    def create(self, object):
        pass

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def read(self, object):
        pass

    @abstractmethod
    def write(self, object):
        pass

    def __init_subclass__(cls, repo_type):
        super().__init_subclass__(repo_type)
        # if repo_type is None:
        #     raise ValueError(
        #         "У подклассов Repo обязательно должен быть указан тип хранилища для регистрации в фабрике классов"
        #     )
        # repos_registry[repo_type] = cls


FileSystem = NewType("FileSystem", AbstractFileSystem)

# TODO: нужно сделать буфер подходящим под структурированные объекты (словари, csv-файлы, avro-файлы и другие)
# TODO: выбор типа файла будет осуществлять в скрапере
# TODO: нужно сделать подходящий миксин для выгрузок данных в любом формате. Скорее прослойкой будет пандас.
# TODO: нужно переписать все методы - учитывая, что грузим мы дикты, нужно их корректно обрабатывать
# а это можно сделать только с помощью pandas или другой библиотеки, заточенной под табличный формат.

import dataclasses


@dataclasses.dataclass
class RepoBuffer:
    mem_size: int = dataclasses.field(default=100)
    internal_buffer_map: dict[str, pl.DataFrame] = dataclasses.field(
        default_factory=dict, repr=False
    )
    # engine: FileSystem = dataclasses.field(default=None, repr=False)
    # cursor: str = dataclasses.field(init=False, default="", repr=False)
    deduplicated_hash: set = dataclasses.field(
        init=False, repr=False, default_factory=set
    )
    repo: "Repo" = dataclasses.field(default=None)
    context: dict = dataclasses.field(default=None)
    write_handler: ClassVar["WriteHandler"] = WriteHandler()
    read_handler: ClassVar["ReadHandler"] = ReadHandler()

    def __get__(self, instance, owner=None):
        if self.repo is None and instance is not None:
            self.repo = instance
            self.context = self.repo.context
            self.mem_size = instance.max_memory_size
        return self

    async def pull_data(self, dataframe: pl.DataFrame):
        # TODO: нужно в функции проверки строк реализовать замену типа null на in
        dedup_data = self.check_rows(dataframe)
        final_data = self.insert_metadata(dedup_data)
        if self.repo.cursor not in self.internal_buffer_map:
            self.internal_buffer_map[self.repo.cursor] = final_data
            internal_df = final_data
        else:
            internal_df = self.internal_buffer_map[self.repo.cursor]
        internal_df = internal_df.extend(final_data)
        if internal_df.estimated_size() > self.mem_size:
            await self.merge_to_backend()

    async def merge_to_backend(self):
        path = self.repo.cursor
        engine = self.repo.engine
        chunk_of_new_data = self.internal_buffer_map[path]
        print(f"Schema of new data is {chunk_of_new_data.schema}")
        storage_data = await engine._cat(path)
        if len(storage_data) > 0:
            backend_fmt = self.repo.output_format
            backend_data, _ = self.read_handler.handle(
                storage_data,
                overload=backend_fmt,
                need_schema=False,
                schema=chunk_of_new_data.schema,
            )
            print(f"Schema of backend data is {backend_data.schema}")
            # TODO: мы здесь можем просто сджойнить датасеты по непересекающемуся множеству хэшей
            backend_data: pl.DataFrame = backend_data.extend(chunk_of_new_data)
            print(
                f"Backend data before filtering duplicates is {backend_data.shape}"
            )
            estimate_columns = backend_data.select(
                pl.exclude(["created_at", "city"])
            ).columns
            backend_data = backend_data.unique(
                subset=estimate_columns, keep="first"
            )
            print(
                f"Backend data after filtering duplicates is {backend_data.shape}"
            )
        else:
            backend_data = chunk_of_new_data
        file = BytesIO()
        self.write_handler.handle(backend_data, file)
        file.seek(0)
        await engine._pipe_file(path, file.getvalue())
        return backend_data

    def insert_metadata(self, dataframe: "pl.DataFrame"):
        updated_data = dataframe.with_columns(
            pl.lit(datetime.now(), pl.Datetime).alias("created_at")
        )
        return updated_data

    def check_rows(self, dataframe: "pl.DataFrame"):
        for field, dtype in dataframe.schema.items():
            if dtype == pl.Null:
                print(f"Field {field} is null, changed to Int64")
                dataframe = dataframe.cast({field: pl.Int64})
                print(f"Dataschema now is {dataframe.schema}")
        dataframe = dataframe.fill_null(0)
        uniq_df = dataframe.filter(dataframe.is_unique())
        return uniq_df


def mb_converter(number):
    if number > 0:
        return number * (2**20)
    else:
        raise ValueError("Буфер памяти в мегабайтах должен быть больше нуля")


int_validator = validators.instance_of(int)


@define(slots=False, auto_attribs=True)
class NetworkRepo(Repo, repo_type="network"):
    network_fs: str = field(default="s3")
    engine_kwargs: dict = field(default={})
    cursor: str = field(default="")
    repo_buffer: ClassVar[RepoBuffer] = RepoBuffer()
    max_memory_size: int = field(
        default=5, converter=mb_converter, validator=int_validator
    )
    available_repos: ClassVar[list[str]] = [
        "dropbox",
        "http",
        "https",
        "gcs",
        "gs",
        "gdrive",
        "sftp",
        "ssh",
        "ftp",
        "hdfs",
        "arrow_hdfs",
        "webhdfs",
        "s3",
        "s3a",
        "wandb",
        "oci",
        "ocilake",
        "adl",
        "abfs",
        "az",
        "dask",
        "dbfs",
        "github",
        "git",
        "smb",
        "jupyter",
        "jlab",
        "libarchive",
        "oss",
        "webdav",
        "dvc",
        "hf",
        "box",
        "lakefs",
    ]
    engine: FileSystem = field(init=False)
    session: str | None = field(default=None)

    def __attrs_post_init__(self):
        self.__build_engine()

    def __build_engine(self):
        if self.network_fs in self.available_repos:
            target_option = {"use_listings_cache": True} | self.engine_kwargs
            fs = filesystem(self.network_fs, **target_option)
            self.engine = fs
            print(f"Build engine on {fs}")
        else:
            raise ValueError(
                "Указанный протокол не входит в список поддерживаемых сетевых протоколов"
            )

    def rebuild_engine(self, kwargs):
        self.engine_kwargs.update(kwargs)
        self.__build_engine()

    async def create(self, filepath):
        bucket, _ = os.path.split(filepath)
        try:
            await self.mk_dir(bucket)
        except FileExistsError:
            print(f"Folder {bucket} is exists")
            await self.engine._touch(filepath)
        self.cursor = filepath

    async def write(self, data, filepath=""):
        filepath = f"{filepath}" or self.cursor
        self.cursor = filepath
        if not await self.engine._exists(filepath):
            await self.create(filepath)
        await self.repo_buffer.pull_data(data)

    async def read(self):
        ...

    async def read_from_backend(self, filepath="", with_update=False):
        filepath = f"{filepath}" or self.cursor
        self.cursor = filepath
        if not await self.engine._exists(filepath):
            await self.create(filepath)
            data = b""
        else:
            if with_update:
                data = await self.repo_buffer.merge_to_backend()
            else:
                data = await self.engine._cat(filepath)
            print("File is not existed - upload data from storage")
        return data

    read = read_from_backend

    async def read_from_buf(self, filepath=""):
        filepath = f"{filepath}" or self.cursor
        self.cursor = filepath
        proxy_df = self.repo_buffer.internal_buffer_map[self.cursor]
        # TODO: нужно сделать выбор формата, в котором будет формироваться вывод

        return proxy_df.internal_df

    async def delete(self, filepath, recursive=False):
        try:
            await self.engine._rm(filepath, recursive=recursive)
        except FileNotFoundError:
            print(f"Not such file {filepath} in backend")

    async def ls_paths(self, path=""):
        path = path if path else os.path.dirname(self.cursor)
        ls = await self.engine._ls(f"{path}/", detail=False)
        return ls

    async def mk_dir(self, path):
        path = os.path.dirname(path) if path else os.path.dirname(self.cursor)
        await self.engine._mkdir(path)
        print(f"Created new file {path}!")

    async def __aenter__(self):
        await self.make_session()
        return self

    async def make_session(self):
        if not self.session:
            session = await self.engine.set_session()
            self.session = session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print("Close connection to repo")
        await self.repo_buffer.merge_to_backend()
        # FIXME: нативно в s3fs сессия после закрытия не открывается заново в движке s3. Нужно вручную открывать сессию
        # Aibotocore3 и прокидывать ее в движок на этапе его создания. Но тогда сломается интерфейс...
        # Пока сессия будет оставаться открытой до конца работы приложения.
        # await self.session.close()


@define(slots=False)
class LocalRepo(Repo, repo_type="local"):
    local_fs: str = field(init=False, default="asynclocal")
    available_repos: ClassVar[list[str]] = ["asynclocal"]
    cursor: str = field(default="")
    opened_fd: dict[str, MemoryFile] = field(default={})
    max_memory_size: int = field(
        default=5, converter=mb_converter, validator=int_validator
    )
    engine: FileSystem = field(init=False)
    engine_kwargs: dict = field(default={})

    def __attrs_post_init__(self):
        if self.output_format in self.io_mapper.keys():
            self.io_tool = self.io_mapper[self.output_format]
            self.__build_engine()
        else:
            raise ValueError(
                f"Формат файлов {self.output_format} не поддерживается"
            )

    def __build_engine(self):
        if self.local_fs in self.available_repos:
            target_option = {"use_listings_cache": True} | self.engine_kwargs
            fs = filesystem(self.local_fs, **target_option)
            self.engine = fs
            print(f"Build engine on {fs}")
        else:
            raise ValueError(
                "Указанный протокол не входит в список поддерживаемых локальных протоколов"
            )

    def rebuild_engine(self, kwargs):
        self.engine_kwargs.update(kwargs)
        self.__build_engine()

    async def create(self, filepath):
        await self.engine._touch(filepath)
        self.opened_fd[filepath] = MemoryFile()
        self.cursor = filepath
        print("Created new file!")

    async def write(self, data, filepath=""):
        filepath = filepath or self.cursor
        self.cursor = filepath
        if not await self.engine._exists(filepath):
            await self.create(filepath)
        elif filepath not in self.opened_fd.keys():
            self.opened_fd[filepath] = MemoryFile()
        prepared_data = self.io_tool(data)
        self.opened_fd[filepath].writelines(prepared_data)
        await self.flush(filepath)

    async def flush(self, filepath="", force=False, updating=False):
        buffer = self.opened_fd[filepath]
        if (buffer.size >= self.max_memory_size) or (
            force or updating and buffer.size > 0
        ):
            # TODO: нам бы сюда пихнуть валидацию на повторяющиеся строки
            storaged_data = await self.engine._cat(filepath)
            delta = set(BytesIO(storaged_data)).symmetric_difference(buffer)
            if delta is not None and delta:
                delta_buf = BytesIO()
                delta_buf.writelines(delta)
                storaged_data += delta_buf.getvalue()
            if force:
                print("Will flushed in force mode")
            elif updating:
                print(
                    "Flushing run in update mode - buffer and backend data are not modified"
                )
                return storaged_data
            await self.engine._pipe_file(filepath, storaged_data)
            self.opened_fd[filepath] = MemoryFile()
            return
        print(f"Size of buffer is {buffer.size}, flushing not run")

    def prepare_to_flush(self, filepath):
        filepath = filepath or self.cursor
        dirname = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        return dirname, filename

    async def read(self):
        ...

    async def read_from_backend(self, filepath="", with_update=False):
        filepath = filepath or self.cursor
        self.cursor = filepath
        if not await self.engine._exists(filepath):
            await self.create(filepath)
            data = b""
        else:
            if with_update:
                data = await self.flush(filepath, updating=True)
            else:
                data = await self.engine._cat(filepath)
            print("File is not existed - upload data from storage")
        return data

    read = read_from_backend

    async def read_from_buf(self, filepath="", with_stored_data=False):
        filepath = filepath or self.cursor
        self.cursor = filepath
        if buffer := self.opened_fd.get(self.cursor, False):
            if buffer.size > 0 and not with_stored_data:
                data = buffer.getvalue()
                print("File is existed - upload data from buffer")
                return data
        elif with_stored_data:
            print(
                "Upload data from backend for display actuall data with buffered changes"
            )
            return await self.read_from_backend(filepath, with_update=True)
        else:
            print("Nothing in buffer")
            return b""

    async def delete(self, filepath, recursive=False):
        await self.engine._rm(filepath, recursive=recursive)
        if self.opened_fd.get(filepath, False):
            del self.opened_fd[filepath]

    async def ls_paths(self, path=""):
        path = os.path.dirname(path) if path else os.path.dirname(self.cursor)
        ls = await self.engine._ls(f"{path}/", detail=False)
        return ls

    async def mk_dir(self, path):
        path = os.path.dirname(path) if path else os.path.dirname(self.cursor)
        # TODO: нужно что-то придумать вместо вот такого вот патча, нарушающего общий интерфейс
        try:
            await self.engine._mkdir(path)
        except FileExistsError:
            print("Dir is exist yet")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.on_exit()

    async def on_exit(self):
        for filepath in self.opened_fd.keys():
            await self.flush(filepath, force=True)
        for file in self.opened_fd.values():
            del file


# TODO: нужно сделать собственный подкласс, чтобы к ключам можно было обращаться как к аттрибутам
class TableMetadata(TypedDict):
    last_commit: datetime | None
    size: int


class TableMapping(TypedDict):
    name: str
    meta: TableMetadata


@define(slots=False)
class SqlRepo(Repo, repo_type="sql"):
    connection_string: str = field()
    cursor: str = field(default="")
    table_mapping: TableMapping = field(default=TableMapping)
    memo_buffer = field(default=create_engine("duckdb:///:memory:"))
    max_memory_size: int = field(
        default=5, converter=mb_converter, validator=int_validator
    )
    engine: Engine = field(init=False)
    engine_kwargs: dict = field(default={})
    option_kwargs: dict = field(default={})

    def __attrs_post_init__(self):
        self.__build_engine()

    def __build_engine(self):
        engine = create_engine(
            self.connection_string,
            connect_args=self.engine_kwargs,
            **self.option_kwargs,
        )
        self.engine = engine
        self.engine

    def rebuild_engine(self, option_args, connect_args=None):
        if connect_args:
            self.engine_kwargs.update(connect_args)
        self.option_kwargs.update(option_args)
        self.__build_engine()

    async def __aenter__(self):
        return self

    def create(self, tablename):
        self.table_mapping[tablename] = TableMetadata(
            last_commit=datetime.now(), size=0
        )
        self.cursor = tablename
        print("Created table! Now its in memory buffer")

    @to_async
    def write(self, data, tablename=""):
        tablename = tablename or self.cursor
        self.cursor = tablename
        if not self.exists(tablename):
            self.create(tablename)
        df = pd.DataFrame.from_records(data)
        df.to_sql(tablename, self.memo_buffer)
        self.table_mapping[tablename]["size"] += sum(
            df.memory_usage(deep=True)
        )
        self.flush(tablename)

    def exists(self, tablename):
        with self.engine.connect() as connection:
            pd.read_sql_table(tablename, connection)
            return True
        return False

    def flush(self, tablename="", force=False, updating=False):
        table_meta = self.table_mapping[tablename]
        last_commit = table_meta["last_commit"]
        table_size = table_meta["size"]
        if table_size >= self.max_memory_size or (
            force or updating and table_size > 0
        ):
            with self.engine.connect() as connection:
                db_data = pd.read_sql_table(tablename, connection)
                buf_data = pd.read_sql_table(tablename, self.memo_buffer)[
                    buf_data["created_at"] > last_commit
                ]
                storaged_data = pd.concat([db_data, buf_data])
                if force:
                    print(
                        "Will flushed in force mode - backend data will be modified and buffer cleared"
                    )
                elif updating:
                    print(
                        "Flushing run in update mode - buffer and backend data are not modified"
                    )
                    return storaged_data
                buf_data.to_sql(tablename, connection)
                table_meta["size"] = 0
                table_meta["last_commit"] = datetime.now()
                return
        print(f"Size of buffer is {table_size}, flushing not run")

    def read(self):
        ...

    @to_async
    def read_from_backend(self, tablename="", with_update=False):
        tablename = tablename or self.cursor
        self.cursor = tablename
        if not self.exists(tablename):
            self.create(tablename)
            data = ""
        else:
            if with_update:
                data = self.flush(tablename, updating=True)
            else:
                data = pd.read_sql_table(tablename, self.memo_buffer)
            print("File is not existed - upload data from storage")
        return data

    read = read_from_backend

    @to_async
    def read_from_buf(self, tablename="", with_stored_data=False):
        tablename = tablename or self.cursor
        self.cursor = tablename
        if self.table_mapping[tablename] and not with_stored_data:
            data = pd.read_sql_table(tablename, self.memo_buffer)
            return data
        elif with_stored_data:
            print(
                "Upload data from backend for display actuall data with buffered changes"
            )
            return self.read_from_backend(tablename, with_update=True)
        else:
            print("Nothing in buffer")
            return ""

    def delete(self, tablename, only_buffer=False):
        drop_stm = text(f"DROP TABLE {tablename}")
        with self.engine.connect() as db_conn, self.memo_buffer.connect() as bf_conn:
            bf_conn.execute(drop_stm)
            if only_buffer:
                return
            db_conn.execute(drop_stm)
            del self.table_mapping[tablename]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.on_exit()

    @to_async
    def on_exit(self):
        for table in self.table_mapping.keys():
            self.delete(table, only_buffer=True)


if __name__ == "__main__":
    path = "bucket/folder/file.txt"
    print(os.path.split(path))

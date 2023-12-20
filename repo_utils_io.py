from functools import wraps

import polars as pl

dispatch_input_map = {}
dispatch_output_map = {}


def dispatch_input(*ext):
    registry = dispatch_input_map
    main = None

    def dispatch(ext):
        for ext_set in registry:
            if ext in ext_set or ext == ext_set:
                return registry[ext_set]
        raise RuntimeError(
            "Не зарегистрировано ни одного метода для такого формата данных"
        )

    def add_impl(*ext):
        def impl(func):
            print(
                f'Registred new implemetation of {main} method for input {"formats" if isinstance(ext, tuple) else "format"} {", ".join(ext)}.'
            )
            registry[ext] = func
            return func

        return impl

    def base_method(func):
        # TODO: сделать адаптивную
        registry[ext] = func
        nonlocal main
        main = func.__name__
        print(
            f'Register main dispatching method {main} for input {"formats" if isinstance(ext, tuple) else "format"} {", ".join(ext)}.'
        )

        @wraps(func)
        def wrapper(*args, overload="", need_schema=True, **kwargs):
            self = args[0]
            ext = self.settings.io.input_fmt if not overload else overload
            df = dispatch(ext)(*args, **kwargs)
            print(f"Dataframe before processing {df.head()}")
            try:
                df_and_sent = self.settings.io.schema_registry[ext].transform(
                    df
                )
            except KeyError:
                if need_schema:
                    raise RuntimeError("Схема не зарегистрирована в реестре")
                else:
                    df_and_sent = df, False
            print(
                f"Dataframe after processing {df_and_sent[0].head() if not df_and_sent[1] else None}"
            )
            return df_and_sent

        wrapper.add_impl = add_impl
        return wrapper

    return base_method


def dispatch_output(*ext):
    registry = dispatch_output_map
    main = None

    def dispatch(ext):
        for ext_set in registry:
            if ext in ext_set or ext == ext_set:
                return registry[ext_set]
        raise RuntimeError(
            "Не зарегистрировано ни одного метода для такого формата данных"
        )

    def add_impl(*ext):
        def impl(func):
            print(
                f'Registred new implemetation of {main} for output {"formats" if isinstance(ext, tuple) else "format"} {", ".join(ext)}.'
            )
            registry[ext] = func
            return func

        return impl

    def base_method(func):
        registry[ext] = func
        nonlocal main
        main = func.__name__
        # TODO: сделать адаптивную
        print(
            f'Register main method {main} for output {"formats" if isinstance(ext, tuple) else "format"} {", ".join(ext)}.'
        )

        @wraps(func)
        def wrapper(*args, overload="", **kwargs):
            self = args[0]
            ext = self.settings.io.output_fmt if not overload else overload
            dispatch(ext)(*args, **kwargs)

        wrapper.add_impl = add_impl
        return wrapper

    return base_method


# TODO: это класс аттрибут
class ReadHandler:
    # TODO: здесь будет объект контекста, который он унаследует от базового класса
    context: dict

    def __get__(self, instance, obj=None):
        if instance:
            self.context = instance.context
        return self

    # TODO: на все методы нужно установить жесткую сигнатуру, чтобы её нельзя было перегрузить
    @dispatch_input("dict")
    def handle(self, data, **kwargs):
        df = pl.from_dicts(data, **kwargs)
        return df

    @handle.add_impl("csv")
    def _(self, data, **kwargs):
        df = pl.read_csv(data, **kwargs)
        return df

    @handle.add_impl("json")
    def _(self, data, **kwargs):
        df = pl.read_json(data, **kwargs)
        return df

    @property
    def settings(self):
        return self.context.settings


class WriteHandler:
    context: dict

    def __get__(self, instance, obj=None):
        if instance:
            self.context = instance.context
        return self

    @dispatch_output("csv")
    def handle(self, dataframe: pl.DataFrame, dataobj):
        dataframe.write_csv(dataobj)

    @handle.add_impl("json")
    def _(self, dataframe: pl.DataFrame, dataobj, row_oriented=True):
        dataframe.write_json(dataobj, row_oriented=row_oriented)

    @property
    def settings(self):
        return self.context.settings

from abc import ABCMeta
from typing import ClassVar

__all__ = ["YassFullMeta"]


class YassMeta(type):
    __error: ClassVar[BaseException] = AttributeError(
        "Объекту должен быть передан url или в составе регулярных параметров класса, или как дополнительный ключевой аргмент"
    )

    def __call__(cls, *args, **kwargs):
        url = ""
        new_kwargs = kwargs
        try:
            url = new_kwargs.pop("url")
        except KeyError:
            pass
        instance = super().__call__(*args, **new_kwargs)
        if not url:
            try:
                url = instance.url
            except AttributeError:
                raise cls.__error
        instance.init_context.set(url, instance)
        return instance


class YassFullMeta(YassMeta, ABCMeta):
    ...

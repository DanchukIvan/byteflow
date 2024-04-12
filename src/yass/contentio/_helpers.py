from ast import literal_eval
from collections.abc import Callable, Sequence
from functools import partial
from importlib import import_module
from inspect import Parameter, Signature, signature
from io import BytesIO
from typing import (
    Any,
    NoReturn,
    get_args,
    get_origin,
    get_overloads,
    get_type_hints,
)

__all__: list[str] = [
    "_update_sign",
    "_resolve_annotation",
    "_handle_generic",
    "_check_input_sig",
    "_check_output_sig",
]


def _update_sign(func: Callable, extra_kwargs: dict) -> Callable:
    if not extra_kwargs:
        return func
    sig: Signature = signature(func)
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    new_params: list[Parameter] = []
    for name, param in sig.parameters.items():
        value: Any = extra_kwargs.get(name, param.default)
        if param.kind in (
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        ):
            args.append(value)
        else:
            kwargs[name] = value
        new_params.append(param.replace(name=name, default=value))
    new_sig: Signature = sig.replace(parameters=new_params)
    if kwdefault := getattr(func, "__kwdefaults__", False):
        kwdefault.update(kwargs)  # type: ignore
        setattr(func, "__kwdefaults__", kwdefault)
    elif default := getattr(func, "__defaults__", False):
        default = tuple(args)
        setattr(func, "__defaults__", default)
    elif hasattr(func, "__signature__"):
        func.__signature__ = new_sig
    else:
        func = partial(func, *args, **kwargs)
    return func


def _resolve_annotation(
    annotated: Any, module_name: str
) -> tuple[type, ...] | NoReturn:
    cnt_split = module_name.count(".")
    if isinstance(annotated, str):
        not_resolved = []
        search_area = module_name
        cnt_split = module_name.count(".")
        annotations = annotated.split("|")
        result = []
        for annot in annotations:
            loop_cnt = 0
            while cnt_split >= loop_cnt:
                annot = annot.strip()
                try:
                    import builtins
                    import io
                    import pathlib
                    import typing

                    mod = import_module(search_area)
                    typeclass = (
                        getattr(mod, annot, False)
                        or getattr(builtins, annot, False)
                        or getattr(io, annot, False)
                        or getattr(pathlib, annot, False)
                        or getattr(typing, annot, literal_eval(annot))
                    )
                    if typeclass is not None:
                        result.append(typeclass)
                    break
                except Exception:
                    loop_cnt += 1
                    search_area = search_area.rsplit(".", loop_cnt)[0]
                    if loop_cnt > cnt_split:
                        not_resolved.append(annot)
        if not_resolved:
            msg = f"Для разрешения аннотации, используемой в модуле {module_name}, необходимо импортировать следующие классы: {not_resolved}"
            raise NameError(msg)
        return tuple(result)
    else:
        return tuple(
            t for t in [*get_args(annotated), get_origin(annotated)] if bool(t)
        )


_f: Callable[[type, Any, Parameter], bool] = (
    lambda target_type, _types, param: issubclass(target_type, _types)
    and param.kind
    in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
)


def _handle_generic(
    param: Parameter, annotation: Any, target_type: type
) -> bool:
    if annotation is None or not annotation:
        return False
    status: bool = _f(target_type, annotation, param)
    return status


def _check_input_sig(func: Callable) -> bool:
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[0]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            module_name = str(func.__module__)
            annot = _resolve_annotation(param.annotation, module_name)
        if status := _handle_generic(param, annot, bytes):
            return status
    return False


def _check_output_sig(func: Callable) -> bool:
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[1]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            module_name = str(func.__module__)
            annot = _resolve_annotation(param.annotation, module_name)
        if status := _handle_generic(param, annot, BytesIO):
            return status
    return False

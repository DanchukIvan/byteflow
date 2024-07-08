from ast import literal_eval
from collections.abc import Callable, Sequence
from functools import partial
from importlib import import_module
from inspect import Parameter, Signature, signature
from io import BytesIO
from typing import Any, get_args, get_origin, get_overloads, get_type_hints

__all__ = [
    "check_input_sig",
    "check_output_sig",
    "handle_generic",
    "resolve_annotation",
    "update_sign",
]


def update_sign(func: Callable, extra_kwargs: dict[str, Any]) -> Callable:
    """
    Updates the default values ​​in the parameters of a given function.
    Value binding occurs by argument name, regardless of whether
    positional arguments or keyword are updated.
    Acts like partial, but allows specific updates of arguments.

    Args:
        func (Callable): Target function.
        extra_kwargs (dict[str, Any]): A dict of arguments and values ​​that need to be associated with the function.

    Returns:
        Callable: Function with updated default arguments.
    """
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


def resolve_annotation(annot: Any, annot_owner: Any) -> tuple[type, ...]:
    """
    Converts the string representation of an annotation to a class.
    Ignores None annotation.

    Args:
        annot (Any): The annotation to be converted.
        annot_owner (Any): The object to which the annotation directly belongs. Can be any object containing an annotation.

    Raises:
        NameError: Annotation conversion module not found. The error message indicates the module you are looking for.

    Returns:
        tuple[type, ...] | NoReturn: a tuple with classes corresponding to the annotation (except None).
    """
    module_name = (
        str(annot_owner.__module__)
        if not isinstance(annot_owner, str)
        else annot_owner
    )
    cnt_split = module_name.count(".")
    if isinstance(annot, str):
        not_resolved = []
        search_area = module_name
        cnt_split = module_name.count(".")
        annotations = annot.split("|")
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
            t for t in [*get_args(annot), get_origin(annot)] if bool(t)
        )


_f: Callable[[type, Any, Parameter], bool] = (
    lambda target_type, _types, param: issubclass(target_type, _types)
    and param.kind
    in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
)


def handle_generic(
    param: Parameter, annotation: Any, target_type: type
) -> bool:
    """
    Checking whether the target type is included in the list using a function argument.

    Args:
        param (Parameter): A parameter object that represents the target type.
        annotation (Any): A tuple of annotations represented by classes.
        target_type (type): The type to be checked to see if it is included in the arguments.

    Returns:
        bool: Boolean result of the test performed.
    """
    if annotation is None or not annotation:
        return False
    status: bool = _f(target_type, annotation, param)
    return status


def check_input_sig(func: Callable) -> bool:
    """
    Validates the signature of the function used to deserialize data.
    Such a function must take a bytes object as its first parameter.

    Args:
        func (Callable): Function for deserializing data.

    Returns:
        bool: Boolean result of the test performed.
    """
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[0]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            annot: tuple[type, ...] = resolve_annotation(
                param.annotation, func
            )
        if status := handle_generic(param, annot, bytes):
            return status
    return False


def check_output_sig(func: Callable) -> bool:
    """
    Validates a function for data serialization. Such a function must take a byte buffer object (BytesIO) as its second argument.

    Args:
        func (Callable): Function for serializing data.

    Returns:
        bool: Boolean result of the test performed.
    """
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[1]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            module_name = str(func.__module__)
            annot = resolve_annotation(param.annotation, module_name)
        if status := handle_generic(param, annot, BytesIO):
            return status
    return False

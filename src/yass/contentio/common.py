"""
This module provides a set of shared containers used in other modules of the contentio package.
These containers are mainly represented by dictionary-like singleton objects.
"""

from collections import defaultdict
from collections.abc import Callable

from yass.core import SingletonMixin

__all__ = ["INPUT_MAP", "OUTPUT_MAP"]


class _InputMap(SingletonMixin, dict[str, Callable]):
    """
    Dict-like repository of data input (deserialization) functions.
    The key is the name of the data type in the form of a string, the value is callable,
    assigned as a handler for the corresponding data type.
    """


class _OutputMap(SingletonMixin, dict[str, Callable]):
    """
    Dict-like repository of data output (serialization) functions.
    The key is the name of the data type in the form of a string, the value is callable,
    assigned as a handler for the corresponding data type.
    """


class _IOContextMap(SingletonMixin, defaultdict):
    """
    Dict-like repository of registered IO contexts. Any object can be used as a key (usually an instance of the resource request class).
    This key is also referred to as the binding key. The IO contexts associated with this key are stored in a list that is
    created automatically when the binding key is registered.
    """


INPUT_MAP = _InputMap()
OUTPUT_MAP = _OutputMap()

from os import path

from beartype.claw import beartype_package

pkg: str = path.basename(path.dirname(__file__))
beartype_package(pkg)

from .base import *
from .blob import *

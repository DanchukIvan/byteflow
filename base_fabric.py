from attrs import define, field

import conditions
from registies import fabric_registry


class Fabric:
    fabric_repo = fabric_registry

    def __init_subclass__(cls, fabric_type):
        cls.fabric_repo[fabric_type] = cls

    @classmethod
    def get_fabric(cls, cls_type):
        return cls.fabric_repo[cls_type]

    @classmethod
    def get_cls_types(cls):
        lst = [key for key in cls.fabric_repo]
        return lst

    @classmethod
    def get_fabrics(cls):
        return cls.__subclasses__()


@define(slots=False)
class RepoFabric(Fabric, fabric_type="repos"):
    registry = repo_registry

    @classmethod
    def build_class(cls, cls_type):
        return cls.registry[cls_type]

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def accept(cls, instance):
        if issubclass(instance, Repo):
            return cls()


@define(slots=False)
class CrawlerCombine(Fabric, fabric_type="scrapers"):
    registry = scrapers_registry

    @classmethod
    def build_class(cls, cls_type):
        return cls.registry[cls_type]

    @classmethod
    def create(cls, url):
        resources = [r for r in cls.init_context[url]]
        scrapers_lst = list()
        for resource in resources:
            type_r = resource.resource_type
            crawler = cls.build_class(type_r)(resource)
            scrapers_lst.append(crawler)
            if len(resources) > 1:
                return scrapers_lst
            return scrapers_lst[0]

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def accept(cls, instance):
        if issubclass(instance, Scraper):
            return cls()


@define(slots=False)
class ResourceFabric(Fabric, fabric_type="resources"):
    registry = resources_registry

    @classmethod
    def build_class(cls, cls_type):
        return cls.registry[cls_type]

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def accept(cls, instance):
        if issubclass(instance, Resource):
            return cls()


@define(slots=False)
class SchemaFabric(Fabric, fabric_type="schemas"):
    registry = schemas_registry

    @classmethod
    def build_class(cls, cls_type):
        return cls.registry[cls_type]

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def accept(cls, instance):
        if issubclass(instance, BaseTextSchema):
            return cls()


class ConditionFabric(Fabric, fabric_type="conditions"):
    registry = conditions.conditions_registry

    @classmethod
    def build_class(cls, cls_type):
        return cls.registry[cls_type]

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def accept(cls, instance):
        if issubclass(instance, BaseCondition):
            return cls()

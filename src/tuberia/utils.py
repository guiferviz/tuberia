import importlib

from tuberia.flow import Flow


def get_module_member(module_name, member_name):
    m = importlib.import_module(module_name)
    return getattr(m, member_name)


def get_flow_from_qualified_name(name: str) -> Flow:
    module_name, class_name = name.rsplit(".", 1)
    return get_module_member(module_name, class_name)

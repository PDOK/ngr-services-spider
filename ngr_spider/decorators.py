from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass


def asdict_minus_none(obj, dict_factory=dict):
    """Based on dataclasses._asdict_inner"""
    if hasattr(type(obj), "__dataclass_fields__"):
        result = []
        for field in fields(obj):
            value = asdict_minus_none(getattr(obj, field.name), dict_factory)
            if value is not None:
                result.append((field.name, value))
        return dict_factory(result)
    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return type(obj)(*[asdict_minus_none(v, dict_factory) for v in obj])
    if isinstance(obj, (list, tuple)):
        return type(obj)(asdict_minus_none(v, dict_factory) for v in obj)
    if isinstance(obj, dict):
        return type(obj)(
            (
                asdict_minus_none(k, dict_factory),
                asdict_minus_none(v, dict_factory),
            )
            for k, v in obj.items()
            if v is not None
        )
    return deepcopy(obj)


def nested_dataclass(*args, **kwargs):
    def wrapper(cls):
        cls = dataclass(cls, **kwargs)
        original_init = cls.__init__

        def __init__(self, *args, **kwargs):
            for name, value in kwargs.items():
                field_type = cls.__annotations__.get(name, None)
                print(name, value)
                if hasattr(field_type, "__args__"):
                    inner_type = field_type.__args__[0]
                    if is_dataclass(inner_type):
                        new_obj = [inner_type(**dict_) for dict_ in value]
                        kwargs[name] = new_obj

            original_init(self, *args, **kwargs)

        cls.__init__ = __init__
        return cls

    return wrapper(args[0]) if args else wrapper

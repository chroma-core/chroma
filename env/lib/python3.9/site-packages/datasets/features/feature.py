from dataclasses import dataclass, fields


@dataclass
class Feature:
    pass

    def to_dict(self):
        result = {}
        for f in fields(self):
            value = getattr(self, f.name).to_dict()
            if not f.init or value != f.default or f.metadata.get("include_in_asdict_even_if_is_default", False):
                result[f.name] = value
        return result

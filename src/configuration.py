import dataclasses
import json
from dataclasses import dataclass
from typing import List

import dataconf


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


@dataclass
class Configuration(ConfigurationBase):
    embed_column: str
    pswd_api_key: str
    model: str


import dataclasses
import json
from dataclasses import dataclass
from typing import List

import dataconf

import logging
from pydantic import BaseModel, Field, ValidationError, field_validator
from keboola.component.exceptions import UserException
class Configuration(BaseModel):
    print_hello: bool
    api_token: str = Field(alias="#api_token")
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

    @field_validator('api_token')
    def token_must_be_uppercase(cls, v):
        if not v.isupper():
            raise UserException('API token must be uppercase')
        return v
# class ConfigurationBase:
#     @staticmethod
#     def _convert_private_value(value: str):
#         return value.replace('"#', '"pswd_')

#     @staticmethod
#     def _convert_private_value_inv(value: str):
#         if value and value.startswith("pswd_"):
#             return value.replace("pswd_", "#", 1)
#         else:
#             return value

#     @classmethod
#     def load_from_dict(cls, configuration: dict):
#         json_conf = json.dumps(configuration)
#         json_conf = ConfigurationBase._convert_private_value(json_conf)
#         return dataconf.loads(json_conf, cls, ignore_unexpected=True)

#     @classmethod
#     def get_dataclass_required_parameters(cls) -> List[str]:
#         return [cls._convert_private_value_inv(f.name)
#                 for f in dataclasses.fields(cls)
#                 if f.default == dataclasses.MISSING
#                 and f.default_factory == dataclasses.MISSING]

# @dataclass
# class Configuration(ConfigurationBase):
#     embed_column: str
#     pswd_api_key: str
#     model: str
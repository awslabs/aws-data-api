import fastjsonschema
import chalicelib.parameters as params
from chalicelib.exceptions import InvalidArgumentsException


class SchemaCacheEntry:
    _entry_type = None
    _schema = None
    _schema_validator = None
    _schema_validation_refresh_hitcount = None
    _usage_count = 0

    def __init__(self, entry_type: str, schema: dict,
                 refresh_count: int = params.DEFAULT_SCHEMA_VALIDATION_REFRESH_HITCOUNT):
        self._entry_type = entry_type
        self._schema = schema
        self._schema_validator = fastjsonschema.compile(self._schema)
        self._schema_validation_refresh_hitcount = refresh_count
        self._usage_count = 0

    def validate_item(self, item: dict):
        try:
            self._schema_validator(item)
            self._usage_count += 1
        except Exception as e:
            raise InvalidArgumentsException(e)

    def needs_refresh(self) -> bool:
        if self._usage_count >= self._schema_validation_refresh_hitcount:
            return True
        else:
            return False

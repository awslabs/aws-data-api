import chalicelib.utils as utils
from boto3.dynamodb.conditions import Attr
from chalicelib.dynamo_table_utils import DynamoTableUtils
from chalicelib.exceptions import InvalidArgumentsException
import chalicelib.parameters as params


class ApiMetadata:
    _region = None
    _logger = None
    _table_name = None
    _dynamo_helper = None
    _api_control_table = None
    _kms_key_arn = None

    def __init__(self, region, logger, kms_key_arn: str = None):
        self._region = region
        self._logger = logger
        self._kms_key_arn = kms_key_arn
        self._dynamo_helper, self._api_control_table = self._get_api_control_table()

    def _get_api_control_table(self):
        dynamo_helper = DynamoTableUtils(region=self._region, logger=self._logger)
        api_control_table = dynamo_helper.verify_control_table(kms_key_arn=self._kms_key_arn)

        return dynamo_helper, api_control_table

    def get_api_metadata(self, api_name: str, stage: str, attribute_filters: list = None):
        self._logger.debug(f"Fetching Metadata for {api_name} in Stage {stage}")
        table_name = utils.get_table_name(api_name, stage)
        meta = self._dynamo_helper.get_item(self._api_control_table,
                                            {'api': table_name, 'type': params.CONTROL_TYPE_META})

        if meta is None:
            return meta
        else:
            if attribute_filters is None:
                return meta
            else:
                self._logger.debug(f"Applying Filter for Attributes: {attribute_filters}")
                # apply the attribute filters
                out = {}
                for f in attribute_filters:
                    if f in meta:
                        out[f] = meta[f]

                return out

    def get_all_apis(self):
        scan_response = self._api_control_table.scan(ProjectionExpression='api, Stage',
                                                     Select='SPECIFIC_ATTRIBUTES',
                                                     FilterExpression=Attr('type').eq(params.CONTROL_TYPE_META))

        apis = []
        for i in scan_response.get("Items"):
            apis.append(i.get("api"))

        return apis

    def delete_all_api_metadata(self, api_name, stage):
        table_name = utils.get_table_name(api_name, stage)
        # delete schemas
        self._dynamo_helper.control_table_delete(control_hash=table_name,
                                                 control_sort=params.CONTROL_TYPE_RESOURCE_SCHEMA)
        self._dynamo_helper.control_table_delete(control_hash=table_name,
                                                 control_sort=params.CONTROL_TYPE_METADATA_SCHEMA)

        # delete API Metadata
        self._dynamo_helper.control_table_delete(control_hash=table_name,
                                                 control_sort=params.CONTROL_TYPE_META)

    # public method to return a data API's json schema
    def get_schema(self, api_name, stage, schema_type):
        table_name = utils.get_table_name(api_name, stage)

        if schema_type.lower() == params.RESOURCE.lower():
            schema_type = params.CONTROL_TYPE_RESOURCE_SCHEMA
        elif schema_type.lower() == params.METADATA.lower():
            schema_type = params.CONTROL_TYPE_METADATA_SCHEMA
        else:
            raise InvalidArgumentsException(f"Invalid Schema Type {schema_type}")

        schema = self._dynamo_helper.get_control_item(table_ref=self._api_control_table, api_name=table_name,
                                                      control_type=schema_type)

        if schema is not None:
            return schema[schema_type]
        else:
            return None

    # public method to create a JSON schema for this data API
    def put_schema(self, api_name, stage, schema_type, caller_identity, schema):
        table_name = utils.get_table_name(api_name, stage)

        if schema_type.lower() == params.RESOURCE.lower():
            set_schema_type = params.CONTROL_TYPE_RESOURCE_SCHEMA
        elif schema_type.lower() == params.METADATA.lower():
            set_schema_type = params.CONTROL_TYPE_METADATA_SCHEMA
        else:
            raise InvalidArgumentsException(f"Invalid Schema Type: {schema_type}")

        # nest the schema into a named object due to possible collisions with hash/sort key
        return self._dynamo_helper.control_table_update(table_name, set_schema_type,
                                                        caller_identity, **{set_schema_type: schema})

    def create_metadata(self, api_name, stage, caller_identity="System", **kwargs):
        table_name = utils.get_table_name(api_name, stage)

        return self._dynamo_helper.control_table_update(control_hash=table_name, control_sort=params.CONTROL_TYPE_META,
                                                        caller_identity=caller_identity, **kwargs)

    def update_metadata(self, api_name, stage, updates, caller_identity="System"):
        if updates is not None and updates != []:
            table_name = utils.get_table_name(api_name, stage)

            return self._dynamo_helper.control_table_update(control_hash=table_name,
                                                            control_sort=params.CONTROL_TYPE_META,
                                                            caller_identity=caller_identity, **updates)
        else:
            return None

    def delete_metadata(self, api_name, stage, metadata_type, caller_identity="System"):
        table_name = utils.get_table_name(api_name, stage)

        return self._dynamo_helper.control_table_delete(control_hash=table_name, control_sort=metadata_type,
                                                        caller_identity=caller_identity)

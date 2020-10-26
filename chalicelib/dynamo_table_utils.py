import boto3
import time
import re
import json
import logging
from decimal import Decimal
import chalicelib.utils as utils
from chalicelib.exceptions import *
from chalicelib.dynamo_expression_handler import DynamoUpdateExpressionHandler
from chalicelib.data_api_encoder import DataApiEncoder
import chalicelib.parameters as params

CE = "ConditionExpression"
UE = "UpdateExpression"
FE = "FilterExpression"
EAN = "ExpressionAttributeNames"
EAV = "ExpressionAttributeValues"


class DynamoTableUtils:
    _dynamo_client = None
    _dynamo_resource = None
    _log = None
    _control_table = None

    def __init__(self, logger, region):
        if region is None:
            raise Exception("Cannot instantiate DynamoTableUtils without Region")
        else:
            self._dynamo_client = boto3.client('dynamodb', region_name=region)
            self._dynamo_resource = boto3.resource('dynamodb', region_name=region)

        self._control_table = self._dynamo_resource.Table(params.CONTROL_TABLE)
        if logger is not None:
            self._log = logger
        else:
            self._log = utils.setup_logging()

    def wait_until_index_active(self, table_name, index_name):
        index_active = False
        notified = False
        while not index_active:
            if self.get_index_status(table_name, index_name) != 'ACTIVE':
                if not notified:
                    self._log.info(f"Waiting for Index {index_name} to be Active")
                    notified = True
                time.sleep(1)
            else:
                index_active = True

        return

    def wait_until_table_active(self, table_name):
        table_active = False
        notified = False
        while not table_active:
            if self.get_table_status(table_name) != 'ACTIVE':
                if not notified:
                    self._log.info(f"Waiting for Table {table_name} to be Active")
                    notified = True
                time.sleep(1)
            else:
                table_active = True

        return

    def wait_until_table_gone(self, table_name):
        table_active = False
        notified = False
        while not table_active:
            try:
                self.get_table_status(table_name)
                time.sleep(1)
            except self._dynamo_client.exceptions.ResourceNotFoundException:
                return

    # method to get indexes for a table
    def get_table_indexes(self, table_name):
        try:
            table_desc = self._dynamo_client.describe_table(TableName=table_name)
            if table_desc is not None and 'Table' in table_desc and 'GlobalSecondaryIndexes' in table_desc['Table']:
                return table_desc['Table']['GlobalSecondaryIndexes']
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            return None

    def get_index_status(self, table_name, index_name):
        indexes = self.get_table_indexes(table_name)

        if indexes is not None:
            for i in indexes:
                if i['IndexName'] == index_name:
                    return i['IndexStatus']

        return None

    # method to determine table status, as boto3 doesn't have a waiter class for index creation :(
    def get_table_status(self, table_name):
        try:
            table_desc = self._dynamo_client.describe_table(TableName=table_name)
            if 'Table' in table_desc:
                return table_desc['Table']['TableStatus']
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            return None

    # method to create/confirm the status of a given dynamoDB table
    def verify_dynamo_table(self, table_name, attributes, key_schema):
        try:
            self._dynamo_client.describe_table(TableName=table_name)
            return self._dynamo_resource.Table(table_name)
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            self._log.info(f'Creating new DynamoDB table: {table_name}')

            self._dynamo_client.create_table(AttributeDefinitions=attributes,
                                             TableName=table_name,
                                             KeySchema=key_schema,
                                             BillingMode=params.PAY_PER_REQUEST,
                                             StreamSpecification={
                                                 'StreamEnabled': True,
                                                 'StreamViewType': 'NEW_AND_OLD_IMAGES'
                                             }
                                             )
            self.wait_until_table_active(table_name)

            return self._dynamo_resource.Table(table_name)

    # method to create/confirm the control table for the system, which stores schemas, table metadata, etc
    def verify_control_table(self):
        control_attributes = [
            {
                'AttributeName': params.CONTROL_HASH,
                'AttributeType': 'S'
            },
            {
                'AttributeName': params.CONTROL_SORT,
                'AttributeType': 'S'
            }
        ]

        control_key = [
            {
                'AttributeName': params.CONTROL_HASH,
                'KeyType': 'HASH'
            },
            {
                'AttributeName': params.CONTROL_SORT,
                'KeyType': 'RANGE'
            }
        ]
        return self.verify_dynamo_table(params.CONTROL_TABLE, control_attributes, control_key)

    # method to retrieve a control table item
    def get_item(self, table, key):
        item = table.get_item(Key=key)

        if item is not None and params.ITEM in item:
            return item[params.ITEM]
        else:
            return None

    # method to retrieve a control table item
    def get_control_item(self, table_ref, api_name, control_type):
        item = table_ref.get_item(Key={
            params.CONTROL_HASH: api_name,
            params.CONTROL_SORT: control_type
        })

        if item is not None and params.ITEM in item:
            i = item[params.ITEM]
            if params.CONTROL_HASH in i:
                del i[params.CONTROL_HASH]
            if params.CONTROL_SORT in i:
                del i[params.CONTROL_SORT]

            return i
        else:
            return None

    # public method to create table metadata
    def create_table_metadata(self, api_name, caller_identity, **kwargs):
        return self.control_table_update(control_hash=api_name, control_sort=params.CONTROL_TYPE_META,
                                         caller_identity=caller_identity, **kwargs)

    # method to delete a control table item
    def control_table_delete(self, control_hash, control_sort, caller_identity):
        args = {
            "Key": {
                params.CONTROL_HASH: control_hash,
                params.CONTROL_SORT: control_sort
            }
        }

        try:
            self._log.debug("Control Table Delete")
            self._log.debug(args)
            self._control_table.delete_item(**args)

            return True
        except Exception as e:
            raise DetailedException(e)

    # method to perform a raw update of a control table item
    def control_table_update(self, control_hash, control_sort, caller_identity, **kwargs):
        update_expressions = []
        attribute_names = {}
        attribute_values = {}

        # pass the body through a json rinse to convert all floats to decimal
        updates = json.loads(json.dumps(kwargs, cls=DataApiEncoder), parse_float=Decimal)

        for k in updates:
            token = self.make_ddb_expressionval(k)
            update_expressions.append("#{0} = :{0}".format(token))
            attribute_names[f"#{token}"] = k
            attribute_values[f":{token}"] = updates[k]

        # create valid update expression
        update_expression = ", ".join(update_expressions)
        update_expression = f"SET {update_expression}"

        args = {
            "Key": {
                params.CONTROL_HASH: control_hash,
                params.CONTROL_SORT: control_sort,
            }, "UpdateExpression": update_expression
            , EAN: attribute_names
            , EAV: attribute_values
            , "ReturnValues": "ALL_NEW"
        }

        self.decorate_update_request(args, caller_identity, params.ACTION_UPDATE, False)

        try:
            self._log.debug("Control Table Update")
            self._log.debug(args)
            response = self._control_table.update_item(**args)

            return_value = {params.DATA_MODIFIED: True}
            if 'Attributes' in response:
                return_value['Attributes'] = response.get('Attributes')

            return return_value
        except self._dynamo_client.exceptions.ConditionalCheckFailedException:
            raise ResourceNotFoundException()
        except Exception as e:
            raise DetailedException(e)

    # private method which ensures that LastUpdateDate and LastUpdatedBy fields are added to every mutating operation
    def decorate_update_request(self, args, caller_identity, update_action, auto_increment=True):
        # extract all the clauses from the update expression
        current_expression = DynamoUpdateExpressionHandler(args.get("UpdateExpression", None))

        # add the LastUpdateDate and LastUpdatedBy fields to the SET portion of the update args
        LUD_A = "#lud"
        LUD_V = ":lud"
        LUB_A = "#lub"
        LUB_V = ":lub"
        LUA_A = "#lua"
        LUA_V = ":lua"

        last_updates = f"{LUD_A} = {LUD_V}, {LUB_A} = {LUB_V}, {LUA_A} = {LUA_V}"
        current_expression.add_expression_item(params.SET, last_updates)

        if EAN not in args:
            args[EAN] = {}
        args[EAN][LUD_A] = params.LAST_UPDATE_DATE
        args[EAN][LUB_A] = params.LAST_UPDATED_BY
        args[EAN][LUA_A] = params.LAST_UPDATE_ACTION

        if EAV not in args:
            args[EAV] = {}
        args[EAV][LUD_V] = utils.get_date_now()
        args[EAV][LUB_V] = caller_identity
        args[EAV][LUA_V] = update_action

        # increment the item version through the ADD portion of the update args
        if auto_increment:
            increment = f"#{params.ITEM_VERSION} :incr"
            current_expression.add_expression_item(params.ADD, increment)
            args[EAN][f'#{params.ITEM_VERSION}'] = params.ITEM_VERSION
            args[EAV][":incr"] = 1

        # re-add the original update expressions
        args['UpdateExpression'] = current_expression.get_expression()

    # helper method to pre-process attribute names to be DDB compliant
    def make_ddb_expressionval(self, string):
        return re.sub('\W+', '', string)

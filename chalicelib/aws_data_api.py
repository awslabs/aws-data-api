from chalicelib.exceptions import *
import chalicelib.utils as utils
import chalicelib.parameters as params
from chalicelib.utils import identity_trace
import logging
from chalicelib.api_metadata import ApiMetadata
from chalicelib.gremlin_handler import GremlinHandler
import sys
import os
import urllib.parse as parser
import json
import boto3
from elasticsearch import Elasticsearch
from aws_xray_sdk.core import patch

__version__ = "0.9.0b1"

# patch boto3 with xray instrumentation if the environment is configured
if utils.strtobool(os.getenv(params.XRAY_ENABLED, 'false')) is True:
    patch(['boto3'])

log = None


# non-class method to get the status of an API, as the Class version represents a fully online API, while non-class
# methods can be used for API's in process
def get_api_status(api_name: str, stage: str, region: str, logger: logging.Logger = None) -> dict:
    """
    Method to return the status of an API Namespace for the specified Stage in a Region.

    :param api_name: The API Namespace to get the status of
    :param stage: The Stage to query for the Namespace status
    :param region: The AWS Region in which the Stage is provisioned
    :return: dict:
        Status: The Status of the API Namespace in the stage
    """
    global log
    if logger is None:
        log = utils.setup_logging()
    else:
        log = logger

    api_metadata_handler = ApiMetadata(region, log)
    s = "Status"
    return {s: api_metadata_handler.get_api_metadata(api_name=api_name, stage=stage).get(s)}


# non-class method to get all registered api endpoints
def get_registry(region: str, stage: str, logger: logging.Logger = None) -> list:
    """
    Method to return the list of all API Namespaces in the Stage and Region

    :param region: The AWS Region to query
    :param stage: The STage to query for Namespaces
    :return: list:
        str: Namespace name
    """
    # create an API Metadata Handler
    global log
    if logger is None:
        logging.basicConfig()
        log = logging.getLogger(params.AWS_DATA_API_NAME)
        log.setLevel(logging.INFO)
    else:
        log = logger

    api_metadata_handler = ApiMetadata(region, log)

    all_apis = api_metadata_handler.get_all_apis()

    return_apis = []
    for a in all_apis:
        if f"-{stage}" in a:
            return_apis.append(a.replace(f"-{stage}", ""))

    return return_apis


# method to stand up a new namespace for Data API's via an async lambda
def async_provision(api_name: str, stage: str, region: str, logger: logging.Logger, **kwargs) -> None:
    """
    Method to provision a new API Namespace in the specified Stage and Region. Get Namespace status with :func:`~aws_data_api.get_api_status`.

    :param api_name: The API Namespace name to create
    :param stage: The Stage where the Namespace should be created
    :param region: The AWS Region where the Stage is deployed
    :param kwargs: Dictionary of provisioning arguments. This can include:

    :return: None
    """

    kwargs['ApiName'] = api_name

    lambda_client = boto3.client("lambda", region_name=region)

    f = f"{params.AWS_DATA_API_NAME}-{stage}-{params.PROVISIONER_NAME}"

    logger.debug(f"Requesting async provision of API {api_name}")
    logger.debug(kwargs)

    response = lambda_client.invoke(FunctionName=f, InvocationType='Event',
                                    Payload=json.dumps(kwargs))

    if "FunctionError" in response:
        if response.get("FunctionError") == "Handled":
            raise DetailedException(response.get("Payload"))
        else:
            raise DetailedException("Unhandled Error Occurred during submission of Async Provisioning Request")
    else:
        return response.get("Status")


# method to load an instance of the Data API class from a metadata dict
def load_api(**kwargs):
    """
    Static method to instantiate a new instance of :func:`~aws_data_api.AwsDataAPI`

    :param kwargs: Dictionary of setup arguments
    :return: :func:`~aws_data_api.AwsDataAPI`
    """
    # remove bits from the keyword args that will bork the metadata call
    utils.remove_internal_attrs(kwargs)

    # now create the API instance
    api = AwsDataAPI(**kwargs)

    return api


# Main class implementing Data API functionality
class AwsDataAPI:
    _full_config = None
    _app = None
    _region = None
    _deployment_stage = None
    _deployed_account = None
    _gremlin_address = None
    _gremlin_endpoint = None
    _es_client = None
    _search_config = None
    _storage_handler = None
    _catalog_database = None
    _api_name = None
    _table_name = None
    _pk_name = None
    _sts_client = None
    _cwl_client = None
    _log_group_name = None
    _last_log_info = None
    _caller_identity = None
    _simple_identity = None
    _logger = None
    _delete_mode = None
    _allow_runtime_delete_mode_change = False
    _crawler_rolename = None
    _table_indexes = None
    _metadata_indexes = None
    _schema_validation_refresh_hitcount = None
    _allow_non_itemmaster_writes = None
    _strict_occv = None
    _dynamo_helper = None
    _lambda_client = None
    _cloudwatch_emitter = None
    _api_metadata_handler = None

    def __init__(self, **kwargs):
        self._region = kwargs.get(params.REGION, os.getenv('AWS_REGION'))

        self._full_config = kwargs
        self._api_name = kwargs.get('ApiName')
        self._table_name = kwargs.get(params.STORAGE_TABLE)

        # setup instance logger
        logging.basicConfig()
        self._logger = logging.getLogger(params.AWS_DATA_API_NAME)
        self._logger.setLevel(logging.INFO)
        global log
        log = self._logger
        log_level = os.getenv(params.LOG_LEVEL_PARAM)
        if log_level is not None:
            log.setLevel(log_level.upper())

        # create the API metadata handler
        self._api_metadata_handler = ApiMetadata(self._region, self._logger)

        log.debug("Instantiating new Data API Namespace")
        log.debug(kwargs)

        # Load class properties from any supplied metadata. These will be populated when hydrating an existing API
        # namespace from DynamoDB
        self._app = kwargs.get(params.APP, None)
        self._deployment_stage = kwargs.get(params.STAGE)
        self._pk_name = kwargs.get(params.PRIMARY_KEY, None)
        self._delete_mode = kwargs.get(params.DELETE_MODE, params.DEFAULT_DELETE_MODE)
        self._allow_runtime_delete_mode_change = kwargs.get(params.ALLOW_RUNTIME_DELETE_MODE_CHANGE,
                                                            params.DEFAULT_ALLOW_RUNTIME_DELETE_MODE_CHANGE)
        self._crawler_rolename = kwargs.get(params.CRAWLER_ROLENAME, None)
        self._table_indexes = kwargs.get(params.TABLE_INDEXES, None)
        self._metadata_indexes = kwargs.get(params.METADATA_INDEXES, None)
        self._schema_validation_refresh_hitcount = kwargs.get(params.SCHEMA_VALIDATION_REFRESH_HITCOUNT,
                                                              params.DEFAULT_SCHEMA_VALIDATION_REFRESH_HITCOUNT)
        self._gremlin_address = kwargs.get(params.GREMLIN_ADDRESS, None)
        self._allow_non_itemmaster_writes = kwargs.get(params.NON_ITEM_MASTER_WRITES_ALLOWED,
                                                       params.DEFAULT_NON_ITEM_MASTER_WRITE_ALLOWED)
        self._strict_occv = kwargs.get(params.STRICT_OCCV, params.DEFAULT_STRICT_OCCV)
        self._catalog_database = kwargs.get(params.CATALOG_DATABASE, params.DEFAULT_CATALOG_DATABASE)

        # setup the storage handler which implements the backend data api functionality
        self._storage_handler = self._get_storage_handler(self._table_name,
                                                          self._pk_name,
                                                          self._region,
                                                          self._delete_mode,
                                                          self._allow_runtime_delete_mode_change,
                                                          self._table_indexes,
                                                          self._metadata_indexes,
                                                          self._schema_validation_refresh_hitcount,
                                                          self._crawler_rolename,
                                                          self._catalog_database,
                                                          self._allow_non_itemmaster_writes,
                                                          self._strict_occv,
                                                          kwargs.get(params.DEPLOYED_ACCOUNT, None),
                                                          kwargs[params.STORAGE_HANDLER],
                                                          pitr_enabled=bool(kwargs.get(params.PITR_ENABLED,
                                                                                       params.DEFAULT_PITR_ENABLED)),
                                                          kms_key_arn=kwargs.get(params.STORAGE_CRYPTO_KEY_ARN, None))

        # setup the gremlin integration if one has been provided
        if self._gremlin_address is not None:
            log.info(f"Binding new Gremlin Handler to address {self._gremlin_address}")
            tokens = self._gremlin_address.split(":")
            self._gremlin_endpoint = GremlinHandler(url=tokens[0], port=tokens[1])

        if "SearchConfig" in kwargs:
            self._search_config = kwargs.get("SearchConfig")

        log.info(f"AWS Data API for {self._catalog_database}.{self._table_name} Online.")

    # method which writes a set of object references to the Gremlin helper class
    def _put_references(self, id, reference_doc):
        g = self._gremlin_endpoint
        if g is not None:
            from_id = utils.get_arn(id, self._table_name, self._deployed_account)

            for r in reference_doc:
                if params.RESOURCE not in r:
                    raise InvalidArgumentsException(f"Malformed Reference: {r}. Must Contain a {params.RESOURCE}")
                else:
                    to_id = r[params.RESOURCE]

                    # remove the resource and ID keys so we can use the rest of the document for extra properties
                    del r[params.RESOURCE]

                    g.create_relationship(label=params.REFERENCES, from_id=from_id, to_id=to_id, extra_properties=r)
        else:
            raise UnimplementedFeatureException(NO_GREMLIN)

    def _get_storage_handler(self, table_name, primary_key_attribute, region, delete_mode,
                             allow_runtime_delete_mode_change, table_indexes,
                             metadata_indexes, schema_validation_refresh_hitcount, crawler_rolename, catalog_database,
                             allow_non_itemmaster_writes, strict_occv, deployed_account, handler_name,
                             pitr_enabled=None, kms_key_arn=None):
        """
        Method to load a Storage Handler class based upon the configured handler name.

        :param table_name:
        :param primary_key_attribute:
        :param region:
        :param delete_mode:
        :param allow_runtime_delete_mode_change:
        :param table_indexes:
        :param metadata_indexes:
        :param schema_validation_refresh_hitcount:
        :param crawler_rolename:
        :param catalog_database:
        :param allow_non_itemmaster_writes:
        :param strict_occv:
        :param deployed_account:
        :param handler_name:
        :param pitr_enabled:
        :param kms_key_arn:
        :return:
        """
        log.info(f"Creating new Data API Storage Handler from {handler_name}")
        sys.path.append("chalicelib")
        storage_module = __import__(handler_name)
        storage_class = getattr(storage_module, "DataAPIStorageHandler")

        return storage_class(table_name, primary_key_attribute,
                             region,
                             delete_mode,
                             allow_runtime_delete_mode_change,
                             table_indexes,
                             metadata_indexes,
                             schema_validation_refresh_hitcount,
                             crawler_rolename,
                             catalog_database,
                             allow_non_itemmaster_writes,
                             strict_occv,
                             deployed_account,
                             pitr_enabled,
                             kms_key_arn)

    # simple accessor method for the pk_name attribute, which is required in some cases for API integration
    def get_primary_key(self):
        return self._pk_name

    # access method that returns a boolean outcome based upon if the provided ID is valid
    # @evented(api_operation="Check")
    @identity_trace
    def check(self, id):
        return self._storage_handler.check(id=id)

    # return a paginated list of elements from the API
    # @evented(api_operation="List")
    @identity_trace
    def list(self, **kwargs):
        return self._storage_handler.list_items(**kwargs)

    # return information about storage usage for this API namespace
    # @evented(api_operation="Usage")
    @identity_trace
    def get_usage(self):
        resources = self._storage_handler.get_usage(table_name=self._table_name)
        metadata = self._storage_handler.get_usage(table_name=utils.get_metaname(self._table_name))

        references = None
        # TODO figure out why the gremlin connection is failing
        # if self._gremlin_endpoint is not None:
        #    references = self._gremlin_endpoint.get_usage()

        usage = {
            params.RESOURCE: resources,
            params.METADATA: metadata
        }

        if references is not None:
            usage[params.REFERENCES] = {"Count": references}

        return usage

    # run the natural language understanding integration, which attaches new Metadata to the Resource
    # @evented(api_operation="Understand")
    @identity_trace
    def understand(self, id, storage_location=None):
        fetch_id = self._validate_arn_id(id)

        # validate the attribute that stores the location of the object
        if storage_location is None:
            storage_location = params.DEFAULT_STORAGE_LOCATION_ATTRIBUTE

        # fetch the resource
        item = self._storage_handler.get(id=fetch_id)

        storage_loc = None

        if item is None:
            raise ResourceNotFoundException(f"Unable to find Resource with ID {fetch_id}")
        else:
            if storage_location in item[params.RESOURCE]:
                storage_loc = item.get(params.RESOURCE).get(storage_location)
            else:
                # storage location may be in metadata
                meta = self._storage_handler.get_metadata(id)
                if storage_location in meta[params.METADATA]:
                    storage_loc = meta.get(params.METADATA).get(storage_location)

        if storage_loc is None:
            raise DetailedException(
                f"Unable to run Metadata Resolver without a Storage Location Attribute in Item Resource or Metadata (Default {params.DEFAULT_STORAGE_LOCATION_ATTRIBUTE})")

        if self._lambda_client is None:
            self._lambda_client = boto3.client("lambda", region_name=self._region)

        # run the understander and metadata update through an async lambda
        f = f"{params.AWS_DATA_API_NAME}-{self._deployment_stage}-{params.UNDERSTANDER_NAME}"

        args = {
            "prefix": storage_loc,
            "id": fetch_id,
            "caller": self._simple_identity,
            "primary_key_attribute": self._pk_name,
            "ApiName": self._api_name,
            "ApiStage": self._deployment_stage
        }
        response = self._lambda_client.invoke(FunctionName=f, InvocationType='Event', Payload=json.dumps(args))

        if "FunctionError" in response:
            if response.get("FunctionError") == "Handled":
                raise DetailedException(response.get("Payload"))
            else:
                raise DetailedException("Unhandled error occurred during submission of async Understanding request")
        else:
            return response.get("StatusCode")

    def _validate_arn_id(self, id):
        # decode the ID as it forms part of the request url
        decoded_id = parser.unquote(id)

        log.debug(f"Validating Resource ARN {id}")

        if utils.get_arn_base() in decoded_id:
            # validate arn structure and then fetch by id
            arn = utils.shred_arn(decoded_id)

            if arn is None:
                raise ResourceNotFoundException(f"Invalid ARN format {decoded_id}")

            if utils.get_caller_account() != arn[params.ARN_ACCOUNT]:
                raise ResourceNotFoundException("Requested resource not available from Data API Account")

            if self._table_name != arn[params.ARN_TABLE]:
                self.exception = ResourceNotFoundException(
                    f"Requested resource {arn[params.ARN_TABLE]} not available from Data API {self._table_name}")
                raise self.exception

            if arn[params.ARN_REGION] != self._region:
                raise ResourceNotFoundException(f"ARN Valid Region {arn[params.ARN_REGION]}")

            fetch_id = arn[params.ARN_ID]
        else:
            fetch_id = decoded_id

        return fetch_id

    # get the Resource, which may include or prefer the Item Master
    # @evented(api_operation="GetResource")
    @identity_trace
    def get(self, id, master_option, suppress_meta_fetch: bool = False):
        fetch_id = self._validate_arn_id(id)
        response = {}
        item = self._storage_handler.get(id=fetch_id, suppress_meta_fetch=suppress_meta_fetch)

        # set the 'Item' in the response unless master_option = prefer
        if params.ITEM_MASTER_ID not in item[params.RESOURCE] or \
                master_option is None or \
                master_option.lower() == params.ITEM_MASTER_INCLUDE.lower():
            response["Item"] = item

        # extract the master if there is one, and the provided master option is 'include' or 'prefer'
        # TODO Test what happens if we have very large Item Master hierarchies here
        if params.ITEM_MASTER_ID in item[params.RESOURCE] and master_option is not None and master_option.lower() in [
            params.ITEM_MASTER_INCLUDE.lower(),
            params.ITEM_MASTER_PREFER.lower()]:
            master = self._storage_handler.get(id=item[params.RESOURCE][params.ITEM_MASTER_ID])
            response["Master"] = master

        return response

    # undelete a Data API Resource that has been soft deleted (non-Tombstone)
    # @evented(api_operation="Restore")
    @identity_trace
    def restore(self, id):
        fetch_id = self._validate_arn_id(id)

        return self._storage_handler.restore(id=fetch_id, caller_identity=self._simple_identity)

    # get the Metadata for a Resource
    # @evented(api_operation="GetMetadata")
    @identity_trace
    def get_metadata(self, id):
        fetch_id = self._validate_arn_id(id)

        return self._storage_handler.get_metadata(id=fetch_id)

    # Delete a Resource and Metadata based upon the specified deletion mode of the system or in the request
    # @evented(api_operation="Delete")
    @identity_trace
    def delete(self, id, **kwargs):
        fetch_id = self._validate_arn_id(id)

        return self._storage_handler.delete(id=fetch_id, caller_identity=self._simple_identity, **kwargs)

    # Update a Data API Resource
    # @evented(api_operation="Update")
    @identity_trace
    def update_item(self, id, **kwargs):
        fetch_id = self._validate_arn_id(id)
        response = None

        # TODO move to Data API
        if params.REFERENCES in kwargs:
            log.debug("Creating Reference Links")
            self._put_references(id, kwargs.get(params.REFERENCES))

        response = self._storage_handler.update_item(caller_identity=self._simple_identity, id=fetch_id, **kwargs)

        return response

    # Drop an entire API Namespace. This will do a backup before dropping the underlying storage tables
    # @evented(api_operation="DropAPI")
    @identity_trace
    def drop(self, do_export=True):
        # drop tables with final backup
        self._storage_handler.drop_table(table_name=self._table_name, do_export=do_export)
        self._storage_handler.drop_table(table_name=utils.get_metaname(self._table_name), do_export=do_export)

        # delete API information
        self._api_metadata_handler.delete_all_api_metadata(self._api_name, self._deployment_stage)

    # Perform a search request against the Resource or Metadata, based upon provided query args
    # @evented(api_operation="Find")
    @identity_trace
    def find(self, **kwargs):
        return self._storage_handler.find(**kwargs)

    def _get_es_endpoint(self):
        return self._search_config.get("ElasticSearchDomain").get("ElasticSearchEndpoint")

    # private lazy loader method for es client to ensure that we don't get constructor stalls if VPC connections are weird
    def _get_es_client(self):
        if self._es_client is None:
            # setup a reference to ElasticSearch if a SearchConfig is setup
            self._es_client = Elasticsearch(hosts=[self._get_es_endpoint()])

        return self._es_client

    # Perform a search request against the configured ES endpoint
    # @evented(api_operation="Search")
    @identity_trace
    def search(self, search_type, **kwargs):
        if self._es_client is None:
            raise UnimplementedFeatureException("No ElasticSearch Endpoint Configured")
        else:
            response = {}

            def _add_results(result_type):
                index_name = utils.get_es_index_name(self._table_name, result_type)
                doc = utils.get_es_type_name(self._table_name, result_type),

                response[result_type] = self._get_es_client().search(index=index_name, doc_type=doc,
                                                                     body=kwargs.get("query"))

            if search_type is not None:
                # perform a search just for the specified type of data
                _add_results(search_type)
            else:
                # perform a search across both Resource and Metadata indexes
                _add_results(params.RESOURCE)
                _add_results(params.METADATA)

            return response

    # Return the API's underlying storage implementations, including tables in use, Dynamo Streams that can be processed
    # and references to Gremlin and ElasticSearch endpoints in use
    # @evented(api_operation="Endpoints")
    @identity_trace
    def get_endpoints(self):
        endpoints = self._storage_handler.get_streams()

        if self._gremlin_address is not None:
            endpoints['GraphURL'] = self._gremlin_address

        if self._search_config is not None:
            endpoints['Elasticsearch'] = self._get_es_endpoint()

        return endpoints

    # Return the JSON schema for an API Namespace
    # @evented(api_operation="GetSchema")
    @identity_trace
    def get_schema(self, schema_type):
        return self._api_metadata_handler.get_schema(api_name=self._api_name, stage=self._deployment_stage,
                                                     schema_type=schema_type)

    # Create or Update a JSON Schema for the API Namespace Resources or Metadata
    # @evented(api_operation="PutSchema")
    @identity_trace
    def put_schema(self, schema_type, schema):
        return self._api_metadata_handler.put_schema(api_name=self._api_name, stage=self._deployment_stage,
                                                     schema_type=schema_type,
                                                     caller_identity=self._simple_identity, schema=schema)

    # Remove the JSON Schema from the Namespace for Resources or Metadata
    # @evented(api_operation="DeleteSchema")
    @identity_trace
    def remove_schema(self, schema_type):
        if schema_type.lower() == params.RESOURCE.lower():
            set_schema_type = params.CONTROL_TYPE_RESOURCE_SCHEMA
        elif schema_type.lower() == params.METADATA.lower():
            set_schema_type = params.CONTROL_TYPE_METADATA_SCHEMA
        else:
            raise InvalidArgumentsException(
                f"Schema Type {schema_type} invalid. Use {params.CONTROL_TYPE_METADATA_SCHEMA} or {params.CONTROL_TYPE_RESOURCE_SCHEMA}")

        return self._api_metadata_handler.delete_metadata(api_name=self._api_name, stage=self._deployment_stage,
                                                          metadata_type=set_schema_type,
                                                          caller_identity=self._simple_identity)

    # Setup the Item Master for a given Resource
    # @evented(api_operation="SetItemMaster")
    @identity_trace
    def item_master_update(self, **kwargs):
        return self._storage_handler.item_master_update(caller_identity=self._simple_identity, **kwargs)

    # Remote the specified Item Master for a given Resource
    # @evented(api_operation="RemoveItemMaster")
    @identity_trace
    def item_master_delete(self, **kwargs):
        item_id = kwargs.get(self._pk_name)

        if item_id is None:
            raise ResourceNotFoundException
        else:
            # validate that this item actually has the correct item master set
            current = self._storage_handler.get(id=item_id)
            assert_item_master = kwargs.get(params.ITEM_MASTER_ID)
            current_master = current.get(params.RESOURCE).get(params.ITEM_MASTER_ID, None)
            if current_master is None:
                return True
            elif current_master != assert_item_master:
                raise InvalidArgumentsException("Item Master {assert_item_master} does not match actual Item Master")
            else:
                return self._storage_handler.remove_resource_attributes(id=item_id,
                                                                        resource_attributes=[params.ITEM_MASTER_ID],
                                                                        caller_identity=self._simple_identity)

    # Extract the Metadata for the API itself
    # @evented(api_operation="GetApiMetadata")
    @identity_trace
    def get_table_metadata(self, attribute_filters=None):
        return self._api_metadata_handler.get_api_metadata(api_name=self._api_name, stage=self._deployment_stage,
                                                           attribute_filters=attribute_filters)

    # Create or Update API Metadata
    # @evented(api_operation="CreateApiMetadata")
    @identity_trace
    def create_table_metadata(self, caller_identity=None, **kwargs):
        try:
            return self._dynamo_helper.create_table_metadata(api_name=self._table_name,
                                                             caller_identity=self._simple_identity if caller_identity is None else caller_identity,
                                                             **kwargs)
        except Exception as e:
            raise DetailedException(e)

    # Perform a search for all References in the Gremlin DB for objects that directly or indirectly reference an API Item
    # @evented(api_operation="GetDownstreamReferences")
    @identity_trace
    def get_downstream(self, id, search_depth=1):
        if self._gremlin_endpoint is not None:
            if id is None:
                raise InvalidArgumentsException("Must have ID to run lineage search")
            else:
                try:
                    return self._gremlin_endpoint.get_outbound(
                        id=utils.get_arn(id, self._table_name, self._deployed_account),
                        search_depth=search_depth)
                except ResourceNotFoundException:
                    return None
                except Exception as e:
                    raise DetailedException(e)
        else:
            raise UnimplementedFeatureException(params.NO_GREMLIN)

    # Perform a search for all References that the provided API Item references, directly or indirectly
    # @evented(api_operation="GetUpstreamReferences")
    @identity_trace
    def get_upstream(self, id, search_depth=1):
        if self._gremlin_endpoint is not None:
            if id is None:
                raise InvalidArgumentsException("Must have ID to run lineage search")
            else:
                try:
                    return self._gremlin_endpoint.get_inbound(
                        id=utils.get_arn(id, self._table_name, self._deployed_account),
                        search_depth=search_depth)
                except ResourceNotFoundException:
                    return None
                except Exception as e:
                    raise DetailedException(e)
        else:
            raise UnimplementedFeatureException(params.NO_GREMLIN)

    def _do_ddb_export_to_s3(self, table_name, export_path, log_path, read_pct, dpu,
                             kms_key_arn, setup_crawler, catalog_database=None):
        if setup_crawler is True and self._crawler_rolename is None:
            raise InvalidArgumentsException(
                "Cannot Setup Crawler for Exported Dataset as API is not configured with a Crawler Role")

        set_table_name = f"{table_name}_{utils.get_date_now()}"
        export = utils.run_glue_export(table_name=set_table_name, s3_export_path=export_path,
                                       kms_key_arn=kms_key_arn,
                                       read_pct=read_pct, log_path=log_path, export_role=self._crawler_rolename,
                                       dpu=dpu)

        if setup_crawler is not None:
            crawler = utils.create_s3_crawler(target_entity_name=set_table_name,
                                              crawler_name=f"{table_name}-export",
                                              crawler_rolename=self._crawler_rolename,
                                              catalog_db=f"{self._catalog_database}-export" if catalog_database is None else catalog_database,
                                              s3_path=export_path,
                                              and_run=True)

            if crawler is not None:
                export['Crawler'] = crawler
            else:
                msg = "Unable to configure Export Location Crawler"
                export['Errors'] = [{"Error": msg}]
                raise DetailedException(message=msg, detail=export)

        return export

    # Get the status of an API Export to S3
    # @evented(api_operation="GetExportStatus")
    @identity_trace
    def get_export_job_status(self, job_name, run_id):
        return utils.get_glue_job_status(job_name=job_name, run_id=run_id)

    # Get a list of all export jobs running
    # @evented(api_operation="GetExportJobs")
    @identity_trace
    def get_running_export_jobs(self, job_name):
        return utils.get_running_export_jobs(job_name=job_name)

    # Start an export of API Data to S3
    # @evented(api_operation="StartExport")
    @identity_trace
    def export_to_s3(self, **kwargs):
        EXPORT_DATA = 'Data'
        EXPORT_META = 'Metadata'
        EXPORT_ALL = 'All'
        export_path = kwargs.get(params.EXPORT_S3_PATH)
        if export_path is None:
            raise Exception("Cannot export without S3 Export Path")
        dpu = int(kwargs.get(params.EXPORT_JOB_DPU, params.DEFAULT_EXPORT_DPU))
        kms_key_arn = kwargs.get(params.KMS_KEY_ARN, None)
        read_pct = int(kwargs.get(params.EXPORT_READ_PCT, 50))
        log_path = kwargs.get(params.EXPORT_LOG_PATH)
        export_type = kwargs.get(params.EXPORT_TYPE, EXPORT_DATA)
        catalog_database = kwargs.get(params.CATALOG_DATABASE)

        export_types = [EXPORT_DATA, EXPORT_META, EXPORT_ALL]
        if not any(x in export_type for x in export_types):
            raise InvalidArgumentsException("ExportType must be one of {0}, {1}, or {2}" % tuple(export_types))

        def _fix_path(path):
            if path[:1] != "/":
                path += "/"

        _fix_path(export_path)

        crawl = kwargs.get(params.EXPORT_SETUP_CRAWLER, None)

        out = {}
        # export main data to s3 location
        if export_type == EXPORT_DATA or export_type == EXPORT_ALL:
            result = self._do_ddb_export_to_s3(table_name=self._table_name, export_path=export_path,
                                               log_path=log_path,
                                               read_pct=read_pct, dpu=dpu,
                                               kms_key_arn=kms_key_arn, setup_crawler=crawl,
                                               catalog_database=catalog_database)
            if result is not None:
                out[EXPORT_DATA] = result

        # export metadata to S3
        if export_type == EXPORT_META or export_type == EXPORT_ALL:
            result = self._do_ddb_export_to_s3(table_name=utils.get_metaname(self._table_name),
                                               export_path=export_path,
                                               log_path=log_path,
                                               read_pct=read_pct, dpu=dpu,
                                               kms_key_arn=kms_key_arn, setup_crawler=crawl,
                                               catalog_database=catalog_database)
            if result is not None:
                out[EXPORT_META] = result

        return out

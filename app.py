# this file contains all of the Chalice routing logic to implement the AWS Data API as a REST JSON Endpoint using IAM
# authentication. The Data API can also be accessed natively as a python library using aws_data_api.py
from chalice import Chalice, CORSConfig, Response, IAMAuthorizer, CognitoUserPoolAuthorizer, BadRequestError, ConflictError, NotFoundError
import http
import os
import boto3
from functools import wraps
import chalicelib.utils as utils
from chalicelib.api_metadata import ApiMetadata
import chalicelib.parameters as params
# TODO move data api implementation to cell based layers
import chalicelib.aws_data_api as dapi
from chalicelib.streams_integration import StreamsIntegration
from chalicelib.exceptions import *
import chalicelib.understander as u
from chalicelib.data_api_cache import DataApiCache
import json

# this environment variable is setup by AWS Lambda
REGION = os.getenv('AWS_REGION')

# this environment variable comes from Chalice
STAGE = os.getenv('STAGE')

# setup authorisers for view methods
iam_authorizer = IAMAuthorizer()
cognito_authorizer = CognitoUserPoolAuthorizer()

set_authorizer = os.getenv(params.AUTHORIZER_PARAM)
if set_authorizer == params.AUTHORIZER_IAM:
    use_authorizer = iam_authorizer
elif set_authorizer == params.AUTHORIZER_COGNITO:
    use_authorizer = cognito_authorizer
elif set_authorizer == params.AUTHORIZER_CUSTOM:
    # TODO figure out how to dynamically load a custom authorizer here
    pass
else:
    use_authorizer = None

# create the Chalice App reference
app_name = "%s-%s" % (params.AWS_DATA_API_SHORTNAME, STAGE)
app = Chalice(app_name=app_name)

# TODO Get rid of this
dynamo_resource = boto3.resource('dynamodb', region_name=REGION)

# setup class logger
log = utils.setup_logging()

# create an API Metadata Handler
api_metadata_handler = ApiMetadata(REGION, log)

# create a cache of all API references tracked by this deployment stage
api_cache = DataApiCache(app=app, stage=STAGE, region=REGION, logger=log)

# create the streams integration handler, which is used by the lambda function embedded at the end of this app
es_indexer = None

# module level settings used as flags for lazy initialisers in functions
search_flow_verified = False

# load the cors config
cors_config = None
with open("chalicelib/cors.json", "r") as f:
    cors_config = json.load(f)

if cors_config.get("AllowAllCORS") == "True":
    cors = True
else:
    cors = CORSConfig(**cors_config.get("Chalice"))


# using a functools wrapper here as normal python decorators aren't compatible with the call signature of chalice
def chalice_function(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        headers = {'Content-Type': 'text/json'}
        try:
            result = f(*args, **kwargs)
            status_code = http.HTTPStatus.OK
            body = None

            if result is None:
                status_code = http.HTTPStatus.NO_CONTENT
            else:
                if isinstance(result, Response):
                    return result
                elif isinstance(result, int):
                    return Response(body=body,
                                    status_code=result,
                                    headers={'Content-Type': 'text/plain'})
                elif isinstance(result, bool):
                    if result is True:
                        status_code = http.HTTPStatus.CREATED
                    else:
                        status_code = http.HTTPStatus.NO_CONTENT
                else:
                    if params.RESPONSE_BODY in result:
                        body = result[params.RESPONSE_BODY]

                        if params.DATA_MODIFIED in result and result.get(params.DATA_MODIFIED) is True:
                            status_code = http.HTTPStatus.CREATED
                        elif params.DATA_MODIFIED in result and result.get(params.DATA_MODIFIED) is False:
                            status_code = http.HTTPStatus.NOT_MODIFIED
                    else:
                        body = utils.decorate(result)

            return Response(body=body,
                            status_code=status_code,
                            headers=headers)
        except ConstraintViolationException as cve:
            log.error(str(cve))
            raise ConflictError(cve)
        except (UnimplementedFeatureException, ResourceNotFoundException) as ufe:
            log.error(str(ufe))
            raise NotFoundError(ufe)
        except InvalidArgumentsException as iae:
            log.error(str(iae))
            raise BadRequestError(iae)
        except DetailedException as ge:
            log.error(str(ge))
            return Response(body=str({"message": ge.message, "detail": ge.detail}),
                            headers={'Content-Type': 'text/json'},
                            status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

    return wrapper


def _add_api_defaults(api_metadata):
    def _add(name, value):
        if value is not None:
            api_metadata[name] = value

    # add all the application level defaults if they aren't overridden in the supplied API Metadata
    _add(params.REGION, REGION)
    _add(params.STAGE, STAGE)


# TODO Can we add a method to query what are the available stages?
@app.route('/namespaces', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def registry():
    return dapi.get_registry(REGION, STAGE)


@app.route('/data-apis', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_all():
    return utils.get_all_data_apis()


@app.route('/{api_name}/provision', methods=['PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def provision_api(api_name):
    body = app.current_request.json_body

    return dapi.async_provision(api_name=api_name, stage=STAGE, region=REGION, logger=log, **body)


@app.route('/{api_name}/drop', methods=['PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def drop_api(api_name):
    do_export = True
    qp = app.current_request.query_params
    df = "DoFinalExport"
    if qp is not None and df in qp:
        do_export = bool(qp.get(df))
    return api_cache.get(api_name).drop(do_export=do_export)


@app.route("/{api_name}/status", methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_api_status(api_name):
    # have to call a non-class method here as the API may not be online and therefore not in cache
    return dapi.get_api_status(api_name, stage=STAGE, region=REGION)


# method to get and create API level metadata - not per-item
@app.route('/{api_name}/info', methods=['GET', 'PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_table_metadata(api_name):
    request = app.current_request

    if request.method == 'GET':
        return api_metadata_handler.get_api_metadata(api_name=api_name, stage=STAGE)
    else:
        response = api_metadata_handler.update_metadata(api_name=api_name, stage=STAGE, updates=request.json_body)

        if response is not None:
            # remove the API from the cache, so that we will reinstantiate the reference on next call
            api_cache.remove(api_name)

            return {params.DATA_MODIFIED: True,
                    params.RESPONSE_BODY: response}
        else:
            return None


@app.route('/{api_name}/usage', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_usage(api_name):
    return api_cache.get(api_name).get_usage()


# method to get and create API level metadata - not per-item
@app.route('/{api_name}/{id}/understand', methods=['PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def run_understanding(api_name, id):
    body = app.current_request.json_body

    return api_cache.get(api_name).understand(id=id, storage_location=body.get(params.STORAGE_LOCATION_ATTRIBUTE,
                                                                               params.DEFAULT_STORAGE_LOCATION_ATTRIBUTE))


# method to get and create API level schema information
@app.route('/{api_name}/schema/{schema_type}', methods=['GET', 'PUT', 'DELETE'], authorizer=use_authorizer, cors=cors)
@chalice_function
def schema(api_name, schema_type):
    if schema_type is None or not any([x.lower() in schema_type.lower() for x in [params.RESOURCE, params.METADATA]]):
        raise BadRequestError("Must supply a schema type of Resource or Metadata")

    request = app.current_request
    api = api_cache.get(api_name)

    if request.method == 'GET':
        return api.get_schema(schema_type)
    elif request.method == 'DELETE':
        delete_performed = api.remove_schema(schema_type)
        return {params.DATA_MODIFIED: delete_performed}
    else:
        return {params.DATA_MODIFIED: True,
                params.RESPONSE_BODY: api.put_schema(schema_type=schema_type,
                                                     schema=app.current_request.json_body)}


# method to set an ItemMaster ID on an Item
@app.route('/{api_name}/ItemMaster', methods=['DELETE', 'PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_item_master(api_name):
    api = api_cache.get(api_name)

    if app.current_request.method == 'PUT':
        return {params.DATA_MODIFIED: True,
                params.RESPONSE_BODY: api.item_master_update(**app.current_request.json_body)}
    else:
        return {params.DATA_MODIFIED: api.item_master_delete(**app.current_request.json_body)}


# method to perform a simple query based on the attributes supplied in the request body
@app.route('/{api_name}/find', methods=['POST'], authorizer=use_authorizer, cors=True)
@chalice_function
def find_item(api_name):
    return api_cache.get(api_name).find(**app.current_request.json_body)


# method to perform an elasticsearch query
@app.route('/{api_name}/search/{search_type}', methods=['PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def api_search(api_name, search_type):
    return api_cache.get(api_name).search(search_type, **app.current_request.json_body)


# method to get, delete, and check for an item with ID from the body/uri
@app.route('/{api_name}', methods=['GET', 'DELETE', 'HEAD', 'PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_general_request(api_name):
    # try to extract the ID from where it might be stored
    api = api_cache.get(api_name)

    primary_key_attribute = api.get_primary_key()
    item_id = None

    if any([x in app.current_request.method for x in ['GET', 'HEAD']]):
        if primary_key_attribute in app.current_request.query_params:
            item_id = app.current_request.query_params.get(primary_key_attribute)
    else:
        # PUT or DELETE
        resource_body = app.current_request.json_body.get(params.RESOURCE)
        if primary_key_attribute in app.current_request.json_body:
            item_id = app.current_request.json_body.get(primary_key_attribute)
            del app.current_request.json_body[primary_key_attribute]
        elif primary_key_attribute in resource_body:
            item_id = resource_body.get(primary_key_attribute)

            del resource_body[primary_key_attribute]

    if item_id is None:
        raise InvalidArgumentsException("Unable to resolve Primary Key for Request")
    else:
        return process_item_request(api_name, item_id)


# method to get, delete, and check for an item based on its ID
@app.route('/{api_name}/{id}', methods=['GET', 'DELETE', 'HEAD', 'PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_item_request(api_name, id):
    if id is None:
        raise InvalidArgumentsException("Unable to transact without Primary Key for Item")

    request = app.current_request
    api = api_cache.get(api_name)

    if request.method == 'GET':
        master = None
        qp = app.current_request.query_params
        if qp is not None and params.ITEM_MASTER_QP in qp:
            master = qp[params.ITEM_MASTER_QP]

        return api.get(id=id, master_option=master)
    elif request.method == 'DELETE':
        return {params.DATA_MODIFIED: api.delete(id=id, **request.json_body)}
    elif request.method == 'HEAD':
        return api.check(id=id)
    elif request.method == 'PUT':
        return {params.DATA_MODIFIED: True,
                "Messages": api.update_item(id=id, **request.json_body)}


# TODO Add a non-id based URL path for this operation
# method to restore an object from deletion
@app.route('/{api_name}/{id}/restore', methods=['PUT'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_restore_request(api_name, id):
    return {params.DATA_MODIFIED: True,
            params.RESPONSE_BODY: api_cache.get(api_name).restore(id=id)}


# method to retrieve metadata only for an item
@app.route('/{api_name}/{id}/meta', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_meta_request(api_name, id):
    return api_cache.get(api_name).get_metadata(id=id)


# method to paginate a bunch of items
@app.route('/{api_name}/list', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def process_list(api_name):
    query_params = app.current_request.query_params

    if query_params is None:
        query_params = {}
    return api_cache.get(api_name).list(**query_params)


# method to get stream information for an API
@app.route('/{api_name}/endpoints', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_endpoints(api_name):
    # get API endpoints
    return api_cache.get(api_name).get_endpoints()


# method to fetch all Items that are descended from the supplied Item
@app.route('/{api_name}/{id}/downstream', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_downstream(api_name, id):
    request = app.current_request
    search_depth = None
    if request.query_params is not None and 'search_depth' in request.query_params:
        search_depth = request.query_params['search_depth']
    return api_cache.get(api_name).get_downstream(id=id, search_depth=search_depth)


# method to fetch all Items that were used to create Items which led to the supplied Item
@app.route('/{api_name}/{id}/upstream', methods=['GET'], authorizer=use_authorizer, cors=cors)
@chalice_function
def get_upstream(api_name, id):
    request = app.current_request
    search_depth = None
    if request.query_params is not None and 'search_depth' in request.query_params:
        search_depth = request.query_params['search_depth']
    return api_cache.get(api_name).get_upstream(id=id, search_depth=search_depth)


# method to export a table to S3, or to determine export job status
@app.route('/{api_name}/export', methods=['GET', 'POST'], authorizer=use_authorizer, cors=cors)
@chalice_function
def export(api_name):
    if app.current_request.method == 'GET':
        p = app.current_request.query_params

        job_name = p.get('JobName', None)
        run_id = p.get('JobRunId', None)

        if job_name is not None and run_id is not None:
            return api_cache.get(api_name).get_export_job_status(job_name=job_name, run_id=run_id)
        elif job_name is not None and run_id is None:
            # get all running jobs for the job name
            return api_cache.get(api_name).get_running_export_jobs(job_name=job_name)
        else:
            raise BadRequestError("Must supply JobName and optionally JobRunId ")
    else:
        request_params = app.current_request.json_body

        return api_cache.get(api_name).export_to_s3(**request_params)


# lambda function to act as an indexer for the update stream from this table
@app.lambda_function(params.INDEXER_NAME)
def indexing_lambda(event, context):
    if 'Records' in event:
        global search_flow_verified
        global es_indexer

        if search_flow_verified is False:
            # determine the source API information to ensure the search flow is configured
            api_name = None
            event_source_tokens = event.get("eventSourceARN").split(":")
            if event_source_tokens[2] == "dynamodb":
                table_spec_tokens = event_source_tokens[5].split("/")
                table = table_spec_tokens[1]

            # create an API Metadata Handler
            api_metadata_handler = ApiMetadata(REGION, log)

            api_metadata = api_metadata_handler.get_api_metadata(api_name, STAGE)

            # verify that the delivery streams are in place
            es_indexer.configure_search_flow(endpoints=api_metadata.get("SearchConfig").get("DeliveryStreams"),
                                             es_domain_name=event[params.ES_DOMAIN],
                                             firehose_delivery_role_arn=event[
                                                 params.FIREHOSE_DELIVERY_ROLE_ARN],
                                             failure_record_bucket=event[
                                                 params.DELIVERY_STREAM_FAILURE_BUCKET]
                                             )
            es_indexer = StreamsIntegration(STAGE, api_metadata.get("SearchConfig"))
            search_flow_verified = True

        output = es_indexer.forward_to_es_firehose(records=event['Records'])


@app.lambda_function(params.PROVISIONER_NAME)
def provisioning_lambda(event, context):
    # TODO Add support for creation of Read/Only and Read/Write IAM Roles during provisioning
    log.debug(event)
    api_name = event.get("ApiName")
    event.pop("ApiName")

    # create an API Metadata Handler
    api_metadata_handler = ApiMetadata(REGION, log)

    # check if this API is already deployed
    table_name = utils.get_table_name(table_name=api_name, deployment_stage=STAGE)
    api_metadata = api_metadata_handler.get_api_metadata(api_name, STAGE)

    if api_metadata is None:
        log.debug(f"API {api_name} not found. Creating new Data API")
        api_metadata = {}
        # add the default parameters from the application container
        _add_api_defaults(api_metadata)
        api_metadata[params.DATA_TYPE] = api_name
        api_metadata[params.DEPLOYED_ACCOUNT] = context.invoked_function_arn.split(":")[4]
        api_metadata[params.STORAGE_TABLE] = table_name
        api_metadata[params.STORAGE_HANDLER] = params.DEFAULT_STORAGE_HANDLER
        api_metadata[params.CATALOG_DATABASE] = params.DEFAULT_CATALOG_DATABASE
    else:
        log.debug(f"API {api_name} already exists. Performing property update and instance rebuild")

        # remove last update date/by information to prevent update collision
        utils.remove_internal_attrs(api_metadata)

    # overlay the supplied parameters onto the api metadata
    api_metadata.update(event)

    # add a pending status
    api_metadata['Status'] = params.STATUS_CREATING

    # add a control table entry for this stage with the current configuration
    api_metadata_handler.create_metadata(api_name=api_name, stage=STAGE, caller_identity='System', **api_metadata)

    api_metadata[params.APP] = app

    # load the api class
    api = dapi.load_api(**api_metadata)

    # setup the search flow
    search_config = None
    if params.ES_DOMAIN in event:
        try:
            search_config = es_indexer.configure_search_flow(endpoints=api.get_endpoints(),
                                                             es_domain_name=event[params.ES_DOMAIN],
                                                             firehose_delivery_role_arn=event[
                                                                 params.FIREHOSE_DELIVERY_ROLE_ARN],
                                                             failure_record_bucket=event[
                                                                 params.DELIVERY_STREAM_FAILURE_BUCKET]
                                                             )
        except KeyError:
            raise BadRequestError(
                f"Unable to provision search configuration without {params.ES_DOMAIN}, {params.FIREHOSE_DELIVERY_ROLE_ARN}, and {params.DELIVERY_STREAM_FAILURE_BUCKET}")

    # add the search config to metadata
    if search_config is not None:
        api_metadata_handler.update_metadata(api_name=api_name, stage=STAGE,
                                             updates=search_config, caller_identity='System')

    # destroy the cache reference to cause a reload on next invoke
    if api_cache.contains(api_name):
        log.debug(f"Invalidating API Cache")
        api_cache.remove(api_name)

    # update the metadata to show that the API is online
    api_metadata_handler.update_metadata(api_name=api_name, stage=STAGE,
                                         updates={"Status": params.STATUS_ACTIVE}, caller_identity='System')
    log.info(f"Provisioning complete. API {api_name} online in Stage {STAGE}")


@app.lambda_function(params.UNDERSTANDER_NAME)
def understander_lambda(event, context):
    log.debug(event)
    understander = None
    api_metadata_handler = ApiMetadata(REGION, log)

    def arg_handler(arg):
        if arg not in event:
            raise InvalidArgumentsException(f"Invocation event must include '{arg}'")
        else:
            return event.get(arg)

    prefix = arg_handler("prefix")
    id = arg_handler("id")

    if understander is None:
        understander = u.Understander(region=REGION)

    api_name = arg_handler("ApiName")
    api_stage = arg_handler("ApiStage")

    # run the understander method
    understanding = understander.understand(prefix=prefix)

    # create a metadata update structure for all non-empty understanding items
    meta_add = {}

    def _meta_adder(val):
        if val in understanding:
            v = understanding.get(val)

            # add non empty structures
            if v is not None and v != [] and v != {}:
                meta_add[val] = v

    if understanding is not None:
        # add the new metadata to the object
        [_meta_adder(x) for x in [u.PAGES,
                                  u.RAW_LINES,
                                  u.KEY_VALUES,
                                  u.ENTITIES,
                                  u.LANGUAGE,
                                  u.SENTIMENT,
                                  u.KEY_PHRASES]
         ]

        api_metadata_handler.create_metadata(api_name, api_stage, caller_identity='System', **meta_add)
        print(f"Metadata Analysis complete for {id}")

        return understanding
    else:
        return None

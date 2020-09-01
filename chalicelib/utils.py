import json
import datetime
import boto3
import os
import time
import logging
import pystache
from distutils import util as _util
from chalicelib.data_api_encoder import DataApiEncoder
import chalicelib.glue_export_dynamo_table as export_utils
from chalicelib.exceptions import DetailedException, ResourceNotFoundException
import chalicelib.parameters as params

_sts_client = None
_iam_client = None


def __precheck_config(config_dict):
    # pre-check settings for the pystache template
    if config_dict.get("stage_name").lower() != 'prod' or (
            config_dict.get("stage_name").lower() == 'prod' and config_dict.get("enable_xray") is True):
        config_dict["use_xray"] = True

    if config_dict.get("auth") is not None:
        config_dict["use_auth"] = True

    if config_dict.get("cors_domain") is not None:
        config_dict["custom_cors"] = True

    if config_dict.get("auth") == params.AUTHORIZER_COGNITO:
        if config_dict.get("cog_pool_name") is not None and config_dict.get("cog_provider_arns") is not None:
            config_dict["use_cognito"] = True
        else:
            raise Exception("Misconfigured Cognito Authorization. Requires User Pool name and Provider ARNS")

    if config_dict.get("custom_domain_name") is not None:
        config_dict["custom_domain"] = True

    if config_dict.get("custom_url_prefix") is not None:
        config_dict["use_custom_prefix"] = True


def generate_configuration_files(config_dict, generate_action, verbose):
    __precheck_config(config_dict)

    # generate the config.json file to .chalice
    __export_template_to_file("template/config.pystache", ".chalice/config.json", config_dict, generate_action, verbose)

    # generate the iam policy
    __export_template_to_file("template/iam_policy.pystache", "iam_policy.json", config_dict, generate_action, verbose)

    # generate the cors config
    if config_dict.get("allow_all_cors") is True or config_dict.get("cors_domain") is not None:
        __export_template_to_file("template/cors.pystache", "chalicelib/cors.json", config_dict, generate_action,
                                  verbose)


def __export_template_to_file(template_file, output_file, config_doc, generate_action=None, verbose=False):
    # import the template file
    print("Target: %s" % output_file)

    with open(template_file) as t:
        template = t.read()

    # create a renderer
    renderer = pystache.Renderer()

    rendered = renderer.render(template, config_doc)

    if generate_action != 'dry-run':
        with open(output_file, 'w') as out:
            out.write(rendered)

        out.close()

        print("Generated configuration successfully")

    if verbose is True:
        print(rendered)


def identity_trace(f):
    def resolve_identity(self, *args, **kwargs):
        # set the class' caller identity if we can get it
        try:
            if self._caller_identity is None:
                self._caller_identity = get_caller_identity()
                self._simple_identity = get_caller_simplename(self._caller_identity)
        except Exception as e:
            self._logger.error(e)

        # return the decorated function
        return f(self, *args, **kwargs)

    return resolve_identity


def strtobool(val):
    # wrapping distutils as I actually want a boolean
    return _util.strtobool(val) == 1


def setup_logging():
    logging.basicConfig()
    log = logging.getLogger(params.AWS_DATA_API_NAME)
    log.setLevel(params.DEFAULT_LOG_LEVEL)

    log_level = os.getenv(params.LOG_LEVEL_PARAM)
    if log_level is not None:
        log.info(f"Setting Log Level to {log_level}")
        log.setLevel(log_level.upper())

    return log


# method to generate the ID for a metadata entry
def get_metaid(id):
    return f"{id}-meta"


# method to return the name of the metadata table for a data API element
def get_metaname(name):
    return f"{name}-Metadata"


def remove_internal_attrs(dict):
    dict.pop(params.APP, None)
    dict.pop(params.LAST_UPDATED_BY, None)
    dict.pop(params.LAST_UPDATE_DATE, None)
    dict.pop(params.LAST_UPDATE_ACTION, None)
    dict.pop('api', None)
    dict.pop('type', None)


def _get_sts_client():
    global _sts_client
    if _sts_client is not None:
        return _sts_client
    else:
        _sts_client = boto3.client("sts", region_name=get_region())

        return _sts_client


def get_region():
    return os.getenv('AWS_REGION')


def get_request_router_logger_name(stage: str) -> str:
    return f'RequestRouter-{stage}' if stage.lower() != 'prod' else 'RequestRouter'


def get_caller_account():
    return _get_sts_client().get_caller_identity()["Account"]


def get_caller_simplename(_identity):
    return f"{_identity['Account']}.{_identity['UserId']}"


def get_caller_identity():
    return _get_sts_client().get_caller_identity()


def get_es_index_name(table_name, index_prefix):
    return f'{params.AWS_DATA_API_NAME.lower()}-{table_name.lower()}-{index_prefix.lower()}'


def get_es_type_name(table_name, index_prefix):
    return f'{table_name}-{index_prefix}'


# method to generate an ARN for an Item in a table
def get_arn(id, table_name, deployed_account):
    return f"{get_arn_base()}:{get_region()}:{deployed_account}:{table_name}:{id}"


def versioned_arn(id, table_name, deployed_account, item_version):
    return f"{get_arn(id, table_name, deployed_account)}:{item_version}"


def shred_arn(arn):
    if ":" not in arn:
        return None
    else:
        tokens = arn.split(":")

        def _get_arn(with_dict):
            d = {
                params.ARN_REGION: tokens[3],
                params.ARN_ACCOUNT: tokens[4],
                params.ARN_TABLE: tokens[5],
                params.ARN_ID: tokens[6]
            }

            if with_dict is not None:
                d.update(with_dict)

            return d

        if len(tokens) == 7:
            return _get_arn()
        elif len(tokens) == 8:
            return _get_arn({
                params.ITEM_VERSION: tokens[7]
            })
        else:
            return None


def get_arn_base():
    return f"arn:aws:{params.AWS_DATA_API_SHORTNAME}"


def decorate(content):
    return json.dumps(content, indent=4, cls=DataApiEncoder)


def get_time_now():
    return time.time()


def get_datetime_now():
    return datetime.datetime.now()


def get_date_now(fmt=None):
    if fmt is None:
        return get_datetime_now().strftime(params.DEFAULT_DATE_FORMAT)
    else:
        return get_datetime_now().strftime(fmt)


def get_table_name(table_name, deployment_stage):
    return f"{table_name}-{deployment_stage}"


def _get_client(name: str):
    return boto3.client(name, region_name=get_region())


def _get_glue_client():
    return _get_client('glue')


def create_s3_crawler(crawler_name, target_entity_name, crawler_rolename, catalog_db, s3_path, and_run=False):
    glue_client = _get_glue_client()

    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=crawler_rolename,
            DatabaseName=catalog_db,
            Description=f'Crawler for S3 Data API Export {target_entity_name}',
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_path
                    }
                ],
            },
            # run every hour on the hour
            Schedule='cron(0 * * * ? *)',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        pass

    if and_run is True:
        glue_client.start_crawler(Name=crawler_name)

    return crawler_name


def get_all_data_apis():
    log = setup_logging()

    api_gw = _get_client("apigateway")
    region = get_region()
    response = {}
    custom_domains = {}

    # load api custom domain names
    all_custom_domains = api_gw.get_domain_names()
    if "items" in all_custom_domains:
        for domain in all_custom_domains.get("items"):
            if "tags" in domain and "source" in domain.get("tags") and domain.get("tags").get(
                    "source") == params.AWS_DATA_API_NAME:
                # get base path mappings for the domain
                base_path_mappings = api_gw.get_base_path_mappings(domainName=domain.get("domainName"))
                if base_path_mappings is not None and "items" in base_path_mappings:
                    for path in base_path_mappings.get("items"):
                        custom_domains[path.get('restApiId')] = {
                            "api": path.get('restApiId'),
                            "stage": path.get('stage'),
                            "basePath": path.get('basePath'),
                            "url": domain.get("domainName"),
                            "cf": domain.get("distributionDomainName")
                        }

    # grab all rest API's and match them to Data API's
    all_apis = api_gw.get_rest_apis()
    if "items" in all_apis:
        for api in all_apis.get("items"):
            if params.AWS_DATA_API_SHORTNAME in api.get("name"):
                stage = api.get("name").replace(f"{params.AWS_DATA_API_SHORTNAME}-", "")

                # TODO remove this restriction to support custom stage names
                if stage.lower() in ['dev', 'test', 'prod', 'int']:
                    entry = {
                        "Endpoint": f"https://{api.get('id')}.execute-api.{region}.amazonaws.com",
                        "Stage": stage
                    }

                    # check for a custom domain name
                    domain_info = custom_domains.get(api.get('id'))
                    if domain_info is not None:
                        entry["URL"] = domain_info.get("url")
                        entry["DistributionDomainName"] = domain_info.get("cf")

                        if domain_info.get('basePath') is not None and domain_info.get('basePath') != '(none)':
                            entry["BasePath"] = domain_info.get('basePath')

                    response[stage] = entry

    return response


def verify_crawler(table_name, crawler_rolename, catalog_db):
    glue_client = _get_glue_client()

    try:
        glue_client.get_crawler(Name=table_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_crawler(
            Name=table_name,
            Role=crawler_rolename,
            DatabaseName=catalog_db,
            Description=f'Crawler for AWS Data API Table {table_name}',
            Targets={
                'DynamoDBTargets': [
                    {
                        'Path': table_name
                    },
                ]
            },
            # run every hour on the hour
            Schedule='cron(0 * * * ? *)',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
            }
        )


def run_glue_export(table_name, s3_export_path, kms_key_arn, read_pct, log_path, export_role, dpu):
    glue_client = _get_glue_client()

    security_config = None
    try:
        crypt = {
            'S3EncryptionMode': 'SSE-S3' if kms_key_arn is None else "SSE-KMS"
        }
        if kms_key_arn is not None:
            crypt['KmsKeyArn'] = kms_key_arn

        security_config = f"{params.AWS_DATA_API_SHORTNAME}-ddb-export-config"
        glue_client.create_security_configuration(
            Name=security_config,
            EncryptionConfiguration={
                'S3Encryption': [crypt],
                'CloudWatchEncryption': {
                    'CloudWatchEncryptionMode': 'DISABLED'
                },
                'JobBookmarksEncryption': {
                    'JobBookmarksEncryptionMode': 'DISABLED'
                }
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        pass

    job_name = f"{params.AWS_DATA_API_SHORTNAME}-{table_name}"

    try:
        glue_client.create_job(
            Name=job_name,
            Description=f"{params.AWS_DATA_API_NAME} Data Export to S3",
            LogUri=log_path,
            Role=export_role,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': f"s3://awslabs-code-{get_region()}/{params.AWS_DATA_API_NAME}/glue_export_dynamo_table.py",
                'PythonVersion': '3'
            },
            MaxRetries=3,
            Timeout=1000,
            SecurityConfiguration=security_config
        )
    except glue_client.exceptions.IdempotentParameterMismatchException:
        # thrown when the job already exists
        pass

    def _argname(v):
        return f"--{v}"

    args = {
        _argname(export_utils.EXPORT_ARG_TABLE_NAME): table_name,
        _argname(export_utils.EXPORT_ARG_READ_PCT): str(read_pct),
        _argname(export_utils.EXPORT_ARG_PREFIX): s3_export_path,
        # only support compressed json formatting for now
        _argname(export_utils.EXPORT_ARG_FORMAT): "json"
    }

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=args,
            AllocatedCapacity=dpu
        )

        if response is not None and 'JobRunId' in response:
            return {
                "JobName": job_name,
                "JobRunId": response['JobRunId']
            }
    except glue_client.exceptions.ConcurrentRunsExceededException as ce:
        raise DetailedException(message="Export Job Already Running")


def get_running_export_jobs(job_name):
    glue_client = _get_glue_client()

    response = glue_client.get_job_runs(
        JobName=job_name
    )

    if response is None:
        raise ResourceNotFoundException(f"Unable to resolve Job Name {job_name}")
    else:
        running = []
        if 'JobRuns' in response:
            for j in response['JobRuns']:
                status = j['JobRunState']

                if not any(x in status for x in ['STARTING', 'RUNNING', 'STOPPING']):
                    pass
                else:
                    running.append(_extract_glue_job_status(j))

        return running


def _extract_glue_job_status(job):
    output = {
        "Status": job['JobRunState'],
        "Started": job['StartedOn'].strftime(params.DEFAULT_DATE_FORMAT),
        'ExecutedDuration': job['ExecutionTime']
    }

    if 'CompletedOn' in job:
        output['Completed'] = job['CompletedOn'].strftime(params.DEFAULT_DATE_FORMAT)

    if 'ErrorMessage' in job:
        output['ErrorMessage'] = job['ErrorMessage']

    if 'Arguments' in job:
        output['Arguments'] = job['Arguments']

    return {job['Id']: output}


def get_glue_job_status(job_name, run_id):
    glue_client = _get_glue_client()

    try:
        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )

        if response is not None:
            return _extract_glue_job_status(response['JobRun'])
    except glue_client.exceptions.EntityNotFoundException:
        raise ResourceNotFoundException("Unable to resolve Job Name or Run ID")

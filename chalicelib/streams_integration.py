import json
import os
import boto3
import botocore
import logging
from chalicelib.exceptions import *
import chalicelib.utils as utils
import chalicelib.parameters as params

DELIVERY_STREAM_NAME = 'KinesisFirehoseDeliveryStream'
DELIVERY_STREAM_ARN = 'DeliveryStreamARN'
ES_DOMAIN = 'ElasticSearchDomain'
ES_ENDPOINT = 'ElasticSearchEndpoint'
DOMAIN_ID = 'DomainId'
DELIVERY_BUFFER_INTERVAL_SECONDS = 300
DELIVERY_BUFFER_EVENTS = 1000
REGION = os.getenv('AWS_REGION')


class StreamsIntegration:
    _region = None
    _table_name = None
    _delivery_streams = {}
    _dest_mapping = {}
    _es_domain_name = None
    _es_domain = None
    _es_client = None
    _fh_client = None
    _api_control_table = None
    _logger = None
    _deployment_stage = None

    def get_endpoint(self):
        if self._es_domain is not None:
            return self._es_domain

    def __init__(self, deployment_stage, search_config):
        streams_integration_logger = f'SearchIntegration-{deployment_stage}' if deployment_stage.lower() != 'prod' else 'SearchIntegration'
        self._logger = utils.setup_logging()
        self._deployment_stage = deployment_stage

        if self._es_client is None:
            self._logger.info("Setting up new ElasticSearch and Firehose clients")
            self._es_client = boto3.client('es', region_name=REGION)
            self._fh_client = boto3.client('firehose', region_name=REGION)

            # create a reference to the api control table in ddb so we can pull metadata
            dynamo_helper, self._api_control_table = utils.get_api_control_table(REGION, self._logger)

            # create the source/dest mapping table
            for t in [params.RESOURCE, params.METADATA]:
                config = search_config.get('DeliveryStreams')[t]
                self._dest_mapping[config['SourceStreamARN']] = config['DestinationDeliveryStreamARN']

    def _describe_delivery_stream(self, stream_name):
        try:
            response = self._fh_client.describe_delivery_stream(
                DeliveryStreamName=stream_name
            )
            if response is None or 'DeliveryStreamDescription' not in response:
                raise ResourceNotFoundException()
            else:
                return {
                    DELIVERY_STREAM_NAME: response['DeliveryStreamDescription']['DeliveryStreamName'],
                    DELIVERY_STREAM_ARN: response['DeliveryStreamDescription'][DELIVERY_STREAM_ARN]
                }
        except:
            return None

    def _verify_event_source(self, stream_arn):
        lambda_client = boto3.client('lambda', region_name=REGION)

        # create the event source
        try:
            lambda_client.create_event_source_mapping(
                EventSourceArn=stream_arn,
                FunctionName=f'{params.AWS_DATA_API_NAME}-{self._deployment_stage}-{params.INDEXER_NAME}',
                Enabled=True,
                BatchSize=DELIVERY_BUFFER_EVENTS,
                StartingPosition='LATEST'
            )
        except botocore.exceptions.ClientError as e:
            # resource conflict exception raised when the stream already exists
            if e.response['Error']['Code'] == 'ResourceConflictException':
                pass
            else:
                raise e

    def _verify_es_delivery_stream(self, index_prefix=None, firehose_role_arn=None, failure_bucket=None):
        if firehose_role_arn is not None and failure_bucket is not None:
            # create the delivery stream with routing to ElasticSearch
            name = f'{params.AWS_DATA_API_NAME}-{self._table_name}-ES-{index_prefix}'

            create = {
                'DeliveryStreamName': name,
                'DeliveryStreamType': 'DirectPut',
                'ElasticsearchDestinationConfiguration': {
                    'RoleARN': firehose_role_arn,
                    'DomainARN': self._es_domain['ARN'],
                    'IndexName': utils.get_es_index_name(self._table_name, index_prefix),
                    'TypeName': utils.get_es_type_name(self._table_name, index_prefix),
                    'IndexRotationPeriod': 'NoRotation',
                    'BufferingHints': {
                        'IntervalInSeconds': DELIVERY_BUFFER_INTERVAL_SECONDS
                    },
                    'S3BackupMode': 'FailedDocumentsOnly',
                    'S3Configuration': {
                        'RoleARN': firehose_role_arn,
                        'BucketARN': f'arn:aws:s3:::{failure_bucket}',
                        'Prefix': f'{params.AWS_DATA_API_NAME}/{self._table_name}/{index_prefix}/',
                        'BufferingHints': {
                            'IntervalInSeconds': DELIVERY_BUFFER_INTERVAL_SECONDS
                        },
                        'CompressionFormat': 'GZIP',
                        'EncryptionConfiguration': {
                            'NoEncryptionConfig': 'NoEncryption'
                        }
                    },
                    'ProcessingConfiguration': {
                        'Enabled': False
                    },
                    'CloudWatchLoggingOptions': {
                        'Enabled': True,
                        'LogGroupName': f'{params.AWS_DATA_API_NAME}-SearchIntegration',
                        'LogStreamName': name
                    }
                }
            }

            try:
                response = self._fh_client.create_delivery_stream(**create)
                return response[DELIVERY_STREAM_ARN]
            except botocore.exceptions.ClientError as e:
                # resource conflict exception raised when the stream already exists
                if e.response['Error']['Code'] == 'ResourceInUseException':
                    response = self._fh_client.describe_delivery_stream(DeliveryStreamName=name)

                    if response is not None:
                        return response['DeliveryStreamDescription']['DeliveryStreamARN']
                else:
                    raise e

    def _validate_es_domain(self, es_domain):
        # validate the elastic search domain
        response = self._es_client.describe_elasticsearch_domain(
            DomainName=es_domain
        )

        DS = 'DomainStatus'
        if response is not None and DS in response:
            return {
                DOMAIN_ID: response[DS][DOMAIN_ID],
                ES_ENDPOINT: response[DS]['Endpoint'],
                'ARN': response[DS]['ARN']
            }
        else:
            raise ResourceNotFoundException(f'ElasticSearch Domain {self._es_domain_name} Not Found')

    def configure_search_flow(self, endpoints, es_domain_name, firehose_delivery_role_arn, failure_record_bucket):
        response = self._validate_es_domain(es_domain_name)
        if response is not None:
            self._es_domain = response
        else:
            raise InvalidArgumentsException("Invalid ES Domain or Cannot Verify ES Domain Detail")

        # configure delivery streams and functions for both the resource stream and the metadata
        stream_arn_list = [{'Type': params.RESOURCE, 'ARN': endpoints[params.RESOURCE_STREAM_ARN]},
                           {'Type': params.METADATA, 'ARN': endpoints[params.METADATA_STREAM_ARN]}]

        print(f"Configuring {len(stream_arn_list)} Search Integration Flows")

        for s in stream_arn_list:
            # verify the firehose delivery stream with routing to the ES domain
            delivery_stream = self._verify_es_delivery_stream(s['Type'], firehose_delivery_role_arn,
                                                              failure_record_bucket)

            # verify the event source that wires the table's stream to the ES indexing function
            self._verify_event_source(stream_arn=s['ARN'])

            self._delivery_streams[s['Type']] = {
                'SourceStreamARN': s['ARN'],
                'DestinationDeliveryStreamARN': delivery_stream
            }

            # setup the delivery stream index on the basis of the dynamodb update stream, so we can look the
            # destination stream up quickly when we receive new records
            self._dest_mapping[s['ARN']] = delivery_stream

        # output a structured payload that describes the stream and ES domain
        return {
            'DeliveryStreams': self._delivery_streams,
            ES_DOMAIN: self._es_domain
        }

    def forward_to_es_firehose(self, records):
        # determine the destination stream
        destination_stream = None
        if 'eventSourceARN' not in records[0]:
            raise ResourceNotFoundException(
                "Record does not contain an eventSourceARN and so is unroutable to Firehose")
        else:
            source_arn = records[0]['eventSourceARN']
            destination_stream = self._dest_mapping.get(source_arn)

        if destination_stream is None:
            raise DetailedException(f"Unable to find Destination Route for Source {source_arn}")

        # construct an elastic search batch index request
        items = []

        for r in records:
            if 'dynamodb' in r:
                # construct the item to be emitted
                item = {}
                payload = r['dynamodb']

                # extract the key values and surface them as '_id'
                ids = []
                for k, v in payload['Keys'].items():
                    for value in v.values():
                        ids.append(value)
                item["document_id"] = ".".join(ids)

                # extract the NewImage values, which is the whole new record
                for k, v in payload['NewImage'].items():
                    for value in v.values():
                        item[k] = value

                # now add the item as base64 encoded json
                items.append({'Data': json.dumps(item)})

        # push the item batch to firehose
        if len(items) > 0:
            self._fh_client.put_record_batch(
                DeliveryStreamName=destination_stream,
                Records=items
            )

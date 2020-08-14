import boto3
import os
import chalicelib.utils as utils
import chalicelib.parameters as params

FLUSH_LIMIT_COUNT = 250
FLUSH_LIMIT_SECONDS = 60
PUT_METRIC_BATCH_SIZE = 20


# CloudWatch should show an event such as:
#
# "AWS Data API's", "API Namespace", "API Stage", "Metric Name", "Event Count", "Interval"
# "AWS Data API's", "Customer",      "Dev",       "Update",      "42",          2020-01-10 10:00:00
# "AWS Data API's", "Customer",      "Dev",       "Update",      "99",          2020-01-10 10:05:00
# same api namespace, different Stage
# "AWS Data API's", "Customer",      "Prod",      "Update",      "23493",       2020-01-10 10:00:00
# "AWS Data API's", "Customer",      "Prod",      "Update",      "54958",       2020-01-10 10:05:00
# ...
# different api namespace
# "AWS Data API's", "Product",       "Dev",       "Update",      "45",          2020-01-10 10:00:00
# "AWS Data API's", "Product",       "Dev",       "Update",      "93",          2020-01-10 10:05:00

# decorator function for CloudWatch Events
def evented(api_operation):
    def event_decorator(f):
        def log_api_purpose(self, *args, **kwargs):
            # lazy load the CloudWatch Event Emitter
            if self._cloudwatch_emitter is None:
                self._cloudwatch_emitter = CloudwatchEmitter(api_name=self._api_name,
                                                             api_stage=self._deployment_stage)

            # emit the instrumented event
            self._cloudwatch_emitter.emit(event={
                "ApiOperation": api_operation
            })

            # return the decorated function
            return f(self, *args, **kwargs)

        return log_api_purpose

    return event_decorator


class CloudwatchEmitter():
    _api_name = None
    _api_stage = None
    _event_dimensions = None
    _cwe_client = None
    _events = []
    _event_count = 0
    _last_flush_time = None

    def __init__(self, api_name, api_stage):
        self._api_name = api_name
        self._api_stage = api_stage
        self._event_dimensions = [{"Name": "ApiNamespace", "Value": self._api_name},
                                  {"Name": "ApiStage", "Value": self._api_stage}]
        # create the cloudwatch client and set log function
        if self._cwe_client is None:
            self._cwe_client = boto3.client('cloudwatch', region_name=os.getenv('AWS_REGION'))

    def emit(self, event):
        self._cwe_client.put_metric_data(Namespace=params.AWS_DATA_API_NAME,
                                         MetricData=[{"MetricName": event.get('ApiOperation'),
                                                      "Value": 1,
                                                      "Timestamp": utils.get_datetime_now(),
                                                      "Dimensions": self._event_dimensions}])

from chalicelib.aws_data_api import AwsDataAPI
from chalice import Chalice, BadRequestError
import chalicelib.aws_data_api as dapi
from chalicelib.api_metadata import ApiMetadata
from chalicelib.exceptions import *
import chalicelib.parameters as params
import logging

CONF_CACHE_HANDLER = 'handler'
CONF_CACHE_CORS = 'cors'


class DataApiCache:
    _api_cache = {}
    _app = None
    _stage = None
    _region = None
    _logger = None

    def __init__(self, app: Chalice, stage: str, region: str, logger: logging.Logger):
        self._app = app
        self._stage = stage
        self._region = region
        self._logger = logger

    def add(self, key: str, api: AwsDataAPI):
        v = {
            CONF_CACHE_HANDLER: api
        }

        self._api_cache[key] = v

    def remove(self, api_name: str):
        if api_name in self._api_cache:
            del self._api_cache[api_name]

    def contains(self, api_name: str):
        if self._api_cache is not None and api_name in self._api_cache:
            return True
        else:
            return False

    # function to retrieve an API from cache, or bootstrap one from metadata
    def get(self, api_name):
        if api_name not in self._api_cache:
            # load from metadata
            self._logger.info(f"Loading API Instance {api_name} Stage {self._stage} from Metadata")
            api_metadata_handler = ApiMetadata(self._region, self._logger)

            api_metadata = api_metadata_handler.get_api_metadata(api_name=api_name, stage=self._stage)

            if api_metadata is None:
                raise BadRequestError(f"Unable to resolve API {api_name} in Stage {self._stage}")
            else:
                if api_metadata.get("Status") == params.STATUS_CREATING:
                    raise InvalidArgumentsException("API Not Active")
                else:
                    api_metadata['app'] = self._app
                    api_metadata[params.REGION] = self._region
                    api_metadata['ApiName'] = api_name

                    # instantiate the API from metadata
                    api = dapi.load_api(**api_metadata)

                    # TODO add caching for CORS objects from API Metadata

                    self.add(api_name, api)

                    return api
        else:
            return self._api_cache[api_name][CONF_CACHE_HANDLER]

import json
import decimal
from chalice.app import MultiDict
from formencode import variabledecode as vd


class DataApiEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        elif isinstance(o, MultiDict):
            return vd.variable_decode(o)
        else:
            return super(DataApiEncoder, self).default(o)

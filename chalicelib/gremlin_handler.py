import chalicelib.utils as utils
import json
import sys

from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T
from gremlin_python.driver.client import Client
from gremlin_python.structure.graph import Vertex, Edge
from chalicelib.exceptions import ConstraintViolationException, ResourceNotFoundException
from chalicelib.neptune import Neptune

ID = 'id'
DEFAULT_SEARCH_DEPTH = 10
DATE = 'date'
CONSTRAINT_VIOLATION_EXCEPTION = 'ConstraintViolationException'


def graph_method(f):
    def setup_graph_method(self, *args, **kwargs):
        # lazy load the graph traverser
        if self._url is not None and self.g is None:
            n = Neptune()
            try:
                self.g = n.graphTraversal(neptune_endpoint=self._url, neptune_port=self._port,
                                          retry_limit=self.connection_retries if self.connection_retries is not None else n.DEFAULT_RETRY_COUNT)
            except Exception as e:
                raise ResourceNotFoundException(f"Unable to connect to Gremlin Endpoint {self._url}:{self._port}")

            print(f"Connected Gremlin Endpoint {self._url}:{self._port}")

        # return the decorated function
        return f(self, *args, **kwargs)

    return setup_graph_method


# method to transform a dict of vertices and edges into a structured json output
def _format_graph_results(from_results):
    out = []

    if from_results is not None:
        for r in from_results:
            if isinstance(r, Vertex) or isinstance(r, Edge):
                out.append(str(r))
            elif isinstance(r, list):
                out = out + _format_graph_results(list(r))
            else:
                if 'e' in r and 'v' in r:
                    edge = r['e'][0]
                    vertex = r['v'][0]

                    o = {"TypeStage": vertex[T.label], ID: vertex[ID][0], "Label": edge[T.label]}
                    extra_props = {}
                    for k, v in edge.items():
                        if str(k) != 'T.label' and str(k) != 'T.id':
                            extra_props[k] = v

                    if len(extra_props) > 0:
                        o['AdditionalProperties'] = extra_props
                    out.append(o)
                else:
                    out.append(str(r))

    return out


class GremlinHandler:
    g = None
    _url = None
    _port = None
    _connection_retries = None
    _gremlin_client = None

    def __init__(self, url, port, connection_retries=None):
        self._url = url
        self._port = port
        self._connection_retries = connection_retries

    # wrapper for next() terminal step and error handler
    def _do_next(self, traverser):
        try:
            return traverser.next()
        except GremlinServerError as e:
            exc_info = sys.exc_info()
            err = json.loads(str(exc_info[1]).split(':', 1)[1])
            if err['code'] == CONSTRAINT_VIOLATION_EXCEPTION:
                raise ConstraintViolationException(err['detailedMessage'])
            else:
                raise e

    # method to perform a fixed depth search based upon the supplied starting vertex and traverser (for inbound or
    # outbound relationships)
    def _depth_search(self, start_vertex, traverser, search_depth=DEFAULT_SEARCH_DEPTH):
        depth = int(search_depth) if search_depth is not None else DEFAULT_SEARCH_DEPTH

        return self.g.V(start_vertex).repeat(traverser.as_('e').otherV()).times(
            depth).emit().project('e', 'v').by(
            __.select('e').valueMap(True).fold()).by(__.valueMap(True).fold()).toList()

    # create a reference from one object to another and add the date
    @graph_method
    def create_edge(self, label, from_id, to_id, extra_properties):
        try:
            # Edge ID is the from id and to id plus the label, so we can have multiple different types of relations
            edge_id = '{}-{}-{}'.format(from_id, label, to_id)

            # create the edge or return it if it already exists
            self._do_next(self.g.E(edge_id).fold().coalesce(
                __.unfold(), __.V(from_id).addE(label).to(__.V(to_id)).property(T.id, edge_id)).property(DATE,
                                                                                                         utils.get_date_now()))
            # get the edge object
            edge = self.g.E(edge_id).limit(1)

            # add the reference data that was supplied as edge properties
            for k, v in extra_properties.items():
                edge = edge.property(k, v)

            if len(extra_properties) != 0:
                edge.next()
        except ConstraintViolationException:
            pass

    @graph_method
    def create_relationship(self, label, from_id, to_id, extra_properties):
        # create from, then create to, then connect them together
        self.g.get_or_create_vertex(label_value=self._table_name, id=from_id)
        self.g.get_or_create_vertex(label_value=self._table_name, id=to_id)
        self.g.create_edge(label=label, from_id=from_id, to_id=to_id, extra_properties=extra_properties)

    # get an object that resides in the graph, by ID
    def _validated_vertex_id(self, id):
        node = self.g.V(id).toList()

        # node may be None or empty array depending on underlying library, so check and return None either way
        return id if node is not None and len(node) > 0 else None

    # delete an object from the graph
    @graph_method
    def delete_vertex(self, id):
        this_object = self._validated_vertex_id(id)

        if this_object is None:
            raise ResourceNotFoundException()
        else:
            self._do_next(self.g.V(this_object).drop())

    # get or create a new object in the graph
    @graph_method
    def get_or_create_vertex(self, label_value, id):
        return self._do_next(
            self.g.V(id).fold().coalesce(__.unfold(),
                                         __.addV(label_value).property(T.id, id).property(ID,
                                                                                          id).property(DATE,
                                                                                                       utils.get_date_now())))

    # return count of vertices
    @graph_method
    def get_usage(self):
        return self.g.V().count().next()

    # return all vertices referenced by this object
    @graph_method
    def get_outbound(self, id, search_depth=DEFAULT_SEARCH_DEPTH):
        this_object = self._validated_vertex_id(id)

        if this_object is None:
            raise ResourceNotFoundException(f"Unable to resolve Object {id}")
        else:
            return _format_graph_results(self._depth_search(this_object, __.outE(), search_depth))

    # return all vertices referencing this object
    @graph_method
    def get_inbound(self, id, search_depth=DEFAULT_SEARCH_DEPTH):
        this_object = self._validated_vertex_id(id)

        if this_object is None:
            raise ResourceNotFoundException(f"Unable to resolve Object {id}")
        else:
            return _format_graph_results(self._depth_search(this_object, __.inE(), search_depth))

    @graph_method
    def _do_query(self, query):
        if self._gremlin_client is None:
            print("Creating new General Purpose Gremlin Client on %s:%s" % (self._url, self._port))
            client = Client(f'wss://{self._url}:{self._port}/gremlin', 'g', pool_size=1)

        return _format_graph_results(client.submit(query))

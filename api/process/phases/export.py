"""Export Module"""

import abc

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import FOAF, RDF


class Export(abc.ABC):
    """Export Interface"""

    @abc.abstractmethod
    def export(self):
        """export function"""
        pass


class GraphWrapper:
    """Graph Wrapper"""

    def __init__(self) -> None:
        self.graph: Graph = Graph()
        self.graph.bind("foaf", FOAF)

    def get_graph(self) -> Graph:
        """get current graph instance"""
        return self.graph


class GraphBuilder:
    """Graph Builder"""

    def __init__(self, graph: Graph) -> None:
        pass

    def build_graph(self):
        """Build Graph from annotations"""
        pass

from graph import GraphWrapper
from rdflib import Graph

"""Export Module"""
import abc


class Export(abc.ABC):
    """Export Interface"""

    @abc.abstractmethod
    def export(self, graph: Graph):
        """export function"""
        pass


class TurtleExport(Export):
    """Turtle Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.ttl", format="turtle")


class XMLExport(Export):
    """XML Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.xml", format="xml")


class JSONLDExport(Export):
    """JSON-LD Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.json", format="json-ld")


class NTRIPLESExport(Export):
    """NTRIPLES Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.nt", format="ntriples")


class NOTATION3Export(Export):
    """NTRIPLES Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.n3", format="n3")


class TRIGExport(Export):
    """TRIG Export"""

    def export(self, graph: Graph):
        graph.serialize(destination="kg.trig", format="trig")


graph_wrapper = GraphWrapper(
    annotated_tables_path="./../../../datasets/hardtabler1/gt/cea_gt.csv")
graph_wrapper.build_kg()
export: Export = NOTATION3Export().export(graph_wrapper.get_graph())

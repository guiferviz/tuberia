from itertools import product
from typing import Optional

import inflection
import networkx as nx

from tuberia import utils
from tuberia.task import FunctionTask, Task, dag, topological_sort_grouped
from tuberia.visualization import open_in_browser


class Flow(Task):
    def __init__(self, name: Optional[str] = None):
        if name is None:
            return inflection.underscore(self.__class__.__name__)
        self.name = name
        self.class_full_path = (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )

    @staticmethod
    def from_qualified_name(name: str) -> "Flow":
        module_name, class_name = name.rsplit(".", 1)
        return utils.get_module_member(module_name, class_name)

    def define(self):
        pass

    def pre_run(self):
        pass

    def dag(self):
        G = dag(self._task_descriptor.get_dependencies(self))
        pre_run = FunctionTask(self.pre_run)
        post_run = FunctionTask(self.post_run)
        subgraph = nx.subgraph(G, G.nodes)
        new_G = nx.DiGraph()
        new_G.add_edge(pre_run, subgraph)
        new_G.add_edge(subgraph, post_run)
        return flatten_subgraph(new_G, subgraph)

    def run(self):
        G = self.dag()
        for i in topological_sort_grouped(G):
            for j in i:
                j.run()

    def post_run(self):
        pass

    def plot(self):
        G = self.dag()
        open_in_browser(G)


def flatten_subgraph(G: nx.DiGraph, subgraph: nx.DiGraph):
    assert subgraph in G
    # Predecessors.
    first_tasks = [j for j in subgraph.nodes if subgraph.in_degree(j) == 0]
    predecessors = list(G.predecessors(subgraph))
    for j, k in product(predecessors, first_tasks):
        G.add_edge(j, k)
    # Successors.
    successors = list(G.successors(subgraph))
    last_tasks = [j for j in subgraph.nodes if subgraph.out_degree(j) == 0]
    for j, k in product(last_tasks, successors):
        G.add_edge(j, k)
    # Add all edges and nodes.
    G.add_nodes_from(subgraph.nodes)
    G.add_edges_from(subgraph.edges)
    # Remove subgraph.
    G.remove_node(subgraph)
    return G

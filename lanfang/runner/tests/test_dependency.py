import lanfang
import unittest


class TestTaskTopologicalGraph(unittest.TestCase):
  def test_add_dependency(self):
    graph_1 = lanfang.runner.TopologicalGraph.from_data([
      ("run_task1", "task1,task2"),
      ("run_task2", "run_task1"),
    ])
    self.assertFalse(graph_1.is_valid())

    graph_2 = lanfang.runner.TopologicalGraph.from_data([
      ("task1", None),
      ("task2", None),
      ("run_task1", "task1,task2"),
      ("run_task2", "run_task1"),
    ])
    self.assertTrue(graph_2.is_valid())

  def test_subset(self):
    graph = lanfang.runner.TopologicalGraph.from_data([
      ("task1", None),
      ("task2", None),
      ("run_task1", "task1,task2"),
      ("run_task2", "run_task1"),
    ])
    with self.assertRaises(ValueError):
      nodes = graph.subset("1,2,4-8").get_nodes()

    nodes = graph.subset("1,2").get_nodes()
    self.assertEqual(len(nodes), 2)

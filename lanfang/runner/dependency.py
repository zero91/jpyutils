from lanfang.utils import deprecated
import copy
import collections
import re


class TopologicalGraph(object):
  """Manage a bunch of nodes with topological relations.
  """

  def __init__(self):
    self._m_node_info = collections.defaultdict(dict)
    self._m_node_initial_id = 0
    self._m_is_valid = None

  @classmethod
  def from_data(cls, nodes):
    """Create an instance from a bunch of nodes.

    Parameters
    ----------
    nodes: list
      A sequence of nodes, each item is (node_name, depends).

    Returns
    -------
    instance: TopologicalGraph
      Instance constructed from input argument.

    """
    instance = cls()
    for node_name, depends in nodes:
      instance.add(node_name, depends)
    return instance

  def add(self, node_name, depends=None):
    """Add a node with its dependent nodes.

    Parameters
    ----------
    node_name: str
      Name of the node.

    depends: string, list, tuple, set, dict
      Dependent nodes list.

    Returns
    -------
    instance: TopologicalGraph
      self instance.

    Raises
    ------
    TypeError: Type of 'depends' is not supported.
    """

    if depends is None or len(depends) == 0:
      depends = set()

    elif isinstance(depends, str):
      depends = set(map(str.strip, depends.split(',')))

    elif isinstance(depends, (list, tuple, set, dict)):
      depends = set(depends)

    else:
      raise TypeError("Parameter 'depends' must be a string or list")

    for name in list(depends) + [node_name]:
      if name not in self._m_node_info:
        self._m_node_info[name] = {
          "initial_id": None,
          "order_id": None,
          "depends": set(),
          "reverse_depends": set(),
        }

    self._m_is_valid = None
    if self._m_node_info[node_name]["initial_id"] is None:
      self._m_node_info[node_name]["initial_id"] = self._m_node_initial_id
      self._m_node_initial_id += 1
    self._m_node_info[node_name]["depends"] |= depends

    for name in depends:
      self._m_node_info[name]["reverse_depends"].add(node_name)

  def depends(self, node_name):
    """Get dependent nodes.

    Parameters
    ----------
    node_name: string
      Name of a node.

    Returns
    -------
    depends: set
      Dependent nodes of 'node_name'.

    Raises
    ------
    KeyError: Can't find the node.

    """
    if node_name not in self._m_node_info:
      raise KeyError("Cannot find node '%s'" % (node_name))
    return copy.deepcopy(self._m_node_info[node_name]["depends"])

  def get_nodes(self, order=True):
    """Get graph nodes in order

    Parameters
    ----------
    order: bool
      Return topological order is set True, otherwise False.

    Returns
    ------
    nodes: list
      Sorted nodes.

    """
    if order is True:
      self.is_valid(raises=True)
      key_func = lambda name: self._m_node_info[name]["order_id"]
    else:
      key_func = lambda name: self._m_node_info[name]["initial_id"]

    return sorted(self._m_node_info, key=key_func)

  def reverse_depends(self, node, recursive=False):
    """Get offspring nodes.

    Parameters
    ----------
    node: str
      The name of the node.

    recursive: bool
      Return all offspring if set True, otherwise return child nodes.

    """
    reverse_nodes = self._m_node_info[node]["reverse_depends"]
    if recursive is True:
      offspring = set()
      for rn in reverse_nodes:
        offspring |= self.reverse_depends(rn, recursive=True)
      reverse_nodes |= offspring
    return reverse_nodes

  @deprecated("Unreasonable method, will be removed in future")
  def get_task_info(self):
    """Get information of all nodes.

    Returns
    -------
    node_info: collections.defaultdict
      {
        "node_name": {
          "initial_id": 0,
          "order_id": 1,
          "depends": set(),
          "reverse_depends": set(),
        },
      }

    """
    self.is_valid(raises=True)
    return copy.deepcopy(self._m_node_info)

  def is_valid(self, raises=False):
    """Evaluate the validation of this graph.

    Returns
    -------
    valid: boolean
      True if the nodes dependency realations is topological, otherwise False.

    """
    if self._m_is_valid is None:
      if self._m_node_initial_id != len(self._m_node_info):
        self._m_is_valid = False

      else:
        node_info = copy.deepcopy(self._m_node_info)
        cur_node_id = 0
        while len(node_info) > 0:
          ready_list = list()
          for name in node_info:
            if len(node_info[name]["depends"]) == 0:
              ready_list.append(name)
          if len(ready_list) == 0:
            self._m_is_valid = False
            break

          for name in sorted(ready_list,
                             key=lambda n: node_info[n]["initial_id"]):
            self._m_node_info[name]["order_id"] = cur_node_id
            cur_node_id += 1
            for depend_name in node_info[name]["reverse_depends"]:
              node_info[depend_name]["depends"].remove(name)
            node_info.pop(name)

        if self._m_is_valid is None:
          self._m_is_valid = True

    if raises and not self._m_is_valid:
      raise ValueError("Current graph is not topological")
    return self._m_is_valid

  def __find_match_nodes(self, pattern_str):
    nodes = []
    pattern = re.compile(pattern_str)
    for nd in self._m_node_info:
      if pattern.match(nd):
        nodes.append(self._m_node_info[nd]["order_id"])
    return nodes

  def __find_range_nodes(self, range_str):
    part = range_str.split('-')
    if not 2 <= len(part) <= 3:
      logging.warning("range syntax only accept length 2 or 3, "\
          "but received length %d" % len(part))
      return []

    for v in part:
      if not v.isdigit() and v != '':
        logging.warning("range syntax only accept digits or empty value, "\
            "but received value '%s'" % v)
        return []

    if part[0].isdigit():
      start = int(part[0])
    else:
      start = 0

    if part[1].isdigit():
      stop = int(part[1])
    else:
      stop = len(self._m_node_info) - 1

    if len(part) == 3 and part[2].isdigit():
      step = int(part[2])
    else:
      step = 1

    if not 0 <= start <= stop < len(self._m_node_info):
      raise ValueError("node '%s' is not at the range of graph size %d" % (
        range_str, len(self._m_node_info)))
    return list(range(start, stop + 1, step))

  def __parse_node(self, node):
    # Find if node match any of the nodes
    nodes = self.__find_match_nodes(node)
    if len(nodes) > 0:
      return nodes

    if node.isdigit():
      node_id = int(node)
      if not 0 <= node_id < len(self._m_node_info):
        raise ValueError("node %d is not at the range of graph size %d" % (
          node_id, len(self._m_node_info)))
      return [node_id]

    # suport for range syntax
    return self.__find_range_nodes(node)

  def _parse_nodes_from_str(self, string):
    nodes = set()
    for seg in string.split(','):
      seg = seg.strip()
      if seg == "":
        continue
      nodes |= set(self.__parse_node(seg))
    return nodes

  def subset(self, nodes):
    """Return a topological graph with a subset nodes.

    Parameters
    ----------
    nodes: string or list
      Nodes can be a pattern or in specific range syntax.
      Suppose we have a batch of nodes as [(0, 'a'), (1, 'b'), ..., (25, 'z')].

      'nodes' support the following formats:
      (1) "-3,5,7-10-2,13-16,19-".
        "-3" means range from 0(start) to 3, which is "0,1,2,3".
        "7-10-2" means range from 7 to 10, step length 2, which is "7,9".
        "13-15" means range from 13 to 15, step length 1, which is "13,14,15".
        "19-" mean range from 19 to 25(end), which is "19,20,21,22,23,24,25".

      (2) "1-4,x,y,z"
        "1-4" mean range from 1 to 4, which is "1,2,3,4"
        "x" mean node 'x', node id is 23.
        "y" mean node 'y', node id is 24.
        "z" mean node 'z', node id is 25.
        So, above string mean jobs "1,2,3,4,23,24,25".

    Returns
    -------
    instance: TopologicalGraph
      Instance with a subset topological graph.
    """

    self.is_valid(raises=True)

    if nodes is None:
      return copy.deepcopy(self)

    if not isinstance(nodes, (list, tuple)):
      nodes = [nodes]

    node_ids = []
    for node in nodes:
      if isinstance(node, str):
        node_ids.extend(self._parse_nodes_from_str(node))
      elif isinstance(node, int):
        node_ids.append(node)
      else:
        raise ValueError("Unsupported node value type %s" % (node))

    valid_nodes = []
    for node, node_info in self._m_node_info.items():
      if node_info["order_id"] in node_ids:
        valid_nodes.append(node)
    valid_nodes.sort(key=lambda t: self._m_node_info[t]["initial_id"])

    valid_nodes_set = set(valid_nodes)
    valid_nodes_depends = list()
    for node in valid_nodes:
      depends = valid_nodes_set & self._m_node_info[node]["depends"]
      valid_nodes_depends.append(depends)
    return self.__class__.from_data(zip(valid_nodes, valid_nodes_depends))


class DynamicTopologicalGraph(TopologicalGraph):
  def __init__(self):
    super(self.__class__, self).__init__()

    self._m_is_latest = False # keep track of current status
    self._m_remove_nodes = set()
    self._m_ready_nodes = []

  def remove(self, node):
    self.is_valid(raises=True)
    if node not in self._m_node_info:
      raise ValueError("node '%s' does not exist" % node)

    if node in self._m_remove_nodes:
      raise ValueError("node '%s' was deleted already" % node)

    if len(self._m_node_info[node]["depends"]) > 0:
      raise ValueError("node '%s' depends to %d nodes, including %s" % (
          node, len(self._m_node_info[node]["depends"]),
          ",".join(self._m_node_info[node]["depends"])))

    self._m_remove_nodes.add(node)
    if node in self._m_ready_nodes:
      self._m_ready_nodes.remove(node)

    for depend_node in self._m_node_info[node]["reverse_depends"]:
      self._m_node_info[depend_node]["depends"].remove(node)
    self._m_is_latest = False

  def top(self, max_nodes_num=-1):
    """Fetch max_nodes_num of ready nodes.

    Parameters
    ----------
    max_nodes_num: int
      Maximun number of nodes to return.
    """

    self._update_queue()
    if max_nodes_num < 0:
      return self._m_ready_nodes[:]
    return self._m_ready_nodes[:max_nodes_num]

  def _update_queue(self):
    if self._m_is_valid is not None and self._m_is_latest:
      return
    self.is_valid(raises=True)

    ready_nodes_set = set(self._m_ready_nodes)
    new_ready_nodes = []
    for node in self._m_node_info:
      if len(self._m_node_info[node]["depends"]) == 0 \
          and node not in self._m_remove_nodes \
          and node not in ready_nodes_set:
        new_ready_nodes.append(node)
    new_ready_nodes.sort(key=lambda n: self._m_node_info[n]["order_id"])
    self._m_ready_nodes.extend(new_ready_nodes)
    self._m_is_latest = True


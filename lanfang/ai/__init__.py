from lanfang.ai.engine import names
from lanfang.ai.engine.base.dataset import Dataset
from lanfang.ai.engine.base.model import Model
from lanfang.ai import dataset
from lanfang.ai import model
#from lanfang.ai.base.resource import Resource
from lanfang.ai.engine.oracle import KerasOracle


def _subclasses(root_class):
  all_subclasses = set()
  for sub_class in root_class.__subclasses__():
    all_subclasses.add(sub_class)
    all_subclasses |= _subclasses(sub_class)
  return all_subclasses


def _register():
  for dataset_class in _subclasses(Dataset):
    Dataset.register(dataset_class.name(), dataset_class)

  for model_class in _subclasses(Model):
    Model.register(model_class.name(), model_class)

  #for resource_class in _subclasses(Resource):
  #  Resource.register(resource_class.name(), resource_class)


_register()
del _subclasses
del _register

__all__ = [_s for _s in dir() if not _s.startswith('_')]

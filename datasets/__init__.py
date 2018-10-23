"""
Public datasets manager.
========================
"""

from .dataset import Dataset
from .snli import SNLIDataset
from .ner import NERDataset

__all__ = ["Dataset", "SNLIDataset"]

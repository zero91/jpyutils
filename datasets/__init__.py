"""
Public datasets manager.
========================
"""

from .dataset import Dataset
from .snli import SNLIDataset
from .ner import NERDataset
from .embeddings import Embeddings

__all__ = ["Dataset", "SNLIDataset", "NERDataset", "Embeddings"]

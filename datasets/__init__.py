"""
Public datasets manager.
========================
"""

from .dataset import Dataset
from .snli import SNLIDataset

__all__ = ["Dataset", "SNLIDataset"]

from . import tags

__all__ = ["tags"]

try:
    from . import text
    __all__.append("text")
except ImportError as e:
    import logging
    logging.warning("Load module 'text' failed, ignored it")

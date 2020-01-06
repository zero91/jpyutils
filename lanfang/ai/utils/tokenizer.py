from lanfang.utils import func
import abc
import collections
import itertools
import operator


class Tokenizer(abc.ABC):
  __tokenizers__ = {}

  @staticmethod
  def register(name, tokenizer):
    if name in Tokenizer.__tokenizers__:
      raise KeyError("Tokenizer '%s' is already exists." % (name))
    Tokenizer.__tokenizers__[name] = tokenizer

  @staticmethod
  def create(name, **kwargs):
    if name not in Tokenizer.__tokenizers__:
      raise KeyError("Can't find tokenizer '%s'." % (name))
    return Tokenizer.__tokenizers__[name](**kwargs)

  @staticmethod
  @abc.abstractmethod
  def name():
    """Name of the tokenizer."""
    pass

  @abc.abstractmethod
  def tokenize(self, s, **kwargs):
    """Tokenize a str."""
    pass

  def build_dict(self, *, text=None,
                          files=None,
                          save_file=None,
                          lowercase=False,
                          extra_tokens=None,
                          min_freq=1,
                          **tokenize_kwargs):
    """Build a dictionary.
    """
    #TODO: token normalization
    if text is None and files is None:
      raise ValueError("One of parameter 'text' and 'files' must be set.")

    if text is not None and files is not None:
      raise ValueError("Only one of parameter 'text' and 'files' can be set.")

    if files is not None:
      text = []
      if isinstance(files, str):
        files = [files]
      for fname in files:
        with open(fname, 'r') as fin:
          text.append(fin.read())
      
    if isinstance(text, str):
      text = [text]

    if lowercase is True:
      tokens = [self.tokenize(t.lower(), **tokenize_kwargs) for t in text]
    else:
      tokens = [self.tokenize(t, **tokenize_kwargs) for t in text]

    token_freq = collections.Counter(itertools.chain(*tokens))
    if save_file is not None:
      with open(save_file, 'w') as fout:
        for token, freq in sorted(token_freq.items(),
                                  key=operator.itemgetter(1),
                                  reverse=True):
          if freq < min_freq:
            break
          fout.write("{}\t{}\n".format(token, freq))
    return token_freq


class IdentityTokenizer(Tokenizer):
  """Treat each element as a single token."""

  @staticmethod
  def name():
    return "identity_tokenizer"

  def tokenize(self, s, sep=None):
    return s


class SimpleTokenizer(Tokenizer):
  """Using str.split to get each token."""

  @staticmethod
  def name():
    return "simple_tokenizer"

  def tokenize(self, s, sep=None):
    return s.strip().split(sep=sep)


# Register all tokenziers
for tokenizer_class in func.subclasses(Tokenizer):
  Tokenizer.register(tokenizer_class.name(), tokenizer_class)

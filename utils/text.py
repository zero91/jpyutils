from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np

def text2array(sentences, word2id, maxlen=None, beg=0, end=1, unknown=2, padding=3):
    """Convert a text into a two-dimensional array.

    Parameters
    ----------
    sentences: list, [[],[]]
        Two dimension list of sentences.

    word2id: dict
        Mapping dictionary for word to id.

    maxlen: integer
        Maximum length of each sentences.

    beg: integer
        Sentry for begining of a sentence.
        Set to None if you don't want to use it.

    end: integer
        Sentry for ending of a sentence.
        Set to None if you don't want to use it.

    unknown: integer
        Words in the sentences that does not exist in the word dictionary,
        should be set to 'unknown' integer.
        Set to None if you want to skip the unknown words.

    padding: integer
        Sentences which length is less than 'maxlen',
        should be padded with 'padding's to fullfill the length requirement.

    Returns
    -------
    array: np.array
        The converted text array.

    sizes: np.array
        The valid size of each sentences.

    """
    sizes = np.array([len(sent) for sent in sentences])
    for sentry in [beg, end]:
        if sentry is not None:
            sizes += 1

    if maxlen is None:
        maxlen = sizes.max()

    shape = (len(sentences), maxlen)
    array = np.full(shape, padding, dtype=np.int32)
    for i, sent in enumerate(sentences):
        sent_ids = list()
        if beg is not None:
            sent_ids.append(beg)

        for token in sent:
            if token not in word2id and unknown is None:
                continue
            sent_ids.append(word2id.get(token, unknown))
            if len(sent_ids) == maxlen:
                break

        if end is not None:
            sent_ids = sent_ids[:maxlen - 1]
            sent_ids.append(end)

        array[i, :len(sent_ids)] = sent_ids
        sizes[i] = len(sent_ids)
    return array, sizes


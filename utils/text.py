from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np
import tensorflow as tf

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


def clip_sentence(sentences, sizes):
    """Clip the input sentences placeholders to the length of the longest one in the batch.

    Parameters
    ----------
    sentences: tf.Tensor
        Tensor with shape (batch, time_steps).

    sizes: tf.Tensor
        Tensor with shape (batch)

    Returns
    -------
    clipped_sents: tensor with shape (batch, time_steps)
    """
    max_batch_size = tf.reduce_max(sizes)
    clipped_sents = tf.slice(sentences, [0, 0], [-1, max_batch_size])
    return clipped_sents


def mask3d(values, sentence_sizes, mask_value, axis=2):
    """Given a batch of matrices, each with shape m x n, mask the values in each
    row after the positions indicated in sentence_sizes.

    Parameters
    ----------
    values: tf.Tensor
        Tensor with shape (batch_size, m, n).

    sentence_sizes: tf.Tensor
        Tensor with shape (batch_size) containing the sentence sizes that should be limited.

    mask_value: number
        Scalar value to assign to items after sentence size.

    axis: integer
        Over which axis to mask values.

    Returns
    -------
    masked_values: tf.Tensor
        A tensor with the same shape as `values`.

    """
    if axis != 1 and axis != 2:
        raise ValueError("'axis' must be equal to 1 or 2")

    if axis == 1:
        values = tf.transpose(values, [0, 2, 1])

    time_steps1 = tf.shape(values)[1]
    time_steps2 = tf.shape(values)[2]

    ones = tf.ones_like(values, dtype=tf.float32)
    pad_values = mask_value * ones
    mask = tf.sequence_mask(sentence_sizes, time_steps2)

    # mask is (batch_size, sentence2_size). we have to tile it for 3d
    mask3d = tf.expand_dims(mask, 1)
    mask3d = tf.tile(mask3d, (1, time_steps1, 1))

    masked = tf.where(mask3d, values, pad_values)
    if axis == 1:
        masked = tf.transpose(masked, [0, 2, 1])
    return masked

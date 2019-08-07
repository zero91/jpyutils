from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import tensorflow as tf
from ...mltools import utils

class NERDataManager(object):
    """Manage data for Named Entity Recognition task.

    Parameters
    ----------
    sentences: list[list]
        2-Dimension array, each element is a tuple (char, tag).

    batch_size: integer
        Batch size of iteration.

    char2id: dict
        char to integer ID mapping.
        Must contains key '<UNK>' and '<PAD' for unknown charactors and paddings.

    tag2id: dict
        tag to integer ID mapping.

    """
    def __init__(self, sentences, batch_size, char2id, tag2id):
        self._m_id2sentence = dict(enumerate(sentences))
        self._m_batch_size = batch_size
        self._m_char2id = char2id
        self._m_tag2id = tag2id
        self._m_id2tag = {v: k for k, v in tag2id.items()}
        self._m_batch_num = (len(sentences) + batch_size - 1) // batch_size
        self._m_batch_data = self._create_batch()

    @property
    def id2sentence(self):
        """Return original order ID for each sentences.

        Returns
        -------
        id2sentence: dict
            sentence_order_id => sentence

        """
        return self._m_id2sentence

    @property
    def id2tag(self):
        return self._m_id2tag

    @property
    def tag2id(self):
        return self._m_tag2id

    def _create_batch(self):
        sorted_sentences = sorted(self._m_id2sentence.items(), key=lambda kv: len(kv[1]))
        batch_data_list = list()
        for i in range(self._m_batch_num):
            batch_sentences_id, batch_sentences = \
                    zip(*sorted_sentences[i * self._m_batch_size : (i + 1) * self._m_batch_size])
            sentences, sentence_tags = zip(*map(lambda sent: zip(*sent), batch_sentences))

            seg_tag2id = utils.text.word_seg_tags("")[2]
            sentence_seg_ids = list(map(
                lambda chars: utils.text.word_seg_tags("".join(chars))[0],
                sentences
            ))
            sentence_seg_ids, sentence_lengths = utils.text.text2array(
                sentence_seg_ids,
                None,
                padding=seg_tag2id['S']
            )

            sentence_char_ids, _ = utils.text.text2array(
                sentences,
                self._m_char2id,
                unknown=self._m_char2id["<UNK>"],
                padding=self._m_char2id["<PAD>"]
            )

            sentence_tags, _ = utils.text.text2array(
                sentence_tags,
                self._m_tag2id,
                padding=self._m_tag2id['O']
            )
            batch_data_list.append([batch_sentences_id, sentence_lengths, sentence_char_ids,
                                    sentence_seg_ids, sentence_tags])
        return batch_data_list

    def iter_batch(self, shuffle=True):
        """Return an iterator on all batches.

        Parameters
        ----------
        shuffle: boolean
            Shuffle all batches.

        Returns
        -------
        it: iterator
            An iterator on all batches. Each element is a list, which contains 5 elements.
                1) sentences_id: Original ID for each sentences.
                2) sentence_lengths: Length for each sentences.
                3) sentence_char_ids: Charactor IDs for each sentences.
                4) sentence_seg_ids: Segmentation position IDs for each sentences.
                5) sentence_tags: True tags for each sentences.

        """
        if shuffle:
            random.shuffle(self._m_batch_data)
        return iter(self._m_batch_data)


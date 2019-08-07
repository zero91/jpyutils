from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from .. import utils

import collections

def precision_recall_f1_score(y_true, y_pred):
    """Compute the precision/recall/f1 score.

    Parameters
    ----------
    y_true: list[list]
        2d array. Ground truth (correct) target values.

    y_pred : list[list]
        2d array. Estimated targets as returned by a tagger.

    Returns
    -------
    score: dictionary
        precision/recall/f1 score, the format of the result is as follows:
        {
            "overall": (precision_1, recall_1, f1_1),
            "PER": (precision_2, recall_2, f1_2),
            "MISC": (precision_3, recall_3, f1_3),
        }
        The value of the key 'overall' is the overall metrics of the input value.

    """
    tag_true_entities = collections.defaultdict(set)
    tag_pred_entities = collections.defaultdict(set)
    for i, (seq_true, seq_pred) in enumerate(zip(y_true, y_pred)):
        tag_true_entities["overall"] |= set([(i, e) for e in utils.tags.get_entities(seq_true)])
        tag_pred_entities["overall"] |= set([(i, e) for e in utils.tags.get_entities(seq_pred)])

    for i, (tag, begin, end) in tag_true_entities["overall"]:
        tag_true_entities[tag].add((i, begin, end))

    for i, (tag, begin, end) in tag_pred_entities["overall"]:
        tag_pred_entities[tag].add((i, begin, end))

    result = collections.defaultdict(dict)
    for tag in set(tag_pred_entities) | set(tag_true_entities):
        true_entities = tag_true_entities[tag]
        pred_entities = tag_pred_entities[tag]
        correct_entities = len(true_entities & pred_entities)

        precision = correct_entities / len(pred_entities) if len(pred_entities) > 0 else 0.
        recall = correct_entities / len(true_entities) if len(true_entities) > 0 else 0.
        f1 = 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0.

        result[tag] = (precision, recall, f1)
    return result


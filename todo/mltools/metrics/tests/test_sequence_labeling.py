from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from jpyutils.mltools.metrics import sequence_labeling

import os
import unittest
import subprocess

class TestSequenceLabeling(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._m_tags_fname = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                        "data", "tags.txt")
        cls._m_y_true, cls._m_y_pred = cls._load_labels(cls, cls._m_tags_fname)

    def test_precision_recall_f1(self):
        with open(self._m_tags_fname) as fin:
            output = subprocess.check_output(['perl', 'conlleval.pl'], stdin=fin).decode('utf-8')
            acc_true, p_true, r_true, f1_true = self._parse_conlleval_output(output)

            result = sequence_labeling.precision_recall_f1_score(self._m_y_true, self._m_y_pred)
            precision, recall, f1 = result["overall"]
            self.assertLess(abs(precision - p_true), 1e-4)
            self.assertLess(abs(recall - r_true), 1e-4)
            self.assertLess(abs(f1 - f1_true), 1e-4)

    def _load_labels(self, fname):
        y_true, y_pred = [], []
        with open(fname, 'r') as fin:
            tags_true, tags_pred = [], []
            for line in fin:
                line = line.rstrip()
                if len(line) == 0:
                    if len(tags_true) != 0:
                        y_true.append(tags_true)
                        y_pred.append(tags_pred)
                        tags_true, tags_pred = [], []
                else:
                    _, _, tag_true, tag_pred = line.split(' ')
                    tags_true.append(tag_true)
                    tags_pred.append(tag_pred)
            else:
                y_true.append(tags_true)
                y_pred.append(tags_pred)
        return y_true, y_pred

    def _parse_conlleval_output(self, text):
        eval_line = text.split('\n')[1]
        items = eval_line.split(' ')
        accuracy, precision, recall = [item[:-2] for item in items if '%' in item]
        f1 = items[-1]

        accuracy = float(accuracy) / 100
        precision = float(precision) / 100
        recall = float(recall) / 100
        f1 = float(f1) / 100
        return accuracy, precision, recall, f1


if __name__ == "__main__":
    unittest.main()

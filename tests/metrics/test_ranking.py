from jpyutils.mltools import metrics

import numpy as np

y_true = np.array([0, 0, 1, 1]) 
y_score = np.array([0.1, 0.4, 0.35, 0.8])
fpr, tpr, thresholds = metrics.roc_curve(y_true, y_score)
print metrics.auc(fpr, tpr, reorder=True)
print metrics.roc_auc_score(y_true, y_score)

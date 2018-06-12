from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import abc
import operator
import copy
import tensorflow as tf
import numpy as np
import yaml

from ... import utils

class DecomposableNLIModelData(object):
    """Manage data for decomposable model.

    Each sample shoule be as the following format (label is not a must):

        ([w1, w2, ..., wk], [t1, t2, ..., tk], label)

    """
    def __init__(self, data, word2id, maxlen=None):
        """Create a new DecomposableNLIModelData object.

        Parameters
        ----------
        data: list
            List of data samples. Each sample should be (sent1, sent2, label),
            or (sent1, sent2) if you don't have the label.

        word2id: dict
            Word dictionary, mapping word to id.

        Raises
        ------
        ValueError: If 'data' is empty or sample's format is not correct.

        """
        if len(data) == 0:
            raise ValueError("'data' is empty")
        if not 2 <= len(data[0]) <= 3:
            raise ValueError("Each sample's length in 'data' should be in 2 or 3")

        sentences1 = list(map(operator.itemgetter(0), data))
        sentences2 = list(map(operator.itemgetter(1), data))
        self._sentences1, self._sizes1 = utils.text.text2array(sentences1, word2id, maxlen=maxlen)
        self._sentences2, self._sizes2 = utils.text.text2array(sentences2, word2id, maxlen=maxlen)

        if len(data[0]) == 3:
            label_set = set(map(operator.itemgetter(2), data))
            self._label2id = dict(zip(sorted(label_set), range(len(label_set))))
            self._labels = np.array(list(map(lambda d: self._label2id[d[2]], data)))
        else:
            self._label2id = None
            self._labels = None
        self._batch_start_pos = 0
        self._samples_num = len(data)
        self._epoches_id = 0

    @property
    def data(self):
        """Data of this object.

        Returns
        -------
        data_tuple: tupe
            Data in format (sents1, sents2, sizes1, sizes2, labels)

        """
        return (self._sentences1, self._sentences2, self._sizes1, self._sizes2, self._labels)

    def next_batch(self, batch_size, epoches=None, shuffle=True):
        """Return next batch of current data manager.

        Parameters
        ----------
        batch_size: integer
            Batch size of returned data.

        epoches: integer
            Epoch number of this operation. After iterate over the data for epoches times,
            the data will be exhausted.

        shuffle: boolean
            Whether to shuffle data after each iteration(epoch).

        Returns
        -------
        data_tuple: tupe
            Data in format (sents1, sents2, sizes1, sizes2, labels)

        """
        if self._batch_start_pos + batch_size > self._samples_num:
            self._epoches_id += 1
            if shuffle:
                self.shuffle()
            self._batch_start_pos = 0

        # data exhausted
        if epoches is not None and self._epoches_id >= epoches:
            return None

        sentences1 = self._sentences1[self._batch_start_pos : self._batch_start_pos + batch_size]
        sentences2 = self._sentences2[self._batch_start_pos : self._batch_start_pos + batch_size]
        sizes1 = self._sizes1[self._batch_start_pos : self._batch_start_pos + batch_size]
        sizes2 = self._sizes2[self._batch_start_pos : self._batch_start_pos + batch_size]
        labels = self._labels[self._batch_start_pos : self._batch_start_pos + batch_size]
        self._batch_start_pos += batch_size
        return (sentences1, sentences2, sizes1, sizes2, labels)

    def shuffle(self):
        """Shuffle internal data and reset batch index.
        """
        rng_state = np.random.get_state()
        for array in [self._sentences1, self._sentences2, self._sizes1, self._sizes2, self._labels]:
            if array is not None:
                np.random.shuffle(array)
                np.random.set_state(rng_state)
        self._batch_start_pos = 0

    def reset(self, shuffle=True):
        """Reset internal state to initial value.

        Parameters
        ----------
        shuffle: boolean
            Whether or not to shuffle the data.

        """
        self._batch_start_pos = 0
        self._epoches_id = 0
        if shuffle:
            self.shuffle()


class DecomposableNLIModel(abc.ABC):
    """Decomposable Natural Language Inference Model.

    This is an implementation of paper "A Decomposable Attention Model
    for Natural Language Inference" by Parikh et al., 2016. Link:

            https://arxiv.org/pdf/1606.01933.pdf

    It applies feed forward MLPs to combinations of parts of the two sentences,
    without any recurrent structure.

    """
    def __init__(self, model_config):
        """Constrcutor

        Parameters
        ----------
        model_config: dict
            Configuration items of model, can be loaded from a YAML config file.

        """
        self._config = copy.deepcopy(model_config)
        #self._check_config() # TODO
        self._build_model()
        self._saver = tf.train.Saver(tf.trainable_variables(),
                                     max_to_keep=self._config['train']['max_to_keep'])
        self._global_step = 0

    def initialize(self, session, embeddings):
        """Initialize all model variables.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        embeddings: np.array
            The contents of the word embeddings with shape (vocab_size, embedding_size)

        """
        init_op = tf.global_variables_initializer()
        session.run(init_op, {self._ph_embeddings: embeddings})

    def initialize_embeddings(self, session, embeddings):
        """Initialize word embeddings.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        embeddings: np.array
            The contents of the word embeddings with shape (vocab_size, embedding_size)

        """
        init_op = tf.variables_initializer([self._embeddings])
        session.run(init_op, {self._ph_embeddings: embeddings})

    def train(self, session, train_data, dev_data):
        """Train the model.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        train_data: DecomposableNLIModelData
            Training instance of DecomposableNLIModelData.

        dev_data: DecomposableNLIModelData
            Developing instance of DecomposableNLIModelData.

        """
        logger = utils.utilities.get_logger(self.__class__.__name__)

        loss_tracker = 0
        accuracy_tracker = 0
        samples_num_tracker = 0
        best_dev_acc = 0
        batch_cnt = 0
        train_data.shuffle()
        for epoch in range(self._config['train']['num_epoches']):
            train_data.reset()
            while True:
                batch = train_data.next_batch(self._config['train']['batch_size'], epoches=1)
                if batch is None:
                    break
                batch_cnt += 1
                self._global_step += 1
                feed_dict = self._create_batch_feed(batch,
                                                    self._config['train']['dropout_rate'],
                                                    self._config['train']['l2_coef'])

                ops = [self._train_op, self._loss, self._accuracy]
                _, loss, accuracy = session.run(ops, feed_dict=feed_dict)

                loss_tracker += loss * batch[0].shape[0]
                accuracy_tracker += accuracy * batch[0].shape[0]
                samples_num_tracker += batch[0].shape[0]
                if batch_cnt % self._config['train']['report_interval'] == 0:
                    avg_loss = loss_tracker / samples_num_tracker
                    avg_accuracy = accuracy_tracker / samples_num_tracker
                    loss_tracker, accuracy_tracker, samples_num_tracker = 0, 0, 0

                    feed_dict = self._create_batch_feed(dev_data.data, 1,
                                                        self._config['train']['l2_coef'])
                    dev_loss, dev_acc = session.run([self._loss, self._accuracy],
                                                    feed_dict=feed_dict)

                    msg = '%d completed epochs, %d batches' % (epoch, batch_cnt)
                    msg += '\tAvg train loss: %f' % avg_loss
                    msg += '\tAvg train acc: %.4f' % avg_accuracy
                    msg += '\tValidation loss: %f' % dev_loss
                    msg += '\tValidation acc: %.4f' % dev_acc
                    if dev_acc > best_dev_acc:
                        best_dev_acc = dev_acc
                        self.save(session)
                        msg += '\t(saved model)'
                    logger.info(msg)

    def predict(self, session, data):
        """Predict label of data.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        data: DecomposableNLIModelData
            Instance of DecomposableNLIModelData.

        Returns
        -------
        label: np.array
            Predicted labels for data.

        """
        feed_dict = self._create_batch_feed(data.data, 1., 0)
        return session.run(self._preds, feed_dict=feed_dict)

    def evaluate(self, session, data):
        """Evaluate model performance.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        data: DecomposableNLIModelData
            Instance of DecomposableNLIModelData, data should have valid labels.

        Returns
        -------
        loss: float
            Cross entropy of this model.

        accuracy: float
            Accuracy of this model.

        """
        feed_dict = self._create_batch_feed(data.data, 1., 0)
        return session.run([self._loss, self._accuracy], feed_dict=feed_dict)

    @classmethod
    def restore(cls, session, model_dir, training=True):
        """Restore a model from a previous saved one on disk.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        model_dir: str
            A directory of previous saved model.

        training: boolean
            Set true if you want to train the model from previous one.

        Returns
        -------
        model: DecomposableNLIModel
            DecomposableNLIModel instance.

        Raises
        ------
        IOError: If 'model_dir' does not exist.
 
        """
        if not os.path.exists(model_dir):
            raise IOError("model dir [%s] does not exists" % (model_dir))

        with open("%s/config.yaml" % (model_dir)) as f:
            config = yaml.safe_load(f)
        m = cls(config)

        ckpt = tf.train.get_checkpoint_state("%s/model" % (model_dir))
        if ckpt and ckpt.model_checkpoint_path:
            m._saver.restore(session, ckpt.model_checkpoint_path)
            m._global_step = int(ckpt.model_checkpoint_path.rsplit('-', 1)[-1])
        else:
            return None

        if training:
            train_vars = [v for v in tf.global_variables() if v.name.startswith('training')]
            init_op = tf.variables_initializer(train_vars)
            session.run(init_op)
        return m

    def save(self, session):
        """Persist a model's information into disk.

        Parameters
        ----------
        session: tf.Session
            TensorFlow session instance.

        """
        save_dir = self._config['train']['save_dir']
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        self._saver.save(session, "%s/model/weights" % (save_dir), global_step=self._global_step)
        with open('%s/config.yaml' % (save_dir), "w") as fout:
            yaml.dump(self._config, fout)

    def _build_model(self):
        self._ph_embeddings = tf.placeholder(
                tf.float32,
                shape=(self._config['data']['vocab_size'], self._config['data']['embedding_size']),
                name='ph_embeddings')

        # sentence plaholders have shape (batch, time_steps)
        self._sentence1 = tf.placeholder(tf.int32, shape=(None, None), name='sentence1')
        self._sentence2 = tf.placeholder(tf.int32, shape=(None, None), name='sentence2')
        self._sentence1_size = tf.placeholder(tf.int32, shape=(None,), name='sent1_size')
        self._sentence2_size = tf.placeholder(tf.int32, shape=(None,), name='sent2_size')
        self._label = tf.placeholder(tf.int32, shape=(None,), name='label')

        self._learning_rate = tf.placeholder(tf.float32, shape=(), name='learning_rate')
        self._l2_coef = tf.placeholder(tf.float32, shape=(), name='l2_coef')
        self._dropout_rate = tf.placeholder(tf.float32, shape=(), name='dropout')
        self._clip_norm_value = tf.placeholder(tf.float32, shape=(), name='clip_norm_value')

        # we initialize the embeddings from a placeholder to circumvent
        # tensorflow's limitation of 2 GB nodes in the graph
        self._embeddings = tf.Variable(self._ph_embeddings, trainable=False, validate_shape=True,
                                                            name='embeddings')

        # clip the sentences to the length of the longest one in the batch
        # this saves processing time
        clipped_sent1 = utils.text.clip_sentence(self._sentence1, self._sentence1_size)
        clipped_sent2 = utils.text.clip_sentence(self._sentence2, self._sentence2_size)
        embedded1 = tf.nn.embedding_lookup(self._embeddings, clipped_sent1)
        embedded2 = tf.nn.embedding_lookup(self._embeddings, clipped_sent2)

        # project dimension of input embeddings into another dimension
        self._projected1, self._projected_dim = self._project_embeddings(embedded1)
        self._projected2, _ = self._project_embeddings(embedded2, True)

        # the architecture has 3 main steps: soft align, compare and aggregate
        # alpha and beta have shape (batch, time_steps, embeddings)
        self._alpha, self._beta = self._attend(self._projected1, self._projected2)

        self._v1 = self._compare(self._projected1, self._beta, self._sentence1_size)
        self._v2 = self._compare(self._projected2, self._alpha, self._sentence2_size, True)

        self._logits = self._aggregate(self._v1, self._v2)
        self._preds = tf.argmax(self._logits, axis=1, name='prediction')

        hits = tf.equal(tf.cast(self._preds, tf.int32), self._label)
        self._accuracy = tf.reduce_mean(tf.cast(hits, tf.float32), name='accuracy')

        cross_entropy = tf.nn.sparse_softmax_cross_entropy_with_logits(logits=self._logits,
                                                                       labels=self._label)
        self._cross_entropy = tf.reduce_mean(cross_entropy)

        weights = [w for w in tf.trainable_variables() if 'weight' in w.name]
        l2_partial_sum = tf.reduce_sum([tf.nn.l2_loss(weight) for weight in weights])
        l2_loss = tf.multiply(self._l2_coef, l2_partial_sum, name='l2_loss')
        self._loss = tf.add(self._cross_entropy, l2_loss, name='loss')

        self._build_training_tensors()

    def _build_training_tensors(self):
        with tf.variable_scope('training'):
            if self._config['train']['algorithm'] == 'adagrad':
                optimizer = tf.train.AdagradOptimizer(self._learning_rate)

            elif self._config['train']['algorithm'] == 'adam':
                optimizer = tf.train.AdamOptimizer(self._learning_rate)

            elif self._config['train']['algorithm'] == 'adadelta':
                optimizer = tf.train.AdadeltaOptimizer(self._learning_rate)

            else:
                raise ValueError('Unknown optimizer: %s' % self._config['train']['algorithm'])

            gradients, variables = zip(*optimizer.compute_gradients(self._loss))
            if self._clip_norm_value is not None:
                gradients, _ = tf.clip_by_global_norm(gradients, self._clip_norm_value)
            self._train_op = optimizer.apply_gradients(zip(gradients, variables))

    def _project_embeddings(self, embeddings, reuse_weights=False):
        """Project word embeddings into another dimension.

        Parameters
        ----------
        embeddings: tf.Tensor
            A tensor with shape (batch, time_steps, embedding_size).

        reuse_weights: boolean
            Reuse weights in internal layers.

        Returns
        -------
        projected: tf.Tensor
            Projected embeddings with shape (batch, time_steps, num_units)

        projection_dim: integer
            Projected dimension.

        """
        if not self._config['parameters']['projection']['enabled']:
            return embeddings, self._config['data']['embedding_size']

        embedding_size = self._config['data']['embedding_size']
        projection_dim = self._config['parameters']['projection']['dim']
        embeddings_2d = tf.reshape(embeddings, [-1, embedding_size])
        with tf.variable_scope('projection', reuse=reuse_weights):
            weights = tf.get_variable('weights',
                                      shape=[embedding_size, projection_dim],
                                      initializer=tf.random_normal_initializer(0.0, 0.01))
            #TODO: use a more effective initializer
            projected = tf.matmul(embeddings_2d, weights)

        time_steps = tf.shape(embeddings)[1]
        projected_3d = tf.reshape(projected, [-1, time_steps, projection_dim])
        return projected_3d, projection_dim

    def _attend(self, sent1, sent2):
        """Compute inter-sentence attention. This is step 1 (attend) in the paper.

        Parameters
        ----------
        sent1: tf.Tensor
            Tensor in shape (batch, time_steps, num_units), the projected sentence 1.

        sent2: tf.Tensor
            Tensor in shape (batch, time_steps, num_units), the projected sentence 2.

        Returns
        -------
        alpha: tf.Tensor
            Subphrase in sent1 that is softly aligned to sent2.

        beta: tf.Tensor
            Subphrase in sent2 that is softly aligned to sent1.

        """
        # this is F in the paper
        with tf.variable_scope('inter-attention') as self._attend_scope:
            # repr1 has shape (batch, time_steps, num_units)
            # repr2 has shape (batch, num_units, time_steps)
            repr1 = self._transform_attend(sent1, self._sentence1_size)
            repr2 = self._transform_attend(sent2, self._sentence2_size, True)
            repr2 = tf.transpose(repr2, [0, 2, 1])

            # compute the unnormalized attention for all word pairs
            # raw_attentions has shape (batch, time_steps1, time_steps2)
            self._raw_attentions = tf.matmul(repr1, repr2)

            # now get the attention softmaxes
            masked1 = utils.text.mask3d(self._raw_attentions, self._sentence2_size, -np.inf)
            self._att_sent1 = tf.nn.softmax(masked1)

            att_transposed = tf.transpose(self._raw_attentions, [0, 2, 1])
            masked2 = utils.text.mask3d(att_transposed, self._sentence1_size, -np.inf)
            self._att_sent2 = tf.nn.softmax(masked2)

            alpha = tf.matmul(self._att_sent2, sent1, name='alpha')
            beta = tf.matmul(self._att_sent1, sent2, name='beta')
        return alpha, beta

    def _compare(self, sentence, soft_alignment, sentence_length, reuse_weights=False):
        """Apply a feed forward neural network to compare one sentence
        to its soft alignment with the other.

        Parameters
        ----------
        sentence: tf.Tensor
            A tensor with shape (batch, time_steps, num_units).

        soft_alignment: tf.Tensor
            Soft alignment of sentence. A tensor with shape (batch, time_steps, num_units).

        sentence_length: tf.Tensor
            Length of sentence. A tensor with shape (batch, ).

        reuse_weights: boolean
            Whether to reuse weights in the internal layers

        Returns
        -------
        compare_res: tf.Tensor
            Compare result. A tensor with shape (batch, time_steps, num_units)

        """
        with tf.variable_scope('comparison', reuse=reuse_weights) as self._compare_scope:
            # sent_and_alignment has shape (batch, time_steps, num_units)
            inputs = [sentence, soft_alignment, sentence - soft_alignment,
                      sentence * soft_alignment]
            sent_and_alignment = tf.concat(axis=2, values=inputs)

            output = self._transform_compare(sent_and_alignment, sentence_length, reuse_weights)
        return output

    def _aggregate(self, v1, v2):
        """Aggregate the representations induced from both sentences and their representations.

        Parameters
        ----------
        v1: tf.Tensor
            sent1's compare result. A tensor with shape (batch, time_steps, num_units).

        v2: tf.Tensor
            sent2's compare result. A tensor with shape (batch, time_steps, num_units).

        Returns
        -------
        logits: tf.Tensor
            Logits over classes. A tensor with shape (batch, num_classes).

        """
        inputs = self._create_aggregate_input(v1, v2)
        with tf.variable_scope('aggregation') as self._aggregate_scope:
            pre_logits = self._feedforward(inputs,
                                           self._aggregate_scope,
                                           self._config['parameters']['aggregate']['dim'])
            with tf.variable_scope('linear'):
                pre_logits_dim = self._config['parameters']['aggregate']['dim'][-1]
                num_classes = self._config['data']['num_classes']
                weights = tf.get_variable('weights',
                                          shape=[pre_logits_dim, num_classes],
                                          initializer=tf.random_normal_initializer(0.0, 0.1))

                bias = tf.get_variable('bias',
                                       shape=[num_classes],
                                       initializer=tf.zeros_initializer())
            logits = tf.nn.xw_plus_b(pre_logits, weights, bias)
        return logits

    def _feedforward(self, inputs, scope, hidden_units, reuse_weights=False, initializer=None):
        """Apply feed forward layers.

        Parameters
        ----------
        inputs: tf.Tensor
            Input tensor.

        scope: str
            Variable scope.

        hidden_units: list
            List of integers, each one is the feed forward neural network's one layer size.

        reuse_weights: boolean
            Reuse the weights inside the same tensorflow variable scope.

        initializer: function
            A function which returns an Op that initializes variables.

        Returns
        -------
        outputs: tf.Tensor
            Result tensor.

        """
        scope = scope or 'feedforward'
        if isinstance(hidden_units, int):
            hidden_units = [hidden_units]
        with tf.variable_scope(scope, reuse=reuse_weights):
            last_inputs = inputs
            for i, dim in enumerate(hidden_units):
                with tf.variable_scope('layer_%d' % (i)):
                    inputs = tf.nn.dropout(last_inputs, self._dropout_rate)
                    last_inputs = tf.layers.dense(inputs, dim, tf.nn.relu,
                                                  kernel_initializer=initializer)
        return last_inputs

    def _transform_attend(self, sentence, length, reuse_weights=False):
        """Apply the transformation on each sentence before attending over each other.
        In the original model, it is a two layer feed forward network.

        Parameters
        ----------
        sentence: tf.Tensor
            Input sentence. A tensor with shape (batch, time_steps, num_units).

        length: tf.Tensor
            Real length of the sentence. Not used in this class.

        reuse_weights: boolean
            Whether to reuse weights inside this scope.

        Returns
        -------
        output: tf.Tensor
            A tensor with shape (batch, time_steps, num_units).

        """
        return self._feedforward(sentence,
                                 self._attend_scope,
                                 self._config['parameters']['attention']['dim'],
                                 reuse_weights=reuse_weights)

    def _transform_compare(self, sentence, length, reuse_weights=False):
        """Apply the transformation on each attended token before comparing.
        In the original model, it is a two layer feed forward network.

        Parameters
        ----------
        sentence: tf.Tensor
            Input sentence. A tensor with shape (batch, time_steps, num_units).

        length: tf.Tensor
            Real length of the sentence. Not used in this class.

        reuse_weights: boolean
            Whether to reuse weights inside this scope.

        Returns
        -------
        Output: tf.Tensor
            A tensor with shape (batch, time_steps, num_units).

        """
        return self._feedforward(sentence,
                                 self._compare_scope,
                                 self._config['parameters']['compare']['dim'],
                                 reuse_weights=reuse_weights)

    def _create_aggregate_input(self, v1, v2):
        """Create and return the input to the aggregate step.

        Parameters
        ----------
        v1: tf.Tensor
            Tensor with shape (batch, time_steps, num_units).

        v2: tf.Tensor
            Tensor with shape (batch, time_steps, num_units)

        Returns
        -------
        input_tensor: tf.Tensor
            A tensor with shape (batch, num_aggregate_inputs)

        """
        # sum over time steps; resulting shape is (batch, num_units)
        v1 = utils.text.mask3d(v1, self._sentence1_size, 0, 1)
        v2 = utils.text.mask3d(v2, self._sentence2_size, 0, 1)
        v1_sum = tf.reduce_sum(v1, 1)
        v2_sum = tf.reduce_sum(v2, 1)
        v1_max = tf.reduce_max(v1, 1)
        v2_max = tf.reduce_max(v2, 1)
        return tf.concat(axis=1, values=[v1_sum, v2_sum, v1_max, v2_max])

    def _create_batch_feed(self, batch_data, dropout_rate, l2_coef):
        sentences1, sentences2, sizes1, sizes2, labels = batch_data
        feed_dict = {
                self._sentence1: sentences1,
                self._sentence2: sentences2,
                self._sentence1_size: sizes1,
                self._sentence2_size: sizes2,
                self._label: labels,
                self._dropout_rate: dropout_rate,
                self._l2_coef: l2_coef,
                self._learning_rate: self._config['train']['learning_rate'],
                self._clip_norm_value: self._config['train']['clip_norm_value'],
        }
        return feed_dict


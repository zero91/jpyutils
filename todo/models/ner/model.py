from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import abc
import copy
import operator
import numpy as np
import tensorflow as tf
import jpyutils

class BiLSTMCRFModel(abc.ABC):
    def __init__(self, model_config):
        config_tpl = {
            "chars_num": 4412,
            "char_dim": 100,
            "seg_ids_num": 4,
            "seg_id_dim": 20,
            "lstm_dim": 100,
            "crf_enabled": True,
            "labels_num": 7,
            "optimizer": "adam",
            "learning_rate": 0.0001,
            "clip": 5.0,
            "keep_prob": 0.8,
        }
        self._m_config = copy.deepcopy(model_config)
        self._build_graph()
        self._m_saver = tf.train.Saver(tf.global_variables(), max_to_keep=5)

    def initialize(self, session, embeddings):
        init_op = tf.global_variables_initializer()
        session.run(init_op, feed_dict={self._m_ph_char_embeddings: embeddings})
        return 

        ckpt = tf.train.get_checkpoint_state(path)
        if ckpt and tf.train.checkpoint_exists(ckpt.model_checkpoint_path):
            logger.info("Reading model parameters from %s" % ckpt.model_checkpoint_path)
            model.saver.restore(session, ckpt.model_checkpoint_path)
        else:
            logger.info("Created model with fresh parameters.")
            session.run(tf.global_variables_initializer())
            if config["pre_emb"]:
                emb_weights = session.run(model.char_lookup.read_value())
                emb_weights = load_vec(config["emb_file"],id_to_char,
                                       config["char_dim"], emb_weights)
                session.run(model.char_lookup.assign(emb_weights))
                logger.info("Load pre-trained embedding.")
        return model

    def train(self, session, train_data, dev_data=None):
        for i in range(200):
            for batch_data in train_data.iter_batch(shuffle=True):
                _, global_step, loss = session.run(
                    [self._m_train_op, self._m_global_step, self._m_loss],
                    feed_dict = self._create_feed_dict(batch_data, self._m_config["keep_prob"])
                )
                if global_step % 100 == 0:
                    print(loss, self.evaluate(session, dev_data, train_data.id2tag))
            print(loss)

    def evaluate(self, session, data, id2label):
        prediction = self.predict(session, data, id2label)

        y_true = [[t for c, t in sentence] for _, sentence in sorted(data.id2sentence.items())]
        y_pred = [sentence for _, (sentence, value) in sorted(prediction.items())]
        eval_res = jpyutils.mltools.metrics.sequence_labeling.precision_recall_f1_score(y_true, y_pred)
        return eval_res

    def predict(self, session, data, id2label=None):
        prediction = dict()
        small = -1e8
        start = np.asarray([small] * self._m_config['labels_num'] + [0]).reshape((1, -1))
        transitions = session.run(self._m_transitions)
        for batch_data in data.iter_batch(shuffle=False):
            batch_logits = session.run(
                self._m_logits,
                feed_dict=self._create_feed_dict(batch_data, 1.0)
            )
            for sentence_id, length, logits in zip(batch_data[0], batch_data[1], batch_logits):
                logits = logits[:length]
                logits = np.concatenate([logits, small * np.ones([length, 1])], axis=1)
                logits = np.concatenate([start, logits], axis=0)
                best_path, path_score = tf.contrib.crf.viterbi_decode(logits, transitions)
                if id2label is None:
                    prediction[sentence_id] = [best_path[1:], path_score]
                else:
                    prediction[sentence_id] = [list(map(id2label.get, best_path[1:])), path_score]
        return prediction

    def _build_graph(self):
        self._build_model()
        self._build_loss()
        self._build_optimizer()

    def _build_model(self):
        # Step 1. Placeholders
        # Pre-trained Variables
        self._m_ph_char_embeddings = tf.placeholder(
            tf.float32,
            shape=(self._m_config['chars_num'], self._m_config['char_dim']),
            name='ph_char_embeddings'
        )
        # Input Variables
        self._m_ph_sentence_lengths = tf.placeholder(
            tf.int32,
            shape=(None,),
            name="ph_sentence_lengths"
        )
        self._m_ph_char_ids = tf.placeholder(tf.int32, shape=(None, None), name="ph_char_ids")
        self._m_ph_seg_ids = tf.placeholder(tf.int32, shape=(None, None), name="ph_seg_ids")
        self._m_ph_labels = tf.placeholder(tf.int32, shape=(None, None), name="ph_labels")
        # Hyper-Parameters
        self._m_ph_keep_prob = tf.placeholder(tf.float32, name="ph_keep_prob")

        # Step 2. Input Embeddings
        with tf.variable_scope("input_embeddings"):
            self._m_char_embeddings = tf.Variable(
                self._m_ph_char_embeddings,
                trainable=False,
                validate_shape=True,
                name="char_embeddings"
            )
            self._m_seg_id_embeddings = tf.get_variable(
                "seg_id_embedding",
                shape=(self._m_config["seg_ids_num"], self._m_config["seg_id_dim"]),
                initializer=tf.contrib.layers.xavier_initializer()
            )
            self._m_embeddings = tf.concat(
                [
                    tf.nn.embedding_lookup(self._m_char_embeddings, self._m_ph_char_ids),
                    tf.nn.embedding_lookup(self._m_seg_id_embeddings, self._m_ph_seg_ids)
                ],
                axis=-1
            )

        # Step 3. Bi-LSTM Layer
        self._m_lstm_inputs = tf.nn.dropout(self._m_embeddings, self._m_ph_keep_prob)
        with tf.variable_scope("Bi-LSTM"):
            lstm_cell = {}
            for direction in ["forward", "backward"]:
                with tf.variable_scope(direction):
                    lstm_cell[direction] = tf.contrib.rnn.CoupledInputForgetGateLSTMCell(
                        self._m_config["lstm_dim"],
                        use_peepholes=True,
                        initializer=tf.contrib.layers.xavier_initializer(),
                        state_is_tuple=True
                    )
            outputs, final_states = tf.nn.bidirectional_dynamic_rnn(
                lstm_cell["forward"],
                lstm_cell["backward"],
                self._m_lstm_inputs,
                dtype=tf.float32,
                sequence_length=self._m_ph_sentence_lengths
            )
        self._m_lstm_outputs = tf.concat(outputs, axis=2)

        # Step 4. Output Layer
        with tf.variable_scope("projection"):
            with tf.variable_scope("hidden"):
                W = tf.get_variable(
                    "W",
                    shape=[self._m_config['lstm_dim'] * 2, self._m_config['lstm_dim']],
                    dtype=tf.float32,
                    initializer=tf.contrib.layers.xavier_initializer()
                )
                b = tf.get_variable(
                    "b",
                    shape=[self._m_config['lstm_dim']],
                    dtype=tf.float32,
                    initializer=tf.zeros_initializer()
                )
                lstm_outputs = tf.reshape(self._m_lstm_outputs,
                                          shape=[-1, self._m_config['lstm_dim'] * 2])
                hidden = tf.tanh(tf.nn.xw_plus_b(lstm_outputs, W, b))

            with tf.variable_scope("logits"):
                W = tf.get_variable(
                    "W",
                    shape=[self._m_config["lstm_dim"], self._m_config["labels_num"]],
                    dtype=tf.float32,
                    initializer=tf.contrib.layers.xavier_initializer()
                )
                b = tf.get_variable(
                    "b",
                    shape=[self._m_config["labels_num"]],
                    dtype=tf.float32,
                    initializer=tf.zeros_initializer()
                )
                logits = tf.nn.xw_plus_b(hidden, W, b)

        self._m_batch_size = tf.shape(self._m_ph_char_ids)[0]
        self._m_num_steps = tf.shape(self._m_ph_char_ids)[-1]
        self._m_logits = tf.reshape(logits, [-1, self._m_num_steps, self._m_config["labels_num"]])

    def _build_loss(self):
        if self._m_config.get("crf_enabled", False):
            self._build_crf_loss()
        else:
            self._build_cross_entropy_loss()

    def _build_cross_entropy_loss(self):
        self._m_sentence_masks = tf.cast(
            tf.sequence_mask(self._m_ph_sentence_lengths, self._m_num_steps),
            tf.float32
        )
        word_cross_entropy = tf.nn.sparse_softmax_cross_entropy_with_logits(
            labels=self._m_ph_labels,
            logits=self._m_logits
        )
        sentence_cross_entropy = tf.reduce_sum(
            #tf.boolean_mask(word_cross_entropy, self._m_sentence_masks)
            tf.multiply(word_cross_entropy, self._m_sentence_masks),
            axis=1
        )
        sentence_mean_cross_entropy = tf.divide(
            sentence_cross_entropy,
            tf.cast(self._m_ph_sentence_lengths, tf.float32)
        )

        self._m_loss = tf.reduce_mean(sentence_mean_cross_entropy)

    def _build_crf_loss(self):
        #log_likelihood, self._m_transition_params = tf.contrib.crf.crf_log_likelihood(
        #    inputs = self._m_logits,
        #    tag_indices = self._m_ph_labels,
        #    sequence_lengths = self._m_ph_sentence_lengths,
        #)
        #self._m_loss = -tf.reduce_mean(log_likelihood)
        #return 
        with tf.variable_scope("crf_loss"):
            small = -1e8
            start_logits = tf.concat(
                [
                    small * tf.ones(shape=(self._m_batch_size, 1, self._m_config["labels_num"])),
                    tf.zeros(shape=(self._m_batch_size, 1, 1))
                ],
                axis=-1
            )
            pad_logits = tf.cast(
                small * tf.ones([self._m_batch_size, self._m_num_steps, 1]),
                tf.float32
            )
            logits = tf.concat([self._m_logits, pad_logits], axis=-1)
            logits = tf.concat([start_logits, logits], axis=1)
            targets = tf.concat(
                [
                    tf.cast(self._m_config["labels_num"] * tf.ones([self._m_batch_size, 1]),
                            tf.int32),
                    self._m_ph_labels
                ],
                axis=-1
            )
            self._m_transitions = tf.get_variable(
                "transitions",
                shape=(self._m_config["labels_num"] + 1, self._m_config["labels_num"] + 1),
                initializer=tf.contrib.layers.xavier_initializer()
            )
            log_likelihood, self._m_transitions = tf.contrib.crf.crf_log_likelihood(
                inputs=logits,
                tag_indices=targets,
                transition_params=self._m_transitions,
                sequence_lengths=self._m_ph_sentence_lengths + 1
            )
            self._m_loss = tf.reduce_mean(-log_likelihood)

    def _build_optimizer(self):
        with tf.variable_scope("optimizer"):
            optimizer = self._m_config["optimizer"]
            if optimizer == "sgd":
                self._m_optimizer = tf.train.GradientDescentOptimizer(
                                            self._m_config["learning_rate"])
            elif optimizer == "adam":
                self._m_optimizer = tf.train.AdamOptimizer(self._m_config["learning_rate"])

            elif optimizer == "adgrad":
                self._m_optimizer = tf.train.AdagradOptimizer(self._m_config["learning_rate"])

            else:
                raise KeyError("Unsupported optimizer '%s'" % (optimizer))

            grads_vars = [
                [tf.clip_by_value(g, -self._m_config["clip"], self._m_config["clip"]), v]
                for g, v in self._m_optimizer.compute_gradients(self._m_loss)
            ]
            self._m_global_step = tf.Variable(0, trainable=False, name="global_step")
            self._m_train_op = self._m_optimizer.apply_gradients(grads_vars, self._m_global_step)

    def _create_feed_dict(self, batch_data, keep_prob):
        _, sentence_lengths, sentence_char_ids, sentence_seg_ids, sentence_labels = batch_data
        feed_dict = {
            self._m_ph_sentence_lengths: sentence_lengths,
            self._m_ph_char_ids: sentence_char_ids,
            self._m_ph_seg_ids: sentence_seg_ids,
            self._m_ph_labels: sentence_labels,
            self._m_ph_keep_prob: keep_prob,
        }
        return feed_dict

    def save_model(sess, model, path, logger):
        checkpoint_path = os.path.join(path, "ner.ckpt")
        model.saver.save(sess, checkpoint_path)
        logger.info("model saved")

    def evaluate_line(self, sess, inputs, id_to_tag):
        trans = self.trans.eval()
        lengths, scores = self.run_step(sess, False, inputs)
        batch_paths = self.decode(scores, lengths, trans)
        tags = [id_to_tag[idx] for idx in batch_paths[0]]
        return result_to_json(inputs[0][0], tags)

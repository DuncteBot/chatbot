#  MIT License
#
#  Copyright (c) 2020 Duncan Sterken
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#

import tensorflow as tf
import tensorflow_addons as tfa

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from sklearn.model_selection import train_test_split

import unicodedata
import re
import numpy as np
import os
import io
import time

from training.train import max_length_output, max_length_input


class NMTDataset:
    def __init__(self, file_func):
        self.file_func = file_func
        self.inp_tokenizer = None
        self.targ_tokenizer = None


    def unicode_to_ascii(self, s):
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    ## Step 1 and Step 2
    def preprocess_sentence(self, w):
        w = self.unicode_to_ascii(w.lower().strip())

        # creating a space between a word and the punctuation following it
        # eg: "he is a boy." => "he is a boy ."
        # Reference:- https://stackoverflow.com/questions/3645931/python-padding-punctuation-with-white-spaces-keeping-punctuation
        w = re.sub(r"([?.!,¿])", r" \1 ", w)
        w = re.sub(r'[" "]+', " ", w)

        # replacing everything with space except (a-z, A-Z, ".", "?", "!", ",")
        w = re.sub(r"[^a-zA-Z?.!,¿]+", " ", w)

        w = w.strip()

        # adding a start and an end token to the sentence
        # so that the model know when to start and stop predicting.
        w = '<start> ' + w + ' <end>'
        return w

    def create_dataset(self, from_file, to_file, num_examples):
        # path : path to spa-eng.txt file
        # num_examples : Limit the total number of training example for faster training (set num_examples = len(lines) to use full data)
        from_lines = io.open(from_file, encoding='UTF-8').read().strip().split('\n')
        from_lines = [self.preprocess_sentence(line) for line in from_lines[:num_examples]]

        to_lines = io.open(to_file, encoding='UTF-8').read().strip().split('\n')
        to_lines = [self.preprocess_sentence(line) for line in to_lines[:num_examples]]

        return zip(from_lines, to_lines)

    # Step 3 and Step 4
    def tokenize(self, lang):
        # lang = list of sentences in a language

        # print(len(lang), "example sentence: {}".format(lang[0]))
        lang_tokenizer = tf.keras.preprocessing.text.Tokenizer(filters='', oov_token='<OOV>')
        lang_tokenizer.fit_on_texts(lang)

        ## tf.keras.preprocessing.text.Tokenizer.texts_to_sequences converts string (w1, w2, w3, ......, wn)
        ## to a list of correspoding integer ids of words (id_w1, id_w2, id_w3, ...., id_wn)
        tensor = lang_tokenizer.texts_to_sequences(lang)

        ## tf.keras.preprocessing.sequence.pad_sequences takes argument a list of integer id sequences
        ## and pads the sequences to match the longest sequences in the given input
        tensor = tf.keras.preprocessing.sequence.pad_sequences(tensor, padding='post')

        return tensor, lang_tokenizer

    def load_dataset(self, from_file, to_file, num_examples=None):
        # creating cleaned input, output pairs
        input, target = self.create_dataset(from_file, to_file, num_examples)

        input_tensor, inp_tokenizer = self.tokenize(input)
        target_tensor, targ_tokenizer = self.tokenize(target)

        return input_tensor, target_tensor, inp_tokenizer, targ_tokenizer

    def call(self, num_examples, BUFFER_SIZE, BATCH_SIZE):
        input_tensor, target_tensor, self.inp_tokenizer, self.targ_tokenizer = self.load_dataset(
            self.file_func('from'),
            self.file_func('to'),
            num_examples
        )

        input_tensor_train, input_tensor_val, target_tensor_train, target_tensor_val = train_test_split(input_tensor,
                                                                                                        target_tensor,
                                                                                                        test_size=0.2)

        train_dataset = tf.data.Dataset.from_tensor_slices((input_tensor_train, target_tensor_train))
        train_dataset = train_dataset.shuffle(BUFFER_SIZE).batch(BATCH_SIZE, drop_remainder=True)

        val_dataset = tf.data.Dataset.from_tensor_slices((input_tensor_val, target_tensor_val))
        val_dataset = val_dataset.batch(BATCH_SIZE, drop_remainder=True)

        return train_dataset, val_dataset, self.inp_tokenizer, self.targ_tokenizer

#####


class Encoder(tf.keras.Model):
    def __init__(self, vocab_size, embedding_dim, enc_units, batch_sz):
        super(Encoder, self).__init__()
        self.batch_sz = batch_sz
        self.enc_units = enc_units
        self.embedding = tf.keras.layers.Embedding(vocab_size, embedding_dim)

        ## ________ LSTM layer in Encoder ------- ##
        self.lstm_layer = tf.keras.layers.LSTM(self.enc_units,
                                               return_sequences=True,
                                               return_state=True,
                                               recurrent_initializer='glorot_uniform')

    def call(self, x, hidden):
        x = self.embedding(x)
        output, h, c = self.lstm_layer(x, initial_state=hidden)
        return output, h, c

    def initialize_hidden_state(self):
        return [tf.zeros((self.batch_sz, self.enc_units)), tf.zeros((self.batch_sz, self.enc_units))]


class Decoder(tf.keras.Model):
    def __init__(self, vocab_size, embedding_dim, dec_units, batch_sz, attention_type='luong'):
        super(Decoder, self).__init__()
        self.batch_sz = batch_sz
        self.dec_units = dec_units
        self.attention_type = attention_type

        # Embedding Layer
        self.embedding = tf.keras.layers.Embedding(vocab_size, embedding_dim)

        # Final Dense layer on which softmax will be applied
        self.fc = tf.keras.layers.Dense(vocab_size)

        # Define the fundamental cell for decoder recurrent structure
        self.decoder_rnn_cell = tf.keras.layers.LSTMCell(self.dec_units)

        # Sampler
        self.sampler = tfa.seq2seq.sampler.TrainingSampler()

        # Create attention mechanism with memory = None
        self.attention_mechanism = self.build_attention_mechanism(self.dec_units,
                                                                  None, self.batch_sz * [max_length_input],
                                                                  self.attention_type)

        # Wrap attention mechanism with the fundamental rnn cell of decoder
        self.rnn_cell = self.build_rnn_cell(batch_sz)

        # Define the decoder with respect to fundamental rnn cell
        self.decoder = tfa.seq2seq.BasicDecoder(self.rnn_cell, sampler=self.sampler, output_layer=self.fc)

    def build_rnn_cell(self, batch_sz):
        rnn_cell = tfa.seq2seq.AttentionWrapper(self.decoder_rnn_cell,
                                                self.attention_mechanism, attention_layer_size=self.dec_units)
        return rnn_cell

    def build_attention_mechanism(self, dec_units, memory, memory_sequence_length, attention_type='luong'):
        # ------------- #
        # typ: Which sort of attention (Bahdanau, Luong)
        # dec_units: final dimension of attention outputs
        # memory: encoder hidden states of shape (batch_size, max_length_input, enc_units)
        # memory_sequence_length: 1d array of shape (batch_size) with every element set to max_length_input (for masking purpose)

        if (attention_type == 'bahdanau'):
            return tfa.seq2seq.BahdanauAttention(units=dec_units, memory=memory,
                                                 memory_sequence_length=memory_sequence_length)
        else:
            return tfa.seq2seq.LuongAttention(units=dec_units, memory=memory,
                                              memory_sequence_length=memory_sequence_length)

    def build_initial_state(self, batch_sz, encoder_state, Dtype):
        decoder_initial_state = self.rnn_cell.get_initial_state(batch_size=batch_sz, dtype=Dtype)
        decoder_initial_state = decoder_initial_state.clone(cell_state=encoder_state)
        return decoder_initial_state

    def call(self, inputs, initial_state):
        x = self.embedding(inputs)
        outputs, _, _ = self.decoder(x, initial_state=initial_state,
                                     sequence_length=self.batch_sz * [max_length_output - 1])
        return outputs
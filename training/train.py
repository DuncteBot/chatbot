import tensorflow as tf
# Let's limit the #training examples for faster training
from training.NMTDataset import NMTDataset, Encoder, Decoder
from helpers import get_train_path, get_test_path

# Selecting the file from the testing base to use
USE_TEST_PATH = True

BUFFER_SIZE = 32000
BATCH_SIZE = 64
num_examples = 30000

if USE_TEST_PATH:
    dataset_creator = NMTDataset(lambda file: get_test_path)
else:
    dataset_creator = NMTDataset(lambda file: get_train_path)

train_dataset, val_dataset, inp_lang, targ_lang = dataset_creator.call(num_examples, BUFFER_SIZE, BATCH_SIZE)

example_input_batch, example_target_batch = next(iter(train_dataset))

print("example_input_batch, example_target_batch")
print(example_input_batch, example_target_batch)

vocab_inp_size = len(inp_lang.word_index)+1
vocab_tar_size = len(targ_lang.word_index)+1
max_length_input = example_input_batch.shape[1]
max_length_output = example_target_batch.shape[1]

embedding_dim = 256
units = 1024
steps_per_epoch = num_examples // BATCH_SIZE

print("max_length_spanish, max_length_english, vocab_size_spanish, vocab_size_english")
print(max_length_input, max_length_output, vocab_inp_size, vocab_tar_size)

## Test Encoder Stack

encoder = Encoder(vocab_inp_size, embedding_dim, units, BATCH_SIZE)


# sample input
sample_hidden = encoder.initialize_hidden_state()
sample_output, sample_h, sample_c = encoder(example_input_batch, sample_hidden)
print('Encoder output shape: (batch size, sequence length, units) {}'.format(sample_output.shape))
print('Encoder h vector shape: (batch size, units) {}'.format(sample_h.shape))
print('Encoder c vector shape: (batch size, units) {}'.format(sample_c.shape))


# Test decoder stack

decoder = Decoder(vocab_tar_size, embedding_dim, units, BATCH_SIZE, 'luong')
sample_x = tf.random.uniform((BATCH_SIZE, max_length_output))
decoder.attention_mechanism.setup_memory(sample_output)
initial_state = decoder.build_initial_state(BATCH_SIZE, [sample_h, sample_c], tf.float32)


sample_decoder_outputs = decoder(sample_x, initial_state)

print("Decoder Outputs Shape: ", sample_decoder_outputs.rnn_output.shape)

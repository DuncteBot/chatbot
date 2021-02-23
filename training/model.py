from training.NMTDataset import NMTDataset
from helpers import get_train_path, get_test_path

# Selecting the file from the testing base to use
USE_TEST_PATH = True

BUFFER_SIZE = 32000
BATCH_SIZE = 64
# Let's limit the training examples for faster training
num_examples = 30000

if USE_TEST_PATH:
    dataset_creator = NMTDataset(lambda file: get_test_path)
else:
    dataset_creator = NMTDataset(lambda file: get_train_path)

train_dataset, val_dataset, inp_data, targ_data = dataset_creator.call(num_examples, BUFFER_SIZE, BATCH_SIZE)

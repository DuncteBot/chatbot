import os

from config import database_path, data_path, filename_regex, prepare_path


def get_dataset_path(timeframe):
    return os.path.join(data_path, timeframe)


def get_timeframe_path(timeframe):
    return os.path.join(data_path, f"RC_{timeframe}")


def get_db_path(timeframe):
    return os.path.join(database_path, f"{timeframe}.db")


def get_timeframes():
    timeframes = []
    
    for f in os.listdir(data_path):
        if os.path.isfile(get_dataset_path(f)):
            print(f)
            match = filename_regex.match(f)
            if match is not None:
                timeframe = match.group(1)
                timeframes.append(timeframe)
            
    return timeframes


def create_table_if_not_exists(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS parent_reply (
    parent_id TEXT PRIMARY KEY,
    comment_id TEXT UNIQUE,
    parent TEXT,
    comment TEXT,
    subreddit TEXT,
    unix INT,
    score INT
    )""")


def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data


def acceptable(data):
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True


def get_test_path(variant):
    return os.path.join(prepare_path, f"test.{variant}")


def get_train_path(variant):
    return os.path.join(prepare_path, f"train.{variant}")


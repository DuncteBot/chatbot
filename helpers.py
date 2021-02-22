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


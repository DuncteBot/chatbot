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

import sqlite3
import pandas as pd
from helpers import get_db_path, get_test_path, get_train_path

# array has to be static and cannot be loaded with get_timeframes
# because the large files will be deleted from the disk
timeframes = [
    '2019-09'
]

for timeframe in timeframes:
    connection = sqlite3.connect(get_db_path(timeframe))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while cur_length == limit:
        df = pd.read_sql(
            """SELECT * FROM parent_reply 
                WHERE unix > {}
                and parent NOT NULL
                and score > 0
                ORDER BY unix ASC LIMIT {}""".format(last_unix, limit),
            connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)

        if not test_done:
            with open(get_test_path('from'), 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content + '\n')

            with open(get_test_path('to'), 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content) + '\n')

            test_done = True

        else:
            with open(get_train_path('from'), 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content + '\n')

            with open(get_train_path('to'), 'a', encoding='utf8') as f:
                for content in df['comment'].values:
                    f.write(str(content) + '\n')

        counter += 1
        if counter % 10 == 0:
            print(counter * limit, 'rows completed so far')

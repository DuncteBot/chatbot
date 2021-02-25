import sqlite3
import pandas as pd
import os
from helpers import get_db_path, get_test_path, get_train_path, get_timeframes
from traceback import print_tb

# array has to be static and cannot be loaded with get_timeframes
# because the large files will be deleted from the disk
# timeframes = [
#     '2019-09'
# ]
timeframes = get_timeframes()


def select_total_rows(dbConnection):
    cursor = dbConnection.cursor()
    cursor.execute(
        "SELECT count(unix) FROM parent_reply"
    )
    row = cursor.fetchone()

    return row[0]


def find_start_unix(dbConnection):
    if not os.path.exists(get_test_path('from')):
        return 0

    with open(get_train_path('from'), 'r', encoding='utf8') as fromTrainFile:
        fileLines = fromTrainFile.readlines()
        commentText = fileLines[-1].strip()

        print("Fetching unix for", commentText)

        # from === parent
        cursor = dbConnection.cursor()
        cursor.execute(
            # the limit is extra important here because our database is huge as fuck
            "SELECT unix FROM parent_reply WHERE parent = ? LIMIT 1",
            (commentText,)
        )
        row = cursor.fetchone()
        print("Unix found", row[0])

        return row[0]


for timeframe in timeframes:
    print('Timeframe', timeframe)

    if not os.path.exists(get_db_path(timeframe)):
        print('Database missing, skipping')
        continue

    with sqlite3.connect(get_db_path(timeframe)) as connection:
        try:
            total_rows = select_total_rows(connection)

            print('Total rows: ', total_rows)

            # limit = 5000
            limit = 1000000
            last_unix = find_start_unix(connection)
            cur_length = limit
            counter = 0
            test_done = last_unix > 0  # if we have a unix it means that we already have a test

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
                        for content in df['parent'].values[:5000]:
                            f.write(content + '\n')

                    with open(get_test_path('to'), 'a', encoding='utf8') as f:
                        for content in df['comment'].values[:5000]:
                            f.write(str(content) + '\n')

                    # append the rest to the train files
                    with open(get_train_path('from'), 'a', encoding='utf8') as f:
                        for content in df['parent'].values[5001:]:
                            f.write(content + '\n')

                    with open(get_train_path('to'), 'a', encoding='utf8') as f:
                        for content in df['comment'].values[5001:]:
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
            print(counter * limit, '/', total_rows, 'rows completed so far')
        except Exception as e:
            print('Writing sql broke')
            print_tb(e)

print('Done')

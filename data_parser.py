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
import sqlite3
import json
from datetime import datetime

from config import filename_regex, data_path
from helpers import create_table_if_not_exists, get_db_path, get_dataset_path, get_timeframe_path, format_data, \
    acceptable

timeframes = []
sql_transaction = []
# start_row = 0
start_row = 62100000  # that is where I stopped it last time
cleanup = 1000000

for f in os.listdir(data_path):
    if os.path.isfile(get_dataset_path(f)):
        print(f)
        match = filename_regex.match(f)
        if match is not None:
            timeframe = match.group(1)
            timeframes.append(timeframe)

print(timeframes)


def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            res = result[0]
            if res == 'False':
                return False
            return result[0]
        else:
            return False
    except Exception as e:
        print('find_parent', e)
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return result[0]
        else:
            return False
    except Exception as e:
        print('find_existing_score', e)
        return False


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 2000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except Exception as e:
                print(str(datetime.now()), s, e)
            # except:
            #     pass
        connection.commit()
        sql_transaction = []


def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = f"""UPDATE parent_reply
        SET parent_id = ?,
        comment_id = ?,
        parent = ?,
        comment = ?,
        subreddit = ?, 
        unix = ?,
        score = ? 
        WHERE parent_id = ?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)

        transaction_bldr(sql)
    except Exception as e:
        print('sql_insert_replace_comment', e)


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score)
        VALUES ("{}","{}","{}","{}","{}",{},{});
        """.format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('sql_insert_has_parent', e)


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score)
        VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('sql_insert_no_parent', e)


for timeframe in timeframes:
    with sqlite3.connect(get_db_path(timeframe)) as connection:
        c = connection.cursor()
        create_table_if_not_exists(c)

        row_counter = 0
        paired_rows = 0

        with open(get_timeframe_path(timeframe), buffering=1000) as f:
            for row in f:
                row_counter += 1

                if row_counter > start_row:
                    try:
                        row = json.loads(row)
                        parent_id = row['parent_id'].split('_')[1]
                        body = format_data(row['body'])
                        created_utc = row['created_utc']
                        score = row['score']

                        comment_id = row['id']

                        subreddit = row['subreddit']
                        parent_data = find_parent(parent_id)

                        existing_comment_score = find_existing_score(parent_id)
                        if existing_comment_score:
                            if score > existing_comment_score:
                                if acceptable(body):
                                    sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit,
                                                               created_utc, score)

                        else:
                            if acceptable(body):
                                if parent_data:
                                    if score >= 2:
                                        sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit,
                                                              created_utc, score)
                                        paired_rows += 1
                                else:
                                    sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                    except Exception as e:
                        print(e)

                if row_counter % 100000 == 0:
                    print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows,
                                                                                  str(datetime.now())))

               # if row_counter > start_row:
                   # if row_counter % cleanup == 0:
print("Cleanin up!")
c.execute('BEGIN TRANSACTION')
# Firsly remove the null values
sql = "DELETE FROM parent_reply WHERE parent IS NULL"
c.execute(sql)
# After that we delete the values that are 'False' (idk how those got there)
sql = "DELETE FROM parent_reply WHERE parent == 'False'"
c.execute(sql)
connection.commit()
c.execute("VACUUM")
connection.commit()

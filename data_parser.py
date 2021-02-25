import sqlite3
import json
from datetime import datetime
from traceback import print_tb

from helpers import create_table_if_not_exists, get_db_path, get_timeframe_path, format_data, \
    acceptable, get_timeframes

timeframes = get_timeframes()
sql_transaction = []
start_row = 0
# start_row = 137500000  # that is where I stopped it last time

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
        print_tb(e)
        return False


def find_existing_score(pid):
    if pid is False:
        return False

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
        print_tb(e)
        return False


def transaction_bldr(sql, bindings = None):
    global sql_transaction
    sql_transaction.append([sql, bindings])
    if len(sql_transaction) > 2000:
        c.execute('BEGIN TRANSACTION')
        for s, b in sql_transaction:
            try:
                if b is not None:
                    c.execute(s, b)
                else:
                    c.execute(s)
            # except Exception as e:
            #     print(str(datetime.now()), s, e)
            except:
                pass
        connection.commit()
        sql_transaction = []


def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """UPDATE parent_reply
        SET parent_id = ?,
        comment_id = ?,
        parent = ?,
        comment = ?,
        subreddit = ?, 
        unix = ?,
        score = ? 
        WHERE parent_id = ?;"""
        
        b = [parentid, commentid, parent, comment, subreddit, int(time), score, parentid]

        transaction_bldr(sql, b)
    except Exception as e:
        print('sql_insert_replace_comment', e)
        print_tb(e)


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        
        b = [parentid, commentid, parent, comment, subreddit, int(time), score]
        
        transaction_bldr(sql, b)
    except Exception as e:
        print('sql_insert_has_parent', e)
        print_tb(e)


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score)
        VALUES (?, ?, ?, ?, ?, ?);"""
        
        b = [parentid, commentid, comment, subreddit, int(time), score]
        
        transaction_bldr(sql, b)
    except Exception as e:
        print('sql_insert_no_parent', e)
        print_tb(e)


for timeframe in timeframes:
    with sqlite3.connect(get_db_path(timeframe)) as connection:
        c = connection.cursor()
        create_table_if_not_exists(c)

        row_counter = 0
        paired_rows = 0

        with open(get_timeframe_path(timeframe), buffering=1000) as f:
            # with open(get_timeframe_path(timeframe)) as f:
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

    # start from 0
    start_row = 0

print('Done')

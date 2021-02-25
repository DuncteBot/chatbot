import sqlite3

from config import filename_regex, data_path
from helpers import get_db_path, get_dataset_path, get_timeframes

timeframes = get_timeframes()

print(timeframes)

for timeframe in timeframes:
    with sqlite3.connect(get_db_path(timeframe)) as connection:
        c = connection.cursor()

        print("Cleanin up!")
        c.execute('BEGIN TRANSACTION')
        # Remove values that we don't want
        sql = "DELETE FROM parent_reply WHERE parent IS NULL OR parent == 'False' OR parent == '0'"
        c.execute(sql)
        connection.commit()
        c.execute("VACUUM")
        connection.commit()

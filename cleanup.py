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
        # Firsly remove the null values
        sql = "DELETE FROM parent_reply WHERE parent IS NULL"
        c.execute(sql)
        # After that we delete the values that are 'False' (idk how those got there)
        sql = "DELETE FROM parent_reply WHERE parent == 'False'"
        c.execute(sql)
        connection.commit()
        c.execute("VACUUM")
        connection.commit()

""" interfaces with sqlite3 database """
import sqlite3
import os
import datetime
from nemweb import CONFIG



class DBHandler:
    def __init__(self, db_name = "nemweb_live.db"):
        db_path = os.path.join(CONFIG['local_settings']['sqlite_dir'], db_name)
        self.conn = sqlite3.connect(db_path)

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            sqlite3.Connection.close()

    def insert(self, dataframe, table_name):
        """
        Inserts dataframe into a table (table name) in an sqlite3 database (db_name).
        Database directory needs to be specfied in config.ini file
        """
        assert self.check_table_existence(table_name), f"Table {table_name} doesn't exist in db!"
        try:
            dataframe.to_sql(table_name, con=self.conn, if_exists='append', index=None)
            self.conn.commit()
        except sqlite3.IntegrityError as error:
            msg = error.args[0].split(":")
            if msg[0] != 'UNIQUE constraint failed':
                raise error


    def get_table_latest_record(self,table_name, timestamp_col="SETTLEMENTDATE"):
        """
        Returns the lastest timestamp from a table in an sqlite3 database
        as a datetime object.

        Timestamp fields in nemweb files usually named "SETTLEMENTDATE".
        Sometimes INTERVAL_DATETIME is used.
        """
        assert self.check_table_existence(), f"Table {table_name} doesn't exist in db!"
        result = self.conn.execute(
            "SELECT MAX({0}) FROM {1};".format(timestamp_col, table_name)
        )
        date_str = result.fetchall()[0][0]
        result.close()
        return datetime.datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')


    def check_table_existence(self,table_name):
        """
        Checks to see whether the table given exists in the current database
        """
        exists = True
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT * FROM {0} LIMIT 1;".format(table_name))
        except sqlite3.OperationalError as error:
            msg = error.args[0].split(":")
            if msg[0] == 'no such table':
                exists = False
        cursor.close()
        return exists


    def create_table(self,table_name,keycols,colnames):
        """
        Create a table with specification defined in colnames (Dict).
        Uses keycolnames (List type) to create a unique index in table to prevent duplicate entries.
        """
        querystring = 'CREATE TABLE {0} ({1}, CONSTRAINT uniqueness UNIQUE ({2}));'.format(
            table_name,
            ', '.join([f'{k.upper()} {v.upper()}' for k,v in colnames.items()]),
            ', '.join([s.upper() for s in keycols]))

        cursor = self.conn.cursor()
        cursor.execute(querystring)
        self.conn.commit()
        cursor.close()

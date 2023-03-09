import cx_Oracle
import pandas as pd


class Oracle:
    def __init__(self, *args, **kwargs):
        if 'user' in kwargs:
            self.user = kwargs['user']
        else:
            print(
                'ORACLE: missing user argument, using NETCAD')
            self.user = 'NETCAD'
        if 'password' in kwargs:
            self.pword = kwargs['password']
        else:
            print(
                'ORACLE: missing password argument, using NETCAD')
            self.pword = 'NETCAD'
        if 'TNS' in kwargs:
            self.dsn = kwargs['TNS']
        else:
            raise(
                'ORACLE: no TNS is provided, can\'t connect to DB. Exiting...')

    def select(self, columns="*", **kwargs):
        """
        :param columns: ARRAY - list of columns, default is *
        :param kwargs: STR - table and options. Both have to be called. Options can be empty string.
        :return:
        """
        db = cx_Oracle.connect(user=self.user, password=self.pword, dsn=self.dsn)
        cursor = db.cursor()
        if columns != '*':
            columns = ','.join(map(str, columns))

        if "table" not in kwargs:
            print('ORACLE: No table is given for SELECT')
            cursor.close()
            return

        options = ""
        if "options" in kwargs:
            options = f"WHERE {kwargs['options']}"

        try:
            print('Running SQL:', f"SELECT {columns} FROM {kwargs['table']} {options}")
            cursor.execute(f"SELECT {columns} FROM {kwargs['table']} {options}")
            data = list(cursor.fetchall())
            c =0
            for i in data:
                row = list(i)
                row[0] = row[0].read()
                data[c] = row
                c+=1
            return {
                "desc": [row[0] for row in cursor.description],
                "data": data
            }
        except cx_Oracle.Error as e:
            print('ORACLE veritabanında okuma işlemi sırasında hata.. ==> ', e)
            db.commit()
        db.commit()
        cursor.close()

    def insert(self, list_of_dicts, **kwargs):
        """
        :param list_of_dicts: ARRAY - data = [{"column1":"row1-item1", "column2":"row1-item2"},
                                        {"column1":"row2-item1", "column2":"row2-item2"}]
        :param kwargs: STR - table to insert
        :return:
        """
        if "table" not in kwargs:
            print('No table is given for INSERT')
            return

        all_keys = list(set().union(*(d.keys() for d in list_of_dicts)))
        all_vals = list(set().union(*(d.values() for d in list_of_dicts)))

        db = cx_Oracle.connect(user= self.user, password=self.pword, dsn=self.dsn)
        cursor = db.cursor()

        try:
            cursor.executemany("INSERT INTO " + kwargs['table'] + " (" + ",".join(all_keys) + ") " +
                               "VALUES(" + ",".join(["%s"] * len(all_keys)) + ")",
                               [tuple(d.get(k, "NULL") for k in all_keys) for d in list_of_dicts])
        except cx_Oracle.Error as e:
            print('ORACLE veritabanında insert işlemi sırasında hata.. ==> ', e)
            db.commit()

        db.commit()
        cursor.close()

    def insert2(self, table, columns, data):
        """
        :param data: ARRAY - [{"column1":"row1-item1", "column2":"row1-item2"},
                                        {"column1":"row2-item1", "column2":"row2-item2"}]
        :param columns: ARRAY - column list
        :param table: STR - table name
        :return:
        """

        db = cx_Oracle.connect(user= self.user, password=self.pword, dsn=self.dsn)
        cursor = db.cursor()

        try:
            cursor.executemany(f"INSERT INTO {table} ({','.join(map(str, columns))}) "
                               f"VALUES({', '.join(list(map(lambda x: ':'+str(columns.index(x)+1), columns)))})", data)
        except cx_Oracle.Error as e:
            print('ORACLE veritabanında çoklu insert işlemi sırasında hata.. ==> ', e)
            db.commit()

        db.commit()
        cursor.close()

    def execute(self, sql):
        """
        :param sql: STR - Direct execution for given SQL
        :return:
        """

        db = cx_Oracle.connect(user= self.user, password=self.pword, dsn=self.dsn)
        cursor = db.cursor()

        try:
            for i in sql.replace('\n', '').split(';'):
                cursor.execute(i)
        except cx_Oracle.Error as e:
            print('ORACLE veritabanında SQL çalıştırma sırasında hata ==> ', e)

        db.commit()
        cursor.close()

    def update(self, table, column, value, column_id):
        """
        :param table:
        :param column_id:
        :param value:
        :param column:
        :return:
        """

        db = cx_Oracle.connect(user=self.user, password=self.pword, dsn=self.dsn)
        cursor = db.cursor()

        try:
            cursor.executemany(f"UPDATE {table} SET {column} = {value} WHERE OBJECTID = :1", id)
        except cx_Oracle.Error as e:
            print('ORACLE veritabanında çoklu update işlemi sırasında hata.. ==> ', e)
            db.commit()

        db.commit()
        cursor.close()

    def dataframe(self, table, schema=''):
        """
        Returns dataframe of the selected table
        :param schema: Optional - Name of the schema
        :param table: Name of the table
        :return:
        """
        db = cx_Oracle.connect(user=self.user, password=self.pword, dsn=self.dsn)
        if schema == '':
            return pd.read_sql(f"SELECT * FROM {table}", con=db)
        else:
            return pd.read_sql(f"SELECT * FROM {schema}.{table}", con=db)

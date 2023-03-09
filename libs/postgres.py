from datetime import datetime

import psycopg2


class Postgres:
    def __init__(self, *args, **kwargs):
        if "host" in kwargs:
            self.host = kwargs["host"]
        elif "ip" in kwargs:
            self.host = kwargs["ip"]
        else:
            print("Postgres: missing host argument, using localhost")
            self.host = "localhost"
        if "dbname" in kwargs:
            self.db_postgres = kwargs["dbname"]
        elif "db" in kwargs:
            self.db_postgres = kwargs["db"]
        else:
            print("Postgres: missing database argument, using postgres")
            self.db_postgres = "postgres"
        if "user" in kwargs:
            self.user_postgres = kwargs["user"]
        else:
            print("Postgres: missing user argument, using postuser")
            self.user_postgres = "postgres"
        if "pword" in kwargs:
            self.pass_postgres = kwargs["pword"]
        elif "pass" in kwargs:
            self.pass_postgres = kwargs["pass"]
        else:
            print("Postgres: missing password argument, using ntc123*")
            self.pass_postgres = "ntc123*"
        if "port" in kwargs:
            self.port = kwargs["port"]
        else:
            print("Postgres: missing password argument, using 5432")
            self.port = "5432"
        if "logtable" in kwargs:
            self.logTable = kwargs["logtable"]
        else:
            self.logTable = "log"
        self.logCheck = False

    def create(self, table, columns):
        """
        :param table: STR - Name of the table
        :param columns: OBJECT - {'col_name': 'col_type'}
        :return:
        """
        poly = ""
        if "poly" in columns:
            poly = f",poly {columns['poly']}"
        columns = ""
        for key, value in columns.items():
            if key == "poly":
                continue
            columns += f",{key} {value}"

        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()

        TABLE_SQL = (
            "CREATE TABLE " + table + " ("
            "  objectid SERIAL NOT NULL PRIMARY KEY"
            f"{poly} {columns} )"
        )

        try:
            cursor.execute(TABLE_SQL)
        except psycopg2.ProgrammingError as e:
            print(
                "Postgres veritabanında yeni tablo oluşturma sırasında hata.. ==> ", e
            )
            db.commit()

        db.commit()
        cursor.close()

    def select(self, columns="*", **kwargs):
        """
        :param columns: ARRAY - list of columns, default is *
        :param kwargs: STR - table and options. Both have to be called. Options can be empty string.
        :return:
        """
        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()
        if columns != "*":
            columns = ",".join(map(str, columns)).lower()

        if "table" not in kwargs:
            print("Postgres: No table is given for SELECT")
            cursor.close()
            return
        options = ""
        if "options" in kwargs:
            options = f"WHERE {kwargs['options']}"

        try:
            cursor.execute(f"SELECT {columns} FROM {kwargs['table']} {options}")
            return {
                "desc": [row[0] for row in cursor.description],
                "data": cursor.fetchall(),
            }
        except psycopg2.ProgrammingError as e:
            print("Postgres veritabanında okuma işlemi sırasında hata.. ==> ", e)
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
            print("No table is given for INSERT")
            return

        all_keys = list(set().union(*(d.keys() for d in list_of_dicts)))
        all_vals = list(set().union(*(d.values() for d in list_of_dicts)))

        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()

        try:
            cursor.executemany(
                "INSERT INTO "
                + kwargs["table"]
                + " ("
                + ",".join(all_keys)
                + ") "
                + "VALUES("
                + ",".join(["%s"] * len(all_keys))
                + ")",
                [tuple(d.get(k, "NULL") for k in all_keys) for d in list_of_dicts],
            )
        except psycopg2.ProgrammingError as e:
            print("Postgres veritabanında insert işlemi sırasında hata.. ==> ", e)
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

        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()

        try:
            cursor.executemany(
                f"INSERT INTO {table} ("
                + '"'
                + '","'.join(map(str, columns))
                + '")'
                + f"VALUES({', '.join(list(map(lambda x: ' %s', columns)))})",
                data,
            )
        except psycopg2.Error as e:
            print("POSTGRES veritabanında çoklu insert işlemi sırasında hata.. ==> ", e)
            db.commit()

        db.commit()
        cursor.close()

    def execute(self, sql):
        """
        :param sql: STR - Direct execution for given SQL
        :return:
        """
        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()

        try:
            cursor.execute(sql)
            if ("update", "insert", "delete", "truncate", "alter") in sql.lower():
                return {"desc": None, "data": None}
            elif "select" in sql.lower():
                return {
                    "desc": [row[0] for row in cursor.description],
                    "data": cursor.fetchall(),
                }
        except psycopg2.ProgrammingError as e:
            print("Postgres veritabanında SQL çalıştırma sırasında hata ==> ", e)

        db.commit()
        cursor.close()

    def execMultiline(self, sql):
        """
        :param sql: STR - Direct execution for given SQL
        :return:
        """
        db = psycopg2.connect(
            f"dbname='{self.db_postgres}' user='{self.user_postgres}' host='{self.host}' password='{self.pass_postgres}' port='{self.port}'"
        )
        cursor = db.cursor()

        for i in sql.replace("\n", "").strip().split(";"):
            if i == "":
                continue
            try:
                cursor.execute(sql)
            except psycopg2.ProgrammingError as e:
                print("Postgres veritabanında SQL çalıştırma sırasında hata ==> ", e)
        db.commit()
        cursor.close()

    def log(self, event, desc):
        table = self.logTable
        if self.logCheck is False:
            log = self.execute(
                f"select * from information_schema.tables where table_name='{table}'"
            )["data"]
            if len(log) == 0:
                self.create(
                    table,
                    {
                        "event": "varchar(255)",
                        "description": "text",
                        "date": "timestamp",
                    },
                )
            self.logCheck = True
        self.insert2(
            table, ["event", "description", "date"], [(event, desc, datetime.now())]
        )

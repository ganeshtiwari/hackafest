import sqlite3


class DB:
    def __init__(self, dbname):
        self.connection = sqlite3.connect(dbname)

    def execute(self, statement):
        try:
            cursor = self.connection.cursor()
            output = cursor.execute(statement)
            self.connection.commit()
        except Exception as e:
            raise Exception(e)
        else:
            return output

    def close_connection(self):
        self.connection.close()





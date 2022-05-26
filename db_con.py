import mysql.connector as sql
from mysql.connector import Error


class Database:
    def __init__(self):
        self.host_name = None
        self.user_name = None
        self.pass_word = None

    def connect(self, host_name, user_name, pass_word):
        self.host_name = str(host_name)
        self.user_name = str(user_name)
        self.pass_word = str(pass_word)
        try:
            conn = sql.connect(host=self.host_name, user=self.user_name, password=self.pass_word)
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("USE DATABASE ")
                return cursor
        except Error as e:
            print("Error in connecting to Database")
            print(e)

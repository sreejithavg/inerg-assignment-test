import sqlite3
class SqliteDB:
    def __init__(self):
        self.conn =  sqlite3.connect("main_db")
        self.conn.row_factory = sqlite3.Row 
        self.cursor = self.conn.cursor()
        self.create_table("AnualProduction")

    def create_table(self,table_name):
        # 3. Create a table for users
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}
                               (ID INTEGER PRIMARY KEY,
                                OIL INTEGER NOT NULL,
                                GAS INTEGER NOT NULL ,
                                BRINE INTEGER NOT NULL
                            )''')
        # Commit the changes
        self.conn.commit()
    
    def insert_users_bulk(self, insert_query_data,anual_production):
        # Insert multiple users at once
        self.cursor.executemany(insert_query_data, anual_production)
        self.conn.commit()
        if self.cursor.rowcount > 0:
            return True,f"Successfully inserted {self.cursor.rowcount} row(s)."
        else:
            return False,"Insert failed. No rows affected."
 
    def drop_table(self, table_name):
        try:
            # Drop the specified table if it exists
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            return f"Table '{table_name}' deleted successfully."
        except Exception as e:
            return f"An error occurred: {str(e)}"
    def close(self):
        # Close the connection
        self.conn.close()


    def fetchOneData(self,query_string,data_variable):
        self.cursor.execute(query_string,data_variable)
        row = self.cursor.fetchone()
        self.conn.close()
        return row
    
import queue
import sqlite3  
import datetime
import MySQLdb


class ConnectionPool:
    def __init__(self, **kwargs):
        self.size = kwargs.get('size', 10)
        self.kwargs = kwargs
        self.conn_queue = queue.Queue(maxsize=self.size)
        sqlite3.register_adapter(datetime, self.adapt_datetime)  
        sqlite3.register_converter("DATETIME", self.convert_datetime)  
        for i in range(self.size):
            self.conn_queue.put(self._create_new_conn())

    # 创建一个新的日期时间适配器  
    def adapt_datetime(dt):  
        return dt.isoformat() if dt is not None else None  
    
    # 创建一个新的日期时间转换器  
    def convert_datetime(text):  
        return datetime.fromisoformat(text) if text is not None else None  
    
    def _create_new_conn(self):
        return sqlite3.connect("csr_database.db")

    def _put_conn(self, conn):
        self.conn_queue.put(conn)

    def _get_conn(self):
        conn = self.conn_queue.get()
        if conn is None:
            self._create_new_conn()
        return conn

    def exec_sql(self, sql):
        conn = self._get_conn()
        try:
            with conn as cur:
                cur.execute(sql)
                return cur.fetchall()
        except Exception as e:
            # 可以加一行将异常记录到日志中
            raise e
        finally:
            self._put_conn(conn)

    def __del__(self):
        try:
            while True:
                conn = self.conn_queue.get_nowait()
                if conn:
                    conn.close()
        except queue.Empty:
            pass

    def create_table(self,sql="",table_name=""):
        try:
            check_table_query = f''' SELECT name FROM sqlite_master WHERE type='table' AND name=?;'''
            # 要创建的表的名称  
            table_name = table_name 
            conn =self._get_conn()
            cur = conn.cursor()
            
            if cur:
                cur.execute(check_table_query, (table_name,))  
                table_exists = cur.fetchone() is not None
                    # 如果表不存在，则创建它  
                if not table_exists:  
                    cur.execute(sql)  
                    # 提交事务  
                    conn.commit()
        except Exception as e:
            raise e

if __name__ == "__main__":
    connpool = ConnectionPool(size=10)
    table_name= 'csr_file'
    sql = f'''  
                        CREATE TABLE {table_name} (  
                            date TEXT NOT NULL,  
                            file_name TEXT NOT NULL,  
                            dir_name TEXT NOT NULL,  
                            csr_dir TEXT NOT NULL,  
                            csr BLOB NOT NULL,
                            status TEXT NOT NULL,
                            PRIMARY KEY (file_name)  
                        );  
                    '''
    connpool.create_table(sql=sql,table_name=table_name)
    table_name= 'csr_error_file'
    sql = f'''  
                        CREATE TABLE {table_name} (  
                            date TEXT NOT NULL,  
                            file_name TEXT NOT NULL,  
                            dir_name TEXT NOT NULL,  
                            csr_dir TEXT NOT NULL,  
                            csr BLOB NOT NULL,
                            status TEXT NOT NULL,
                            PRIMARY KEY (file_name)  
                        );  
                    '''
    connpool.create_table(sql=sql,table_name=table_name)

    table_name= 'csr_dir'
    sql = f'''  
                        CREATE TABLE {table_name} (  
                            csr_dir TEXT NOT NULL,
                            path TEXT NOT NULL,  
                            files TEXT NOT NULL,
                            count TEXT NOT NULL,  
                            uperror TEXT NOT NULL,
                            date TEXT NOT NULL,
                            PRIMARY KEY (csr_dir)  
                        );  
                    '''
    connpool.create_table(sql=sql,table_name=table_name)

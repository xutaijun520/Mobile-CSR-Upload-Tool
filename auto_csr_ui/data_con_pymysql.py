import queue
import sqlite3  
import datetime
import pymysql


class ConnectionPool:
    def __init__(self, **kwargs):
        self.size = kwargs.get('size', 10)
        self.kwargs = kwargs
        self.conn_queue = queue.Queue(maxsize=self.size)
        sqlite3.register_adapter(datetime, self.adapt_datetime)  
        sqlite3.register_converter("DATETIME", self.convert_datetime)  
        self.database='csr_database'
        for i in range(self.size):
            self.conn_queue.put(self._create_new_conn())

    # 创建一个新的日期时间适配器  
    def adapt_datetime(dt):  
        return dt.isoformat() if dt is not None else None  
    
    # 创建一个新的日期时间转换器  
    def convert_datetime(text):  
        return datetime.fromisoformat(text) if text is not None else None  
    
    def _create_new_conn(self):
        cnx = pymysql.connect(  
        host='192.168.19.138',  # 例如: "localhost"  
        user='csr_user',  # 你的MySQL用户名  
        password='Office.1',  # 你的MySQL密码  
        db='csr_database',  # 你要连接的数据库名  
        charset='utf8mb4',  # 字符集设置为utf8mb4，以支持emoji等字符  
        #cursorclass=pymysql.cursors.DictCursor  # 返回字典类型的结果  
         )
        return cnx

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
            check_table_query = f'''  SELECT table_name   
                FROM information_schema.tables   
                WHERE table_schema = %s AND table_name = %s;  '''
            # 要创建的表的名称  
            table_name = table_name 
            conn =self._get_conn()
            cur = conn.cursor()
            
            if cur:
                cur.execute(check_table_query, (self.database, table_name))  
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
    sql = f'''CREATE TABLE `{table_name}` (  
    `date` VARCHAR(255) NOT NULL,  
    `file_name` VARCHAR(255) NOT NULL,  -- 假设文件名不会太长  
    `dir_name` VARCHAR(255) NOT NULL,  
    `csr_dir` VARCHAR(255) NOT NULL,  
    `csr` BLOB NOT NULL,  
    `status` VARCHAR(255) NOT NULL,  
    PRIMARY KEY (`file_name`)  -- 如果文件名不会太长且唯一，可以作为主键  
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    '''
    connpool.create_table(sql=sql,table_name=table_name)


    table_name= 'csr_dir'
    sql = f'''  CREATE TABLE `{table_name}` (  
    `csr_dir` VARCHAR(255) NOT NULL,  
    `path` VARCHAR(255) NOT NULL,  
    `files` VARCHAR(255) NOT NULL,  
    `count` INT NOT NULL,  -- 假设count是整数类型  
    `uperror` VARCHAR(255) NOT NULL,  
    `date` VARCHAR(255) NOT NULL,  -- 假设date是日期类型  
    PRIMARY KEY (`csr_dir`)  -- 保留csr_dir作为主键，但请注意性能问题  
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    connpool.create_table(sql=sql,table_name=table_name)

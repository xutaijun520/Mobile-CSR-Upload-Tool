import subprocess
import datetime
import logging
import schedule
import os
import shutil
import concurrent.futures
import time
from data_con_pymysql import ConnectionPool
from threading import Lock
#import sqlite3

class AutoCsr:
    def __init__(self):
        self.threaddic={}
        self.flag =False
        self.all_files = []
        #self.connectpool = ConnectionPool(size=30)
        #需要上传的目录
        self.src_dir=""
        #同时上传的粒度
        self.chunk_size=1
        self.all_dir=None
        self.lock = Lock()
        self.init_logging()

    def init_logging(self):
        # 配置日志记录器
        today = datetime.date.today()
        log_filename = f'log_{today.strftime("%Y-%m-%d")}.txt'
        logging.basicConfig(filename=log_filename, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

    def re_date(time=""):
        return datetime.datetime.fromtimestamp(time)

    def dir_mtime(self,abs_path=None):
        return os.stat(abs_path).st_mtime

    #获取目标目录的文件夹
    def get_directories(self,path):  
        return [[name,self.dir_mtime(os.path.join(path, name)),os.path.join(path, name)] for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))] 

    #获取需要上传的文件
    def get_all_files_in_directory(self,directory=None):
        if os.path.exists(directory):
            files_dic = {}
            count = 0
            files = []
            for dirpath, dirnames, filenames in os.walk(directory):
                for i in filenames:
                    count+=1
                    files.append(os.path.join(dirpath,i))
            else:
                files_dic[os.path.basename(directory)]=directory
                files_dic['files']=files
                files_dic['count']=count
                files_dic['uperror'] = False
                return files_dic
        else:
            return {}

    #将传输完成的文件夹记录在数据库
    def save_up_done(self,waite_up_direct=None):
        tablename='csr_dir'
        if waite_up_direct:
            self.connectpool = ConnectionPool(size=1)
            conn = self.connectpool._get_conn()
            cursor = conn.cursor()
            sql = f"INSERT INTO {tablename} (csrdir,files,count,status,uperror)  VALUES (%s, %s,%s, %s,%s)"
            #date_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(sql, (*[v for v in  waite_up_direct.values()],))
            conn.commit()
        pass

    #将文件存入数据库,需要提前检查是否已经在数据库中，存在上传成功只更新状态，不存在
    def save_to_data(self,tablename="",batch=None,o_batch=None,status=None):
        lock = Lock()
        lock.acquire()
        try:
            if batch:
                for file in batch:
                    if os.path.exists(file):

                        self.connectpool = ConnectionPool(size=1)
                        conn = self.connectpool._get_conn()
                        cursor = conn.cursor()
                        temp_filename = os.path.basename(file)
                        temp_dirname = os.path.basename(os.path.dirname(file))
                        if batch:
                            if file in o_batch:
                                sql = f'UPDATE {tablename} SET status=%s WHERE file_name=%s'
                                cursor.execute(sql,(status,temp_filename))
                                conn.commit()
                            else:
                                with open(file=file,mode='rb') as f:
                                    #判断文件是否已经存在数据库，如果存在需要判定状态是否为0为0的执行上传，成功后更新状态。
                                    filedata = f.read()
                                    sql = f"INSERT INTO {tablename} (date, file_name,dir_name, csr_dir, csr,status)  VALUES (%s,%s, %s, %s, %s,%s)"
                                    date_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    cursor.execute(sql, (date_now,temp_filename,temp_dirname,file,filedata,status))
                                    conn.commit()

                    else:
                        print("{}不存在".format(file))
            else:
                logging.error('save_to_date function error')
        except Exception as e:
            logging.error(e)
        finally:
            lock.release()
            cursor.close()
            conn.close()

    def  check_insys(self,batch=None):
        lock = Lock()
        lock.acquire()
        tablename='csr_file'
        batch = batch
        nowbatch = []
        try:
            self.connectpool = ConnectionPool(size=1)
            conn = self.connectpool._get_conn()
            cursor = conn.cursor()
            for file in batch:

                sql = f'SELECT csr_dir,status from {tablename} WHERE dir_name=%s and file_name=%s'
                temp_filename = os.path.basename(file)
                temp_dirname = os.path.basename(os.path.dirname(file))
                cursor.execute(sql,(temp_dirname,temp_filename))
                result = cursor.fetchone()
                if result:
                    nowbatch.append(result)
        except Exception as e:
            logging.error(e)
        finally:
            lock.release()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return nowbatch


    #分文件夹上传
    def upload_csr(self,batch=None,o_batch=None):
        args1 = ['--json-csr']
        batch=batch
        # 调用.exe文件并捕获输出
        try:
            #不在数据库中或状态为0的才需要执行上传
            # all_insys =self.check_insys(batch)
            # o_batch=[]

            # if all_insys:
            #     o_batch=[i[0] for i in all_insys if not int(i[1]) ]
            #     print(o_batch)
            #     time.sleep(3000)
            #     all_batch=[i[0] for i in all_insys]
            #     batch=[x for x in batch if x not in all_batch]+o_batch
            if batch:
                
                result = subprocess.run(["cli.exe"] + args1 + batch, capture_output=True, text=True)
                if result.returncode == 0:
                    # print("命令执行成功")
                    #self.move_files_with_structure(src_dir, dst_dir, files=batch)
                    self.save_to_data(tablename='csr_file',batch=batch,o_batch=o_batch,status="1")
                    logging.info("成功")
                else:
                    # print(f"命令执行失败，返回码：{result.returncode}")
                    print(f"{result.stdout}")
                    self.save_to_data(tablename='csr_file',batch=batch,o_batch=o_batch,status="0")
                    logging.error(batch)
        except Exception as e:
            logging.error(e)

    #查询数据库文件夹是否存在,存在返回结果
    def select_dir(self,dirs=None):
        if dirs is None or not isinstance(dirs, list):  
            print("请输入一个文件夹列表")  
            return []  
        tablename='csr_dir'
        self.connectpool = ConnectionPool(size=1)
        placeholders = ','.join(['%s'] * len(dirs))
        sql = f"SELECT * FROM {tablename} as A WHERE csr_dir IN ({placeholders})"
        conn = self.connectpool._get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, dirs)
        result = cursor.fetchall()
        if result:
            cursor.close()
            conn.close()
            return result
        else:
            cursor.close()
            conn.close()
            return []
        
    def jisuan_all_files(self,all_files=None):
        all_insys =self.check_insys(all_files)
        if all_insys:
            o_batch=[i[0] for i in all_insys if i[1]=='0' ]
            i_batch=[i[0] for i in all_insys if i[1]=='1']
            all_batch = [i[0] for i in all_insys]
            batch=[x for x in all_files if x not in all_batch]+o_batch
            return batch,o_batch
    

    #开启上传多线程
    def up_thread(self,chunk_size=None,src=None):
        all_files=self.get_all_files_in_directory(src)['files']
        all_files,o_batch=self.jisuan_all_files(all_files=all_files) if self.jisuan_all_files(all_files=all_files) else (all_files,[])
        if all_files!=None:
        # 创建一个线程池，最大线程数为10
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as self.executor:
                print("线程池创建成功")
                self.threaddic['executor']=self.executor
                for i in range(0, len(all_files), chunk_size):
                    # 提取当前批次的元素
                    batch = all_files[i:i + chunk_size]
                    # 提交任务到线程池
                    future = self.executor.submit(self.upload_csr,batch,o_batch)

    #数量是否一致
    def check_count(self,src=None,des=None):
        dictionary = {t[0]: t[1:] for t in des}  
        try:
            tablename = "csr_file"
            self.connectpool = ConnectionPool(size=1)
            sql = f"SELECT count(dir_name) FROM {tablename} WHERE dir_name=%s and status=%s"
            conn = self.connectpool._get_conn()
            cursor = conn.cursor()
            cursor.execute(sql, (src,"1",))
            result = cursor.fetchone()
            conn.commit()
            pass
        except Exception as e:
            logging.error(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            if result:
                if dictionary[src][2] == result[0]:
                    return True
                elif int(dictionary[src][2]) < int(result[0]):
                    return True
                else:
                    return False
    #写入csr_dir
    def write_csr_dir(self,csr_dir=None,date=None,path=None):   
        self.lock.acquire()
        try:
            if os.path.exists(csr_dir):
                self.get_all_files_in_directory(csr_dir)
                return None
            all_files_in_dir = self.get_all_files_in_directory(path)
            all_files = [csr_dir,path,"{}".format(all_files_in_dir['count']),all_files_in_dir['count'],all_files_in_dir['uperror'],date]
            tablename='csr_dir'
            self.connectpool = ConnectionPool(size=1)
            sql = f"INSERT INTO {tablename} (csr_dir,path,files,count,uperror,date) VALUES (%s, %s,%s, %s,%s,%s)"
            conn = self.connectpool._get_conn()
            cursor = conn.cursor()
            cursor.execute(sql, all_files)
            conn.commit()  
        except Exception as e:
            logging.error(e)
        finally:
            if cursor:  
                cursor.close()  
            if conn:  
                conn.close()
            self.lock.release()

    #主程序
    def auto_csr_start(self,src_dir="D:\\PycharmProjects\\auto_csr\\csr",dst_dir="D:\\PycharmProjects\\auto_csr\\csr-success",chunk_size=1):
        self.src_dir = src_dir
        dst_dir = dst_dir
        self.chunk_size = chunk_size
        self.all_dir = self.get_directories(src_dir)
        all_dir_temp = [name[0] for name in self.all_dir]
        select_dir = self.select_dir(all_dir_temp)
        if self.all_dir:
            for i in  self.all_dir:
                if i[0] in [ i[0] for i in select_dir if i]:
                    #self.up_thread()
                    if not self.check_count(src=i[0],des=select_dir):
                        self.up_thread(self.chunk_size,i[2])
                else:
                    #如果不在需要将目录信息写入数据库并执行上传操作
                    #获取目录的上传文件信息
                    self.write_csr_dir(i[0],i[1],i[2])
                    self.up_thread(self.chunk_size,i[2])
                    
        else:
            print("文件夹为空") 

    #定时任务开启主程序
    def start(self,src_dir,dst_dir,chunk_size,cron_date):
        try:
            schedule.every().day.at(cron_date).do(lambda: self.auto_csr_start(src_dir=src_dir,dst_dir=dst_dir,chunk_size=chunk_size))
            while True:
                # 运行所有可以运行的任务
                schedule.run_pending()
                time.sleep(1)
                if self.flag:
                    print("定时任务关闭成功")
                    schedule.clear()  # 清除所有任务
                    break
        except Exception as e:
            print(e)

    #停止线程池、定时任务
    def auto_csr_stop(self,flag=True):
        try:
            self.flag=flag
            if self.flag:
                if self.threaddic['executor']:
                    self.threaddic['executor'].shutdown(wait=False)
                    
                    print("强制关闭线程成功")
            else:
                print("")
        except Exception as e:
            print(e)

if __name__ == "__main__":
    test = AutoCsr()
    test.auto_csr_start()
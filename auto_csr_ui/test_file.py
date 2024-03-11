
import os
import datetime
# class test:
#     def __init__(self) -> None:
#         self.src = "D:\\PycharmProjects\\auto_csr\\csr"
#         self.all_files = []
#     def get_directories(self,path):  
#         return [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))] 

#     def get_all_files_in_directory(self,directory):
#         for direct in self.get_directories(directory):
#             #与处理完成的文件夹做对比，若没有完成的继续迭代
#             files_dic = {}
#             abs_path = os.path.join(directory,direct)
#             files_dic[abs_path]=direct
#             count = 0
#             files = []
#             for dirpath, dirnames, filenames in os.walk(abs_path):
#                 for i in filenames:
#                     count+=1
#                     files.append(os.path.join(dirpath,i))
#             else:
#                 files_dic['files']=files
#                 files_dic['count']=count
#                 self.all_files.append(files_dic)
#         return self.all_files
    
# if __name__=="__main__":
#     test = test()
#     print(test.get_all_files_in_directory(test.src)[3])
test = os.stat('test')

def re_date(time=""):
    return datetime.datetime.fromtimestamp(time)

print(re_date(test.st_ctime),re_date(test.st_mtime),re_date(test.st_atime))

src = "C:\\A\B\C"
print(os.path.basename(src))

dirs = ['20231106']
from data_con import ConnectionPool
tablename='csr_file'
connectpool = ConnectionPool(size=1)

#sql = f"SELECT csr_dir FROM {tablename} as a WHERE csr_dir IN ({placeholders})"
sql = f'SELECT csr_dir from {tablename} WHERE dir_name=? and status=? and file_name=?'
conn = connectpool._get_conn()
cursor = conn.cursor()
#cursor.execute(sql, dirs)
cursor.execute(sql,('20231106','0','YJP2NB037D100063_20231106192158_CSR.json',))
result = cursor.fetchone()
print(result)
import subprocess
import datetime
import logging
import schedule
import os
import shutil
import concurrent.futures
import time

class AutoCsr:
    
    def __init__(self):
        self.threaddic={}
        self.flag=False
        # 获取当前日期并格式化
        today = datetime.date.today()
        log_filename = f'log_{today.strftime("%Y-%m-%d")}.txt'
        # 配置日志记录器
        logging.basicConfig(filename=log_filename, level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s')

    def move_files_with_structure(self,src_dir, dst_dir, files=None):
        # 创建目标文件夹（如果不存在）
        os.makedirs(dst_dir, exist_ok=True)

        # 遍历源目录的所有文件和子目录
        for root, dirs, file_list in os.walk(src_dir):
            # 如果提供了文件列表，只考虑在列表中的文件
            if files:
                files = [os.path.basename(f) for f in files]
                file_list = [f for f in file_list if f in files]

                # 构建源目录和目标目录的相对路径
            relative_path = os.path.relpath(root, src_dir)
            dst_path = os.path.join(dst_dir, relative_path)

            # 创建目标目录（如果不存在）
            os.makedirs(dst_path, exist_ok=True)

            # 移动文件到目标目录
            for file in file_list:
                src_file_path = os.path.join(root, file)
                dst_file_path = os.path.join(dst_path, file)
                shutil.move(src_file_path, dst_file_path)

    def get_all_files_in_directory(self,directory):
        all_files = []
        for dirpath, dirnames, filenames in os.walk(directory):
            for i in filenames:
                all_files.append(os.path.join(dirpath,i))
        return all_files

    def upload_csr(self,src_dir,dst_dir,batch=None):
        args1 = ['--json-csr']
        # 调用.exe文件并捕获输出
        try:
            result = subprocess.run(["cli.exe"] + args1 + batch, capture_output=True, text=True)
            if result.returncode == 0:
                # print("命令执行成功")
                self.move_files_with_structure(src_dir, dst_dir, files=batch)
                logging.info("成功")
            else:
                # print(f"命令执行失败，返回码：{result.returncode}")
                print(f"{result.stdout}")
                logging.error(batch)
        except Exception as e:
            print(e)

    def auto_csr_start(self,src_dir="D:\\PycharmProjects\\auto_csr\\csr",dst_dir="D:\\PycharmProjects\\auto_csr\\csr-success",chunk_size=1):
        src_dir = src_dir
        dst_dir = dst_dir
        chunk_size = chunk_size
        #
        print(src_dir,dst_dir,chunk_size)
        all_files = self.get_all_files_in_directory(src_dir)
        print(all_files)
        # 创建一个线程池，最大线程数为4
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as self.executor:
            print("线程池创建成功")
            self.threaddic['executor']=self.executor
            for i in range(0, len(all_files), chunk_size):
                # 提取当前批次的元素
                batch = all_files[i:i + chunk_size]
                # 提交任务到线程池
                self.executor.submit(self.upload_csr,src_dir,dst_dir, batch)

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
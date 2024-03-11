一、程序说明
安卓手机生产过程中，要求上传CSR文件，CSR文件是一段json代码。上传程序已经由客户实现，cli.exe。并提供了使用命令 cli.exe --json-csr xxx.json。
因此此程序是为了实现调用cli.exe命令执行自动上传文件，并将结果记录在数据库中。
为了信息安装项目中默认删除了cli.exe，该项目程序您也可以用于其他需要执行的程序，但需要您自己修改代码。
此程序免费使用。谢谢。
二、兼容版本
python:3.11
mysql:8.0
python-扩展包:schedule、pymysql
二、程序启动入口
1、startapp 桌面界面入口
![图片](https://github.com/xutaijun520/Mobile-CSR-Upload-Tool/assets/42400726/73ab6310-05d8-41d7-851a-dbd055b5252f)

2、show_result 基于flask的报表界面（！这可能需要您自行搭建基于WSGI的WEB服务器）
![图片](https://github.com/xutaijun520/Mobile-CSR-Upload-Tool/assets/42400726/5ee548ff-de2c-45e2-85f6-14335e82b95d)

3、auto_csr_database 可以直接运行
三、BI
参考视图SQL：
CREATE VIEW csr_up_total AS SELECT   
                dir_name,  
                COUNT(*) AS total_count,  
                SUM(CASE WHEN status = '0' THEN 1 ELSE 0 END) AS status_zero_count,
                SUM(CASE WHEN status = '1' THEN 1 ELSE 0 END) AS status_one_count 
                FROM   csr_file  
                GROUP BY   dir_name
                ORDER by dir_name desc;
用于BI报表，如果有BI工具的话可以使用。
![图片](https://github.com/xutaijun520/Mobile-CSR-Upload-Tool/assets/42400726/ed1d3159-7404-43cd-a85e-adab0e1ccadd)

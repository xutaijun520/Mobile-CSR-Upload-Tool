
from flask import Flask, render_template
from data_con_pymysql import ConnectionPool
app = Flask(__name__)  
  
@app.route('/')  
def hello_world():
        try:
                connectpool = ConnectionPool(size=1)
                sql = f"""
                SELECT   
                dir_name,  
                COUNT(*) AS total_count,  
                SUM(CASE WHEN status = '0' THEN 1 ELSE 0 END) AS status_zero_count,
                SUM(CASE WHEN status = '1' THEN 1 ELSE 0 END) AS status_one_count 
                FROM   csr_file  
                GROUP BY   dir_name
                ORDER by dir_name desc
                LIMIT 0,16;
                """
                conn = connectpool._get_conn()
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                data = {'result':result}
                conn.commit()

        except Exception as e:
                print(e)
        return render_template('index.html',data=data)
  
if __name__ == '__main__':  
    app.run(debug=True)
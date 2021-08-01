import pymysql


class MysqlConnect:
    def __init__(self, dbconfig):
        self.host = dbconfig["host"]
        self.user = dbconfig["user"]
        self.password = dbconfig["password"]
        self.db = dbconfig["database"]

    def get_data(self, sql):

        try:
            conn = pymysql.connect(host=self.host, port=3306, user=self.user, passwd=self.password,
                                   db=self.db)  # 连接数据库字符串

            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)  # 使用cursor()方法获取操作游标

            result = cursor.execute(sql)  # 执行SQL语句
            data = cursor.fetchall()  # 取出数据库所有数据

            cursor.close()
            conn.close()
            return data  # 返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host))

    def update_data(self, sql):

        try:
            conn = pymysql.connect(host=self.host, port=3306, user=self.user, passwd=self.password,
                                   db=self.db)  # 连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result = cursor.execute(sql)  # 执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host))

    def insert_data(self, sql):

        try:
            conn = pymysql.connect(host=self.host, port=3306, user=self.user, passwd=self.password,
                                   db=self.db)  # 连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute(sql)  # 执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host))


if __name__ == "__main__":
    mysql = MysqlConnect({"host": "8.129.214.224", "user": "root", "password": "13106999886@Ab", "database": "freight"})
    data = mysql.get_data("select * from freight_gd_elgd_f_201601")
    print(data[0]["freq"])

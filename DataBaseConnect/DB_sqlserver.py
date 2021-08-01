import pymssql


class MssqlConnect:
    def __init__(self, dbconfig) -> None:
        # 类的构造函数，初始化DBC连接信息
        self.server = dbconfig['host']
        self.user = dbconfig['user']
        self.password = dbconfig['password']
        self.database = dbconfig['database']

    def __enter__(self) -> 'cursor':
        # 得到数据库连接信息，返回conn.cursor()
        if not self.database:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()
        if not self.cursor:
            raise (NameError, "连接数据库失败")  # 将DBC信息赋值给cur
        else:
            return self.cursor

    def conn(self):
        return pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)

    def get_data(self, sql):

        try:
            conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
            cursor = conn.cursor(as_dict=True)  # 使用cursor()方法获取操作游标

            result = cursor.execute(sql)  # 执行SQL语句
            data = cursor.fetchall()  # 取出数据库所有数据

            cursor.close()
            conn.close()
            return data  # 返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.server))

    def update_data(self, sql):

        try:
            conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database,autocommit=True)
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result = cursor.execute(sql)  # 执行SQL语句
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.server))

    def insert_data(self, sql):

        try:
            conn = pymssql.connect(server=self.server, user=self.user, password=self.password, database=self.database,autocommit=True)
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute(sql)  # 执行SQL语句
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.server))

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cursor.close()
        self.conn.close()


if __name__=="__main__":
    mysql = MssqlConnect({"host":"218.192.166.14","user":"sa","password":"linzch2017","database":"darer49"})
    data = mysql.get_data("select * from summary_1")
    print(data)
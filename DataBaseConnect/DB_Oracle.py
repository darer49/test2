import cx_Oracle


class OracleConnect:
    def __init__(self, dbconfig):
        '''
        :param username:    数据库用户名
        :param password:    数据库密码
        :param host_ip:     数据库主机IP地址
        :param port:        数据库主机端口
        :param instance_name:   数据库实例名称
        :param sql:         数据库执行SQL语句
        '''
        self.username = dbconfig["user"]
        self.password = dbconfig["password"]
        self.host_ip = dbconfig["host"]
        self.instance_name = dbconfig["database"]
        self.port = "1521"
        self.mode = cx_Oracle.DEFAULT_AUTH

    def get_data(self, sql):

        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username, self.password, self.host_ip, self.port,
                                                             self.instance_name), mode=self.mode)  # 连接数据库字符串

            cursor = conn.cursor()  # 使用cursor()方法获取操作游标

            result = cursor.execute(sql)  # 执行SQL语句
            columns = [col[0] for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            data = cursor.fetchall()  # 取出数据库所有数据

            cursor.close()
            conn.close()
            return data  # 返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def get_dic(self, sql):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username, self.password, self.host_ip, self.port,
                                                             self.instance_name), mode=self.mode)  # 连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result = cursor.execute(sql)  # 执行SQL语句
            index = cursor.description
            cursor.close()
            conn.close()
            return index  # 返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def update_data(self, sql):

        try:
            conn = cx_Oracle.connect(
                '{}/{}@{}:{}/{}'.format(self.username, self.password, self.host_ip, self.port,
                                        self.instance_name), mode=self.mode)  # 连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result = cursor.execute(sql)  # 执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def insert_data(self, sql):

        try:
            conn = cx_Oracle.connect(
                '{}/{}@{}:{}/{}'.format(self.username, self.password, self.host_ip, self.port,
                                        self.instance_name), mode=self.mode)  # 连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute(sql)  # 执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))


if __name__ == "__main__":

    octest = OracleConnect({"user":"sys","password":"linzch2020","host":"202.38.228.20","database":"orcl"})
    octest.mode = cx_Oracle.SYSDBA
    # octest.username="123"
    data = octest.get_data("select * from EMISSION_RESULT_MONTH202103_TOTAL")
    print(data)

#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pymysql
from sql_func import *


class Mysql(object):
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.cursor = None

    @staticmethod
    def help():
        print("open in browser => https://shockerli.net/post/python3-pymysql/")

    def __enter__(self):
        self.db = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                  database=self.database)
        self.cursor = self.db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            # 如果出现异常
            print("An Exception: %s." % exc_val)

        self.db.close()
        return True

    def insert(self, table_name, **kwargs):
        """
        :param table_name: 要插入的表名  type: string
        :param kwargs: 要插入的属性名和属性值
        :return:
        """
        self.executeSql(insert(table_name, **kwargs))

    def delete(self, table_name, **kwargs):
        """
        :param table_name: 要删除的表名  type: string
        :param kwargs: 指定属性名和属性值(约束)
        :return: 
        """
        self.executeSql(delete(table_name, **kwargs))

    def update(self, table_name, new_items={}, where={}):
        """
        :param table_name: 要更新的表名  type: string
        :param kwargs: 要插入的属性名和属性值  type: dict
        :param where: 约束  type: dict
        :return: 
        """
        self.executeSql(update(table_name, new_items, where))

    def select_return_by_tuple(self, table_name, column_names=[], where={}, limit=None, offset=0):
        """
        :param table_name: 要查询的表名  type: string
        :param column_names: 待查询的属性  type: list
        :param where: 约束  type: dict
        :param limit: 返回记录数  type: int
        :param offset: SELECT语句开始查询的数据偏移量,必须指定limit  type: int
        :return: 以元组方式返回 ((),())
        """
        self.executeSql(select(table_name, column_names, where, limit, offset))
        return self.cursor.fetchall()

    def select_return_by_dict(self, table_name, column_names=[], where={}, limit=None, offset=0):
        """
        :param table_name: 要查询的表名  type: string
        :param column_names: 待查询的属性  type: list
        :param where: 约束  type: dict
        :param limit: 返回记录数  type: int
        :param offset: SELECT语句开始查询的数据偏移量,必须指定limit  type: int
        :return: 以字典方式返回 [{}, {}]
        """
        self.executeSql(select(table_name, column_names, where, limit, offset))
        result_tuple = self.cursor.fetchall()
        result_dict = []

        if not len(column_names):  # 未指定返回属性,默认返回全部属性
            # 获取全部属性名
            for column_info in self.cursor.description:
                column_names.append(column_info[0])

        for item in result_tuple:
            temp = {}
            for i in range(len(column_names)):
                temp[column_names[i]] = item[i]
            result_dict.append(temp)

        return result_dict

    def executeSql(self, sql):
        try:
            # 执行SQL语句
            self.cursor.execute(sql)
            # 提交到数据库执行，为了及时跟进数据库这句最好加上
            self.db.commit()
        except:
            # 如果发生错误则回滚
            self.db.rollback()

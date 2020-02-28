#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pymongo


class Mongo(object):
    def __init__(self, host="127.0.0.1", port=27017, username="", password="", database="test"):
        self.client = pymongo.MongoClient(host=host, port=port, username=username, password=password)
        self.db = self.client[database]

    def drop(self, collection_name):
        """
        删除整个集合
        :param collection_name: 要删除的集合名
        """
        collection = self.db[collection_name]
        collection.drop()

    def insert(self, collection_name, document) -> list:
        """
        :param collection_name: 要插入的集合名
        :param document: 要插入的数据 可以是 内含字典的列表 [{}, {}] 或 一个字典 {}
        :return: 插入的 _id 列表
        """
        collection = self.db[collection_name]
        if isinstance(document, dict):
            return collection.insert_many([document]).inserted_ids
        else:
            return collection.insert_many(document).inserted_ids

    def delete(self, collection_name, filter_: dict = {}, justOne: bool = False) -> int:
        """
        删除文档
        :param collection_name: 要删除的集合名
        :param filter_: 条件
        :param justOne: 是否只删除一个记录 True 删除一个，False 没有限制
        :return: 删除的行数
        """
        collection = self.db[collection_name]
        if justOne:
            return collection.delete_one(filter_).deleted_count
        else:
            return collection.delete_many(filter_).deleted_count

    def select(self, collection_name, filter_: dict = {}, justOne: bool = False):
        """
        查询文档
        :param collection_name: 要查询的集合名
        :param filter_: 条件
        :param justOne: 是否只查询一个记录 True 查询一个，False 没有限制
        :return: 查询结果<迭代器>
        """
        collection = self.db[collection_name]
        if justOne:
            def inner():
                yield collection.find_one(filter_)

            return inner()
        else:
            return collection.find(filter_)

    def update(self, collection_name, filter_: dict = {}, update: dict = {}, justOne: bool = False):
        """
        更新文档
        :param collection_name: 要更新的集合名
        :param filter_: 条件
        :param update: 要更新的键值对
        :param justOne: 是否只更新一个记录 True 更新一个，False 没有限制
        :return: 匹配数和影响数
        """
        collection = self.db[collection_name]
        if justOne:
            temp = collection.update_one(filter_, {"$set": update})
            return temp.matched_count, temp.modified_count
        else:
            temp = collection.update_many(filter_, {"$set": update})
            return temp.matched_count, temp.modified_count


if __name__ == '__main__':
    mongo = {
        "host": "localhost",
        "port": "27017",
        "username": "admin",
        "password": "123456",
        "database": "tutu"
    }

    m = Mongo(**mongo)
    m.insert("info", {"name": "kainhuck"})
    m.delete("info", {"name": "kainhuck"}, justOne=True)
    m.delete("info", {"name": "kainhuck"})
    m.update("info", filter_={"name": "kainhuck"}, update={"name": "tutu"}, justOne=True)
    m.update("info", filter_={"name": "kainhuck"}, update={"name": "tutu"})
    m.select(collection_name="info", justOne=True)
    ret = m.select(collection_name="info")
    for each in ret:
        print(each)

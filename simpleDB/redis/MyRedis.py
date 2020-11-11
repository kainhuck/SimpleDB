#!/usr/bin/python3
# -*- coding: utf-8 -*-

from redis import StrictRedis
from .func import decode_list, decode_list_tuple


class Redis(object):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: str = None):
        self.redis = StrictRedis(host=host, port=port, db=db, password=password)

    @staticmethod
    def help():
        """
        打印帮助信息
        """
        print("open in browser => https://cloud.tencent.com/developer/article/1151834")


    def increase(self, name, amount=1) -> int:
        """
        增加操作
        :param name: 名字
        :param amount: 增加的值，默认1
        :return: 增加后的值
        """
        return self.redis.incr(name, amount)

    def decrease(self, name, amount=1) -> int:
        """
        减少操作
        :param name: 名字
        :param amount: 减少的值，默认1
        :return: 减少后的值
        """
        return self.redis.decr(name, amount)

    def set_keys(self, **kwargs):
        """
        以字符串保存
        :param kwargs: 要插入的字符串键值对
        """
        self.redis.mset(kwargs)

    def get_keys(self, *string_name, decode="utf-8") -> list:
        """
        返回字符串的字，解码
        :param string_name: 要查询的字符串名
        :param decode: 是否返回解码后的字符串，默认是
        """
        if decode:
            return decode_list(self.redis.mget(string_name), decode)
        return self.redis.mget(string_name)

    def add_list_tail(self, list_name, *value) -> int:
        """
        从后追加进列表
        :param list_name: 列表名
        :value: 值
        :return: 列表大小
        """
        return self.redis.rpush(list_name, *value)

    def add_list_head(self, list_name, *value) -> int:
        """
        从后追加进列表
        :param list_name: 列表名
        :value: 值
        :return: 列表大小
        """
        return self.redis.lpush(list_name, *value)

    def get_list_length(self, list_name) -> int:
        """
        返回列表长
        :param list_name: 列表名
        :return: 列表长度
        """
        return self.redis.llen(list_name)

    def pop_list_head(self, list_name, decode="utf-8") -> str:
        """
        弹出列表头
        :param list_name: 列表名
        :param decode: 是否解码,以和中方式解码
        :return: 返回列表第一个值
        """
        if decode:
            return self.redis.rpop(list_name).decode(decode)
        return self.redis.rpop(list_name)

    def pop_list_tail(self, list_name, decode="utf-8") -> str:
        """
        弹出列表尾
        :param list_name: 列表名
        :param decode: 是否解码,以何种方式解码
        :return: 返回列表最后一个值
        """
        if decode:
            self.redis.lpop(list_name).decode(decode)
        return self.redis.lpop(list_name)

    def get_list_value(self, list_name, start=0, end=-1, decode="utf-8") -> list:
        """
        从列表中获取元素
        :param list_name: 列表名
        :param start: 起始下标
        :param end: 截至下标
        :param decode: 是否解码，以何种方式解码
        :return: 返回[star, end]之间的值
        """
        if decode:
            return decode_list(self.redis.lrange(list_name, start, end), decode)
        return self.redis.lrange(list_name, start, end)

    def set_list_value(self, list_name, index, value):
        """
        给列表指定下标赋值，越界报错
        :param list_name: 列表名
        :param index: 下标
        :param value: 新值
        """
        self.redis.lset(list_name, index, value)

    def remove_list_item_by_value(self, list_name, value, count=1) -> int:
        """
        删除名为list_name列表中值为value的元素count个
        :param list_name: 列表名
        :param value: 值
        :param count: 待删除的个数
        :return: 删除的个数
        """
        return self.redis.lrem(list_name, count, value)

    def add_to_set(self, set_name, *values) -> int:
        """
        向集合中增加元素
        :param set_name: 集合名
        :param values: 数据
        :return: 返回插入的个数
        """
        return self.redis.sadd(set_name, *values)

    def remove_set_item_by_value(self, set_name, *values):
        """
        向集合中删除元素
        :param set_name: 集合名
        :param values: 要删除的胡数据
        :return: 返回删除的个数
        """
        return self.redis.srem(set_name, *values)

    def get_set_length(self, set_name):
        """
        返回集合长度
        :param set_name: 集合名
        :return: 集合长度
        """
        return self.redis.scard(set_name)

    def pop_set_random(self, set_name, count=1, decode="utf-8") -> list:
        """
        从集合中随机弹出count个值
        :param set_name: 集合名
        :param count: 弹出的个数
        :param decode: 是否解码，以及编码方式
        :return: 集合中的值
        """
        if decode:
            return decode_list(self.redis.spop(set_name, count), decode)
        return self.redis.spop(set_name, count)

    def inter_set(self, set_names: list, decode="utf-8") -> set:
        """
        求列表中的集合的交集
        :param set_names: 集合名列表
        :param decode: 是否对返回结果解码，以及解码方式
        :return: 交集，集合类型
        """
        if decode:
            return set(decode_list(self.redis.sinter(set_names), decode))
        return self.redis.sinter(set_names)

    def union_set(self, set_names: list, decode="utf-8") -> set:
        """
        求列表中的集合的并集
        :param set_names: 集合名列表
        :param decode: 是否对返回结果解码，以及解码方式
        :return: 并集，集合类型
        """
        if decode:
            return set(decode_list(self.redis.sunion(set_names), decode))
        return self.redis.sunion(set_names)

    def diff_set(self, set_names: list, decode="utf-8") -> set:
        """
        求列表中的集合的差集
        :param set_names: 集合名列表
        :param decode: 是否对返回结果解码，以及解码方式
        :return: 差集，集合类型
        """
        if decode:
            return set(decode_list(self.redis.sdiff(set_names), decode))
        return self.redis.sdiff(set_names)

    def get_set_all(self, set_name, decode="utf-8") -> set:
        """
        返回集合中的所有元素
        :param set_name: 集合名列表
        :param decode: 是否对返回结果解码，以及解码方式
        :return: 差集，集合类型
        """
        if decode:
            return set(decode_list(self.redis.smembers(set_name), decode))
        return self.redis.smembers(set_name)

    def get_set_random(self, set_name, count=1, decode="utf-8") -> list:
        """
        从集合中随机获取count个值
        :param set_name: 集合名
        :param count: 弹出的个数
        :param decode: 是否解码，以及编码方式
        :return: 集合中的值
        """
        if decode:
            return decode_list(self.redis.srandmember(set_name, count), decode)
        return self.redis.srandmember(set_name, count)

    def add_to_zset(self, zset_name, **map):
        """
        向有序集合中加入元素
        :param zset_name: 有序集合名
        :param map: 键值对，key为插入的元素值，value为权重int类型
        """
        self.redis.zadd(zset_name, map)

    def remove_zset_item_by_value(self, zset_name, *value):
        """
        删除有序集合中名为value的元素
        :param zset_name: 有序集合名
        :param value: 要删除的值
        """
        self.redis.zrem(zset_name, *value)

    def increase_zset_item_by_value(self, zset_name, value, amount=1):
        """
        将有序集合zset_name中值为value的权重加amount
        :param zset_name: 有序集合名
        :param value: 要增加权重的值
        :param amount: 增加的权重,负数为减
        """
        self.redis.zincrby(zset_name, amount, value)

    def item_rank_in_zset(self, zset_name, value, reverse=False) -> int:
        """
        返回value在zset中的排名
        :param zset_name: 有序集合名
        :param value: 值
        :param reverse: False -> 从小到大  True -> 从大到小
        :return: 返回value在zset中的排名
        """
        if reverse:
            return self.redis.zrevrank(zset_name, value)
        return self.redis.zrank(zset_name, value)

    def get_zset_value_by_rank(self, zset_name, start=0, end=-1, withscores=False, decode="utf-8",
                               reverse=False) -> list:
        """
        从有序集合中获取下标在指定范围的值
        :param zset_name: 有序集合名
        :param start: 开始下标
        :param end: 结束下标
        :param withscores: 是否带有权值
        :param decode: 返回结果是否解码
        :param reverse: False -> 从小到大 True -> 从大到小
        :return: 从有序集合中获取下标在指定范围的值，list类型
        """
        if withscores:
            if decode:
                return decode_list_tuple(self.redis.zrange(zset_name, start, end, reverse, withscores), decode)
            return self.redis.zrange(zset_name, start, end, reverse, withscores)
        else:
            if decode:
                return decode_list(self.redis.zrange(zset_name, start, end, reverse, withscores), decode)
            return self.redis.zrange(zset_name, start, end, reverse, withscores)

    def get_zset_value_by_score(self, zset_name, min, max, start=None, num=None, withscores=False,
                                decode="utf-8", ) -> list:
        """
        从有序集合中获取权值在指定范围的值
        :param zset_name: 有序集合名
        :param min: 最小权值
        :param max: 最大权值
        :param start: 从哪个下标开始查找
        :param num: 查找个数 start 必须 和 num同时设定
        :param withscores: 是否带有权值
        :param decode: 返回结果是否解码
        :param reverse: False -> 从小到大 True -> 从大到小
        :return: 有序集合中获取权值在指定范围的值，list类型
        """
        if withscores:
            if decode:
                return decode_list_tuple(self.redis.zrangebyscore(zset_name, min, max, start, num, withscores), decode)
            return self.redis.zrangebyscore(zset_name, min, max, start, num, withscores)
        else:
            if decode:
                return decode_list(self.redis.zrangebyscore(zset_name, min, max, start, num, withscores), decode)
            return self.redis.zrangebyscore(zset_name, min, max, start, num, withscores)

    def count_zset_value_by_score(self, zset_name, min, max) -> int:
        """
        统计在权值在区间内的数量
        :param zset_name: 有序集合名
        :param min: 最小值
        :param max: 最大值
        :return: 返回数量
        """
        return self.redis.zcount(zset_name, min, max)

    def get_zset_length(self, zset_name) -> int:
        """
        返回zset长度
        :param zset_name: 有序集合名
        :return: 返回长度
        """
        return self.redis.zcard(zset_name)

    def remove_zset_item_by_rank(self, zset_name, start, end):
        """
        从有序集合中删除下标在指定范围中的值
        :param zset_name: 有序集合名
        :param start: 开始排名
        :param end: 结束排名
        """
        self.redis.zremrangebyrank(zset_name, start, end)

    def remove_zset_item_by_score(self, zset_name, min, max):
        """
        从有序集合中删除权值在指定范围中的值
        :param zset_name: 有序集合名
        :param min: 最小值
        :param max: 最大值
        """
        self.redis.zremrangebyscore(zset_name, min, max)

    def add_to_map(self, map_name, **kwargs) -> int:
        """
        向映射中添加键值对
        :param map_name: 映射名
        :param kwargs: 要添加的键值对
        :return: 增加的键值对的个数
        """
        self.redis.hmset(map_name, kwargs)
        return len(kwargs)

    def remove_map_item_by_keys(self, map_name, *keys) -> int:
        """
        删除映射中键为keys中的元素
        :param map_name: 映射名
        :param keys: 要删除的键的list
        :return: 删除的个数
        """
        self.redis.hdel(map_name, *keys)
        return len(keys)

    def update_map_item(self, map_name, **kwargs) -> int:
        """
        修改映射键值对
        :param map_name: 映射名
        :param kwargs: 新的键值对，如果不存在则添加
        :return: 修改的个数
        """
        for key, value in kwargs.items():
            self.redis.hsetnx(map_name, key, value)
        return len(kwargs)

    def get_value_from_map_by_keys(self, map_name, *keys, decode="utf-8") -> list:
        """
        从映射中获取对应key的value
        :param map_name: 映射名
        :param keys: 要获取的key
        :param decode: 返回结果是否解码，以及以何种方式解码
        :return: 返回对应的value
        """
        if decode:
            return decode_list(self.redis.hmget(map_name, keys), decode)
        return self.redis.hmget(map_name, keys)

    def get_map_length(self, map_name) -> int:
        """
        获取映射的元素个数
        :param map_name: 映射名
        :return: 元素个数
        """
        return self.redis.hlen(map_name)

    def get_map_all_keys(self, map_name, decode="utf-8") -> list:
        """
        返回映射所有的key
        :param map_name: 映射名
        :param decode: 返回结果是否解码，以及以何种方式解码
        :return: 映射key列表
        """
        if decode:
            return decode_list(self.redis.hkeys(map_name), decode)
        return self.redis.hkeys(map_name)

    def get_map_all_values(self, map_name, decode="utf-8") -> list:
        """
        返回映射所有的value
        :param map_name: 映射名
        :param decode: 返回结果是否解码，以及以何种方式解码
        :return: 映射value列表
        """
        if decode:
            return decode_list(self.redis.hvals(map_name), decode)
        return self.redis.hvals(map_name)


if __name__ == '__main__':
    connect = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": "123456"
    }
    r = Redis(**connect)
    data = {
        "name:01": "kk",
        "name:02": "tt"
    }
    # r.insert_string(name1="das",name2="grw")
    r.set_keys(**data)
    # print(r.select_string("name1", "name2"))
    # print(r.select_string("name:01", "name:02"))
    # r.help()

    # print(r.increase("age", 4))
    # print(r.decrease("age", 4))
    # r.insert_string(name="kain")
    # print(r.get_keys("name", "age"))
    # r.add_list_tail("users", "tutu", "kangkang")
    # print(r.get_list_value("users"))
    # print(r.get_list_length("users"))
    # print(r.add_to_set("set01", "value1", "value2", "value3"))
    # print(r.add_to_set("set02", "value1", "value4", "value5"))
    # r.add_to_set("set01", "value4")
    # print(r.get_set_length("set01"))
    # print(r.pop_set_random("set01", 2))
    # print(r.inter_set(["set01", "set02"]))
    # print(r.get_set_random("set01", 2))
    # r.add_to_zset("zset:01", kainhuck=10, xsy=20)
    # print(r.item_rank_in_zset("zset:01", "kangkang", reverse=False))
    # print(r.get_zset_value_by_rank("zset:01", withscores=True, reverse=True))
    # print(r.get_zset_value_by_score("zset:01", 0, 100, withscores=True))
    # print(r.count_zset_value_by_score("zset:01", 0, 30))
    # print(r.add_to_map("map:01", hobby="tutu", lover="xsy"))
    # r.remove_map_item_by_keys("map:01", "name", "age")
    # r.zset
    # print(r.update_map_item("map:01", name="dad"))
    # print(r.get_value_from_map_by_keys("map:01", "name"))
    print(r.get_map_length("map:01"))
    print(r.get_map_all_keys("map:01"))
    print(r.get_map_all_values("map:01"))

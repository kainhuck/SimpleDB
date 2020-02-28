#!/usr/bin/python3
# -*- coding: utf-8 -*-


def decode_list(ll: list, decode="utf-8"):
    """
    将列表中的每一个元素用utf-8解码
    """
    return [each.decode(decode) for each in ll]


def decode_list_tuple(lt: list, decode="utf-8"):
    """
    将列表中的每一个元组的第一反而元素用utf-8解码
    """
    return [(each[0].decode(decode), each[1]) for each in lt]

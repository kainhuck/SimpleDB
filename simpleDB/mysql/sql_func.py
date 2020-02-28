# 这里定义了一些构造sql语句的函数
def insert(table_name, **kwargs):
    '''
    构造sql插入语句

    :param table_name: 要插入的表名  type: string
    :param kwargs: 要插入的属性名和属性值
    :return: sql语句

    INSERT INTO table_name ( field1, field2,...fieldN )
                       VALUES
                       ( value1, value2,...valueN );
    '''
    # 创建sql语句
    field = '('
    for each in kwargs.keys():
        field += each + ','
    field = field[:-1] + ') VALUES ('

    for each in kwargs.values():
        field += '"' + str(each) + '",'
    field = field[:-1] + ');'

    sql = "INSERT INTO %s %s" % (table_name, field)
    return sql


def delete(table_name, **kwargs):
    """
    构造sql删除语句

    :param table_name: 要删除的表名  type: string
    :param kwargs: 指定属性名和属性值(约束)
    :return: sql语句

    DELETE FROM table_name [WHERE Clause]
    """
    field = ''
    if len(kwargs):
        field = " WHERE "
        for item in kwargs.items():
            field += item[0] + '="' + str(item[1]) + '" AND '
        field = field[:-5]
    sql = "DELETE FROM %s%s;" % (table_name, field)
    return sql


def update(table_name, new_items={}, where={}):
    """
    构造sql更新语句

    :param table_name: 要更新的表名  type: string
    :param kwargs: 要插入的属性名和属性值  type: dict
    :param where: 约束  type: dict
    :return: sql语句

    UPDATE table_name SET field1=new-value1, field2=new-value2
    [WHERE Clause]
    """
    field = ""
    where_part = ""

    for item in new_items.items():
        field += str(item[0]) + '="' + str(item[1]) + '" AND '
    field = field[:-5]

    if len(where):
        where_part = " WHERE "
        for item in where.items():
            where_part += str(item[0]) + '="' + str(item[1]) + '" AND '
        where_part = where_part[:-5]

    sql = "UPDATE %s SET %s%s;" % (table_name, field, where_part)
    return sql


def select(table_name, column_names=[], where={}, limit=None, offset=0):
    """
    构造sql选择语句

    :param table_name: 要查询的表名  type: string
    :param column_names: 待查询的属性  type: list
    :param where: 约束  type: dict
    :param limit: 返回记录数  type: int
    :param offset: SELECT语句开始查询的数据偏移量,必须指定limit  type: int
    :return: sql语句

    SELECT column_name,column_name
    FROM table_name
    [WHERE Clause]
    [LIMIT N][ OFFSET M]
    """
    field = "*"
    where_part = ""
    limit_part = ""
    offset_part = ""

    if len(column_names):
        # 构造查询字段
        field = ""
        for each in column_names:
            field += str(each) + ","
        field = field[:-1]

    if len(where):
        # 构造 where 部分
        where_part = " WHERE "
        for item in where.items():
            where_part += str(item[0]) + '="' + str(item[1]) + '" AND '
        where_part = where_part[:-5]

    if limit != None:
        # 构造 limit 部分
        limit_part = " LIMIT " + str(int(limit))

        if offset > 0:
            # 构造 offset 部分:
            offset_part = " OFFSET " + str(int(offset))

    sql = "SELECT %s FROM %s%s%s%s;" % (field, table_name, where_part,
                                        limit_part, offset_part)
    return sql

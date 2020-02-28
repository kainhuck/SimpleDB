from distutils.core import setup


requires=["pymysql", "pymongo", "redis"]

setup(name="simpleDB",
      version="0.1",
      author="kainhuck",
      author_email="kainhuck@163.com",
      description="simple use of Mysql MongoDB and Redis",
      packages=["simpleDB", "simpleDB.mongo", "simpleDB.mysql", "simpleDB.redis"])

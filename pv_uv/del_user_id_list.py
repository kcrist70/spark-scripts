import redis
from config import config
config = config('../example.ini')
"""
因插错redis表，而进行清除操作,清除相关集合,手动列出botname 列表

"""

try:
    pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
    redis_connect79 = redis.Redis(connection_pool=pool79)
except Exception as e:
    raise e




def del_user_id(name):
    key_list = redis_connect79.keys(name + ":*" )
    redis_connect79.delete(name)
    for name in key_list:
        redis_connect79.delete(name)

# #儿童
triobrain = []
# for i in triobrain:
#     del_user_id(i)


redis_connect79.hdel("chatlog_pv",*triobrain)
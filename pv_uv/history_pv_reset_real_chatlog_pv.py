import redis
from config import config
config = config('../example.ini')

try:
    pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
    redis_connect = redis.Redis(connection_pool=pool)
except Exception as e:
    raise e


botname_list = redis_connect.hgetall("chatlog_history_pv")
for botname in botname_list:
    redis_connect.hincrby("chatlog_pv",botname,botname_list[botname])

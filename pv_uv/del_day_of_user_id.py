import redis
from config import config
config = config('../example.ini')

try:
    pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'], port=config['redis-for-uv-pv']['port'],
                                  password=config['redis-for-uv-pv']['password'],
                                  decode_responses=config['redis-for-uv-pv']['decode_responses'])
    redis_connect79 = redis.Redis(connection_pool=pool79)
except Exception as e:
    raise e


key_l = redis_connect79.keys("*:2019-08-12")
for name in key_l:
    nickname = name.split(":")[0]
    num = redis_connect79.scard(name)
    user_ids = redis_connect79.spop(name, num)
    redis_connect79.srem(nickname, *user_ids)
import redis
from rq import Worker, Queue, Connection

#conn = redis.Redis('192.168.55.109', 6379)
conn = redis.Redis('priseOQ.fe.up.pt', 6379)

#conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(Queue('risco'))
        #worker = Worker(map(Queue, ['rta']))
        worker.work()

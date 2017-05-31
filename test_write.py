import datetime
import pycassa
from pycassa.pool import ConnectionPool

import random
import string

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

pool = ConnectionPool('bench', ['localhost:19160'])
cf1 = pycassa.ColumnFamily(pool, 'testinsert')

#for i in range(10000):
#    id = id_generator(5, string.digits)
#    print line.insert(id, {'column': 'accepted'})

b = cf1.batch(queue_size=100)

for i in xrange(1000000):
    key = id_generator(5, string.digits)
    print b.insert(key, {'column': 'accepted'})

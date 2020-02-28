
#import the module
import sys
import logging
import aerospike
from aerospike import predicates as p

# Configure the client
config = {
  'hosts': [ ('127.0.0.1', 3000) ]
}

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()

except:
  import sys
  logging.error("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

client.index_integer_create('test', 'myset', 'phone', 'myset' + '_phone_idx')

def add_customer(customer_id, phone_number, lifetime_value):
  key = ('test', 'myset', customer_id)
  client.put(key, {'phone': phone_number, 'ltv': lifetime_value})


def get_ltv_by_id(customer_id):
    key = ('test', 'myset', customer_id)
    (key, metadata, record) = client.get(key)
    if record == {}:
      logging.error('Requested non-existent customer ' + str(customer_id))
    else:
      return record.get('ltv')

def get_ltv_by_phone(phone_number):
    records = client.query('test', 'myset').select('phone', 'ltv').where(p.equals('phone', phone_number)).results()
    if len(records) > 0:
        return records[0][2]['ltv']
    logging.error('Requested phone number is not found ' + str(phone_number))



for i in range(0, 1000):
  add_customer(i, i, i + 1)


for i in range(0, 1000):
  assert (i + 1 == get_ltv_by_id(i)), "No LTV by ID" + str(i)
  assert (i + 1 == get_ltv_by_phone(i)), "No LTV by phone" + str(i)


# Close the connection to the Aerospike cluster
client.close()
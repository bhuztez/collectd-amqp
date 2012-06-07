import collectd
import string
import threading
import ssl
import amqplib.client_0_8 as amqp


COLLECTD_TYPES = {}
lock = threading.Lock()

AMQP_EXCHANGE = 'graphite'
AMQP_HOST = 'localhost:5672'

METRIC_PREFIX = None

SSL_CERT = None
SSL_KEY = None
SSL_CACERT = None

connection = None
channel = None



def sanitize_field(field):
    field = field.strip()
    trans = string.maketrans(' .', '.'* 2)
    field = field.translate(trans, '()')
    return field



def parse_types_file(path):
    global COLLECTD_TYPES

    with open(path, 'r') as f:
        for line in f:
            fields = line.split()
            if len(fields) < 2:
                continue

            type_name = fields[0]

            if type_name[0] == '#':
                continue

            v = []
            for ds in fields[1:]:
                ds = ds.rstrip(',')
                ds_fields = ds.split(':')

                if len(ds_fields) != 4:
                    collectd.warning('amqp-writer: cannot parse data source %s on type %s' % ( ds, type_name ))
                    continue

                v.append(ds_fields)

            COLLECTD_TYPES[type_name] = v



def amqp_config(config):
    global AMQP_EXCHANGE, AMQP_HOST, AMQP_PORT
    global METRIC_PREFIX
    global SSL_CERT, SSL_KEY, SSL_CACERT
    global connection, channel
    
    for child in config.children:
        if child.key == 'TypesDB':
            for v in child.values:
                parse_types_file(v)
        elif child.key == 'AMQP_EXCHANGE':
            AMQP_EXCHANGE = child.values[0]
        elif child.key == 'AMQP_HOST':
            AMQP_HOST = child.values[0]
        elif child.key == 'METRIC_PREFIX':
            METRIC_PREFIX = child.values[0]
        elif child.key == 'SSL_CERT':
            SSL_CERT = child.values[0]
        elif child.key == 'SSL_KEY':
            SSL_KEY = child.values[0]
        elif child.key == 'SSL_CACERT':
            SSL_CACERT = child.values[0]

    if SSL_KEY is not None:
        connection = amqp.Connection(
            host = AMQP_HOST,
            ssl = {
                'ca_certs': SSL_CACERT,
                'keyfile' : SSL_KEY,
                'certfile': SSL_CERT,
                'cert_reqs': ssl.CERT_REQUIRED })
    else:
        connection = amqp.Connection(host=AMQP_HOST)
    
    channel = connection.channel()
    channel.exchange_declare(AMQP_EXCHANGE, type='topic', durable=True, auto_delete=False)
    collectd.register_write(amqp_write)



def amqp_write(v, data=None):
    if v.type not in COLLECTD_TYPES:
        collectd.warning('amqp-writer: do not know how to handle type %s. do you have all your types.db files configured?' % v.type)
        return

    v_type = COLLECTD_TYPES[v.type]

    if len(v_type) != len(v.values):
        collectd.warning('amqp-writer: differing number of values for type %s' % v.type)
        return

    metric_fields = []
    
    if METRIC_PREFIX is not None:
        metric_fields.append(METRIC_PREFIX)

    metric_fields.append(v.host.replace('.', '_'))

    metric_fields.append(v.plugin)
    if v.plugin_instance:
        metric_fields.append(sanitize_field(v.plugin_instance))

    metric_fields.append(v.type)
    if v.type_instance:
        metric_fields.append(sanitize_field(v.type_instance))

    time = v.time

    lines = []

    for i, value in enumerate(v.values):
        ds_name = v_type[i][0]
        ds_type = v_type[i][1]

        path_fields = metric_fields[:]
        path_fields.append(ds_name)
        metric = '.'.join(path_fields)
        new_value = None        
        lines.append('%s %f %d' % ( metric, value, time ))

    lock.acquire()
    channel.basic_publish(amqp.Message('\n'.join(lines)), AMQP_EXCHANGE)
    lock.release()



collectd.register_config(amqp_config)



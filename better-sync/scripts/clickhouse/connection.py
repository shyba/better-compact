"""Native TLS connection for better-sync; requires explicit trust configuration."""
import os
import ssl


def connect():
    from clickhouse_driver import Client
    ca = os.environ.get('CLICKHOUSE_CA')
    client = Client(host=os.environ['CLICKHOUSE_HOST'], port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
        database=os.environ['CLICKHOUSE_DATABASE'], secure=True, verify=True,
        check_hostname=True, compression='lz4', **({'ca_certs':ca} if ca else {}),
        connect_timeout=3, send_receive_timeout=30,
        settings={'max_threads':2,'max_memory_usage':536870912,'max_execution_time':20,'async_insert':0})
    if ca and os.environ.get('CLICKHOUSE_ALLOW_PINNED_LEAF')=='1':
        original = client.connection._create_ssl_context
        def context(options):
            ctx = original(options)
            ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
            return ctx
        client.connection._create_ssl_context = context
    return client

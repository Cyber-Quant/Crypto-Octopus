import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default',
                                       password='')

db_name = 'crypto'
sql_cmd = 'CREATE DATABASE IF NOT EXISTS %s' % db_name

client.command(sql_cmd)

ret = client.command('SHOW DATABASES')
print(ret)

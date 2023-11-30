import clickhouse_connect
import pandas as pd

from datetime import datetime

client = clickhouse_connect.get_client(host='localhost', username='default',
                                       password='')


def query_spot_agg_trades_range(trade_pair, begin_time, end_time):
    sql_cmd = '''
    SELECT * FROM (
        SELECT * FROM crypto.spot_%s_aggTrades
        WHERE (Timestamp >= %d) AND (Timestamp <= %d) 
    ) ORDER BY Aggregate_tradeId DESC
    ''' % (trade_pair, begin_time, end_time)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_agg_trades_range_df(trade_pair, begin_time, end_time):
    data = query_spot_agg_trades_range(trade_pair, begin_time, end_time)
    column_names = ['Aggregate_tradeId', 'Price', 'Quantity', 'First_tradeId',
                    'Last_tradeId', 'Timestamp', 'Was_the_buyer_the_maker',
                    'Was_the_trade_the_best_price_match']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
begin_time = 1700870400001
end_time = 1700870400003
data = query_spot_agg_trades_range(trade_pair, begin_time, end_time)
df = query_spot_agg_trades_range_df(trade_pair, begin_time, end_time)


def query_spot_agg_trades_trunk(trade_pair, end_time, n):
    sql_cmd = '''
    SELECT * FROM (
        SELECT * FROM crypto.spot_%s_aggTrades
        WHERE (Timestamp <= %d)
    ) ORDER BY Aggregate_tradeId DESC LIMIT %d
    ''' % (trade_pair, end_time, n)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_agg_trades_trunk_df(trade_pair, end_time, n):
    data = query_spot_agg_trades_trunk(trade_pair, end_time, n)
    column_names = ['Aggregate_tradeId', 'Price', 'Quantity', 'First_tradeId',
                    'Last_tradeId', 'Timestamp', 'Was_the_buyer_the_maker',
                    'Was_the_trade_the_best_price_match']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
n = 3
end_time = 1700870400003
data = query_spot_agg_trades_trunk(trade_pair, end_time, n)
df = query_spot_agg_trades_trunk_df(trade_pair, end_time, n)


def query_spot_k_lines_range(trade_pair, cycle, begin_time, end_time):
    sql_cmd = '''
    SELECT * FROM (
        SELECT *  FROM crypto.spot_%s_k_lines_%s
        WHERE (Open_time >= %d) AND (Open_time <= %d)
    ) ORDER BY Open_time DESC
    ''' % (trade_pair, cycle, begin_time, end_time)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_k_lines_range_df(trade_pair, cycle, begin_time, end_time):
    data = query_spot_k_lines_range(trade_pair, cycle, begin_time, end_time)
    column_names = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
                    'Close_time', 'Quote_asset_volume', 'Number_of_trades',
                    'Taker_buy_base_asset_volume',
                    'Taker_buy_quote_asset_volume', 'Ignore']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
cycle = '1h'
begin_time = 1700874000000
end_time = 1700881200000
data = query_spot_k_lines_range(trade_pair, cycle, begin_time, end_time)
df = query_spot_k_lines_range_df(trade_pair, cycle, begin_time, end_time)


def query_spot_k_lines_trunk(trade_pair, cycle, end_time, n):
    sql_cmd = '''
    SELECT * FROM (
        SELECT * FROM crypto.spot_%s_k_lines_%s
        WHERE (Open_time <= %d)
    ) ORDER BY Open_time DESC LIMIT %d
    ''' % (trade_pair, cycle, end_time, n)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_k_lines_trunk_df(trade_pair, cycle, end_time, n):
    data = query_spot_k_lines_trunk(trade_pair, cycle, end_time, n)
    column_names = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
                    'Close_time', 'Quote_asset_volume', 'Number_of_trades',
                    'Taker_buy_base_asset_volume',
                    'Taker_buy_quote_asset_volume', 'Ignore']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
cycle = '1s'
n = 3
end_time = 1700881200000
data = query_spot_k_lines_trunk(trade_pair, cycle, end_time, n)
df = query_spot_k_lines_trunk_df(trade_pair, cycle, end_time, n)


def query_spot_trades_range(trade_pair, begin_time, end_time):
    sql_cmd = '''
    SELECT * FROM (
        SELECT * FROM crypto.spot_%s_trades
        WHERE (time >= %d) AND (time <= %d)
    ) ORDER BY trade_Id DESC
    ''' % (trade_pair, begin_time, end_time)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_trades_range_df(trade_pair, begin_time, end_time):
    data = query_spot_trades_range(trade_pair, begin_time, end_time)
    column_names = ['trade_Id', 'Price', 'qty', 'quoteQty', 'time',
                    'isBuyerMaker', 'isBestMatch']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
begin_time = 1700870400004
end_time = 1700870400006
data = query_spot_trades_range(trade_pair, begin_time, end_time)
df = query_spot_trades_range_df(trade_pair, begin_time, end_time)


def query_spot_trades_trunk(trade_pair, end_time, n):
    sql_cmd = '''
    SELECT * FROM (
        SELECT * FROM crypto.spot_%s_trades
        WHERE (time <= %d)
    ) ORDER BY trade_Id DESC LIMIT %d
    ''' % (trade_pair, end_time, n)
    query_set = client.query(sql_cmd)
    return query_set.result_rows


def query_spot_trades_trunk_df(trade_pair, end_time, n):
    data = query_spot_trades_trunk(trade_pair, end_time, n)
    column_names = ['trade_Id', 'Price', 'qty', 'quoteQty', 'time',
                    'isBuyerMaker', 'isBestMatch']
    return pd.DataFrame(data, columns=column_names)


trade_pair = 'BTCUSDT'
n = 3
end_time = 1700870400006
data = query_spot_trades_trunk(trade_pair, end_time, n)
df = query_spot_trades_trunk_df(trade_pair, end_time, n)


def datetime_str_to_datetime(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")


def datetime_to_timestamp(datetime_obj):
    return int(round(datetime_obj.timestamp() * 1000))


def datetime_str_to_timestamp(datetime_str):
    dt = datetime_str_to_datetime(datetime_str)
    return datetime_to_timestamp(dt)

import clickhouse_connect
import numpy as np
import pandas as pd

from pathlib import Path

client = clickhouse_connect.get_client(host='localhost', username='default',
                                       password='')


def insert_spot_agg_trades(file_path):
    column_names = ['Aggregate_tradeId', 'Price', 'Quantity', 'First_tradeId',
                    'Last_tradeId', 'Timestamp', 'Was_the_buyer_the_maker',
                    'Was_the_trade_the_best_price_match']
    data_type = {
        'Aggregate_tradeId': np.uint64,
        'Price': np.float64,
        'Quantity': np.float64,
        'First_tradeId': np.uint64,
        'Last_tradeId': np.uint64,
        'Timestamp': np.uint64,
        'Was_the_buyer_the_maker': np.bool_,
        'Was_the_trade_the_best_price_match': np.bool_
    }
    df = pd.read_csv(file_path, header=None, names=column_names,
                     dtype=data_type)
    data = df.to_records(index=False).tolist()

    file_name = file_path.stem
    ret = file_name.split('-')
    trade_pair = ret[0]
    table_name = 'crypto.spot_' + trade_pair + '_aggTrades'

    client.insert(table_name, data, column_names=column_names)


file_path = Path('~/Downloads/BTCUSDT-aggTrades-2023-11-25.csv')
insert_spot_agg_trades(file_path)


def insert_spot_k_lines(file_path):
    column_names = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
                    'Close_time', 'Quote_asset_volume', 'Number_of_trades',
                    'Taker_buy_base_asset_volume',
                    'Taker_buy_quote_asset_volume', 'Ignore']
    data_type = {
        'Open_time': np.uint64,
        'Open': np.float64,
        'High': np.float64,
        'Low': np.float64,
        'Close': np.float64,
        'Volume': np.float64,
        'Close_time': np.uint64,
        'Quote_asset_volume': np.float64,
        'Number_of_trades': np.uint64,
        'Taker_buy_base_asset_volume': np.float64,
        'Taker_buy_quote_asset_volume': np.float64,
        'Ignore': np.uint64
    }
    df = pd.read_csv(file_path, header=None, names=column_names,
                     dtype=data_type)
    data = df.to_records(index=False).tolist()

    file_name = file_path.stem
    ret = file_name.split('-')
    trade_pair = ret[0]
    cycle = ret[1]
    table_name = 'crypto.spot_' + trade_pair + '_k_lines_' + cycle

    client.insert(table_name, data, column_names=column_names)


file_path = Path('~/Downloads/BTCUSDT-1s-2023-11-25.csv')
insert_spot_k_lines(file_path)
file_path = Path('~/Downloads/BTCUSDT-1m-2023-11-25.csv')
insert_spot_k_lines(file_path)
file_path = Path('~/Downloads/BTCUSDT-1h-2023-11-25.csv')
insert_spot_k_lines(file_path)
file_path = Path('~/Downloads/BTCUSDT-1d-2023-10.csv')
insert_spot_k_lines(file_path)


def insert_spot_trades(file_path):
    column_names = ['trade_Id', 'Price', 'qty', 'quoteQty', 'time',
                    'isBuyerMaker', 'isBestMatch']
    data_type = {
        'trade_Id': np.uint64,
        'Price': np.float64,
        'qty': np.float64,
        'quoteQty': np.float64,
        'time': np.uint64,
        'isBuyerMaker': np.bool_,
        'isBestMatch': np.bool_
    }
    df = pd.read_csv(file_path, header=None, names=column_names,
                     dtype=data_type)
    data = df.to_records(index=False).tolist()

    file_name = file_path.stem
    ret = file_name.split('-')
    trade_pair = ret[0]
    table_name = 'crypto.spot_' + trade_pair + '_trades'

    client.insert(table_name, data, column_names=column_names)


file_path = Path('~/Downloads/BTCUSDT-trades-2023-11-25.csv')
insert_spot_trades(file_path)

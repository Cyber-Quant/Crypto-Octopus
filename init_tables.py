import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default',
                                       password='')


def create_spot_agg_trades_table(trade_pair):
    sql_cmd = '''
    CREATE TABLE IF NOT EXISTS crypto.spot_%s_aggTrades
    (
        Aggregate_tradeId UInt64, 
        Price Decimal64(8),
        Quantity Decimal64(8),
        First_tradeId UInt64,
        Last_tradeId UInt64,
        Timestamp UInt64,
        Was_the_buyer_the_maker Bool,
        Was_the_trade_the_best_price_match Bool    
    )
    ENGINE = ReplacingMergeTree
    PRIMARY KEY (Aggregate_tradeId)
    ORDER BY (Aggregate_tradeId)
    ''' % trade_pair
    client.command(sql_cmd)


trade_pair = 'BTCUSDT'
create_spot_agg_trades_table(trade_pair)


def create_spot_k_lines_table(trade_pair, cycle):
    sql_cmd = '''
    CREATE TABLE IF NOT EXISTS crypto.spot_%s_k_lines_%s
    (
        Open_time UInt64, 
        Open Decimal64(8),
        High Decimal64(8),
        Low Decimal64(8),
        Close Decimal64(8),
        Volume Decimal64(8),
        Close_time UInt64,
        Quote_asset_volume Decimal64(8),
        Number_of_trades UInt64,
        Taker_buy_base_asset_volume Decimal64(8),
        Taker_buy_quote_asset_volume Decimal64(8),
        Ignore UInt64
    )
    ENGINE = ReplacingMergeTree
    PRIMARY KEY (Open_time)
    ORDER BY (Open_time)
    ''' % (trade_pair, cycle)
    client.command(sql_cmd)


trade_pair = 'BTCUSDT'
cycle = '1s'
create_spot_k_lines_table(trade_pair, cycle)
cycle = '1m'
create_spot_k_lines_table(trade_pair, cycle)
cycle = '1h'
create_spot_k_lines_table(trade_pair, cycle)
cycle = '1d'
create_spot_k_lines_table(trade_pair, cycle)


def create_spot_trades_table(trade_pair):
    sql_cmd = '''
    CREATE TABLE IF NOT EXISTS crypto.spot_%s_trades
    (
        trade_Id UInt64, 
        Price Decimal64(8),
        qty Decimal64(8),
        quoteQty Decimal64(8),
        time UInt64,
        isBuyerMaker Bool,
        isBestMatch Bool    
    )
    ENGINE = ReplacingMergeTree
    PRIMARY KEY (trade_Id)
    ORDER BY (trade_Id)
    ''' % trade_pair
    client.command(sql_cmd)


trade_pair = 'BTCUSDT'
create_spot_trades_table(trade_pair)

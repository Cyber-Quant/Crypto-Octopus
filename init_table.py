import sys
from pathlib import Path
import duckdb

DB_FILE = Path("data.duckdb")


def init_tables(force_rebuild: bool = False):
    if not DB_FILE.exists():
        print(f"❌ 数据库 {DB_FILE} 不存在，请先运行 init_db.py")
        sys.exit(1)

    conn = duckdb.connect(str(DB_FILE))

    if force_rebuild:
        print("🔁 强制重建模式：删除旧表")
        conn.execute("DROP TABLE IF EXISTS kline_1m")
        conn.execute("DROP TABLE IF EXISTS kline_1d")

    # 1分钟K线表
    conn.execute("""
    CREATE TABLE IF NOT EXISTS kline_1m (
        trade_pair VARCHAR,
        open_time BIGINT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        close_time BIGINT,
        quote_asset_volume DOUBLE,
        number_of_trades BIGINT,
        taker_buy_base_volume DOUBLE,
        taker_buy_quote_volume DOUBLE,
        ignore INTEGER
    )
    """)

    # 日K线表
    conn.execute("""
    CREATE TABLE IF NOT EXISTS kline_1d (
        trade_pair VARCHAR,
        open_time BIGINT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        close_time BIGINT,
        quote_asset_volume DOUBLE,
        number_of_trades BIGINT,
        taker_buy_base_volume DOUBLE,
        taker_buy_quote_volume DOUBLE,
        ignore INTEGER
    )
    """)

    conn.close()
    print("✅ 表初始化完成")
    print("   - kline_1m  (1分钟K线)")
    print("   - kline_1d  (日K线)")


def print_usage():
    print("用法:")
    print("  python init_table.py              创建表（不存在则创建）")
    print("  python init_table.py --force/-f    强制删除并重建表")


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) == 0:
        init_tables(force_rebuild=False)
    elif len(args) == 1 and args[0] in ("--force", "-f"):
        init_tables(force_rebuild=True)
    else:
        print_usage()
        sys.exit(1)

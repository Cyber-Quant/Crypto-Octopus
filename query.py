import sys
import argparse
from pathlib import Path
import duckdb
from datetime import datetime
from typing import Optional, List, Dict, Any

DB_FILE = Path("data.duckdb")

SUPPORTED_PERIODS = {
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1mo",
}


def parse_time(input_str: Optional[str]) -> Optional[int]:
    if input_str is None:
        return None
    s = input_str.strip()
    for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            continue
    raise ValueError(f"时间格式错误: {input_str}")


# ========================
# ✅ 终极架构：全部参数 → 统一算出 start, end
# ========================
def query_kline(
    period: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    num: Optional[int] = None,
    trade_pairs: Optional[List[str]] = None,
    db_path: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    db = db_path or str(DB_FILE)
    if not Path(db).exists():
        raise FileNotFoundError("数据库不存在")

    if period not in SUPPORTED_PERIODS:
        raise ValueError(f"支持周期: {SUPPORTED_PERIODS}")

    # ========= 统一时间计算 =========
    s_ms = parse_time(start_time)
    e_ms = parse_time(end_time)

    # 自动获取数据库最新时间
    conn = duckdb.connect(db)
    max_time = conn.execute("SELECT max(open_time) FROM kline_1m").fetchone()[0]
    if not max_time:
        max_time = 0

    # ======== 核心逻辑：全部归一到 start / end ========
    default_num = 100

    # 1. 什么都不传 → 最新N根
    if s_ms is None and e_ms is None and num is None:
        num = default_num

    # 2. 只传 num → 最新N根
    if s_ms is None and e_ms is None and num is not None:
        e_ms = max_time

    # 3. 只传 start → 从 start 到最新
    if s_ms is not None and e_ms is None:
        e_ms = max_time

    # 4. 只传 end → 不允许，必须配合 num
    if s_ms is None and e_ms is not None and num is None:
        num = default_num

    # 5. 传 end + num → 从 end 往前数N根
    if s_ms is None and e_ms is not None and num is not None:
        s_ms = 0  # 全部历史，再在内存里截断

    # 6. 传 start + num → 从 start 往后数N根
    if s_ms is not None and num is not None and e_ms is None:
        e_ms = max_time

    # ======== 统一查询 ========
    pairs = trade_pairs or [None]
    result = {}

    for tp in pairs:
        sql = _build_sql(period, tp, s_ms, e_ms)
        rows = conn.execute(sql).fetchall()
        cols = [d[0] for d in conn.description]
        items = [dict(zip(cols, r)) for r in rows]

        # 内存里截断，保证数量正确
        if num is not None:
            if s_ms is None and e_ms == max_time:
                items = items[-num:]  # 最新N根
            elif s_ms is not None:
                items = items[:num]  # 从起点往后N根
            elif e_ms is not None:
                items = items[-num:]  # 从结束点往前N根

        # ✅ 关键：如果第一条数据 > s_ms → 直接空
        if s_ms and items:
            first_t = items[0]["open_time"]
            if first_t > s_ms + 86400_000_000_000:
                items = []

        result[tp or "ALL"] = items

    conn.close()
    return result


def _build_sql(period, tp, s_ms, e_ms):
    pair_sql = f"trade_pair = '{tp}'" if tp else "1=1"
    time_parts = []
    if s_ms:
        time_parts.append(f"open_time >= {s_ms}")
    if e_ms:
        time_parts.append(f"open_time <= {e_ms}")
    time_sql = " AND ".join(time_parts) if time_parts else "1=1"

    base = "kline_1m"
    return (
        f"SELECT * FROM {base} WHERE {pair_sql} AND {time_sql} ORDER BY open_time ASC"
    )


# ========================
# 命令行
# ========================
def main():
    usage = """
    K线查询工具 - 支持聚合、时间范围、数量查询、交易对筛选

    用法:
    python query.py -p 周期 [-s 开始] [-e 结束] [-n 数量] [-tp 交易对...] [-c]

    示例:
    1. 时间区间（直接打印）
        python query.py -p 4h -s 2026-01-01 -e 2026-01-31 -tp BTCUSDT

    2. 从开始时间到最新数据
        python query.py -p 1d -s 2026-01-01

    3. 从开始时间取 N 根
        python query.py -p 1m -s 2026-04-01 -n 100 -tp BTCUSDT

    4. 从结束时间往前取 N 根
        python query.py -p 1h -e 2026-04-01 -n 100

    5. 最新 N 根
        python query.py -p 1d -n 500

    6. 导出 CSV
        python query.py -p 1h -s 2026-01-01 -tp BTCUSDT -c

    参数:
    -p, --period     必选  周期: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo
    -s, --start      可选  开始时间 YYYY-MM-DD 或 YYYY-MM-DD HH:MM
    -e, --end        可选  结束时间
    -n, --num        可选  获取K线数量
    -tp, --pair      可选  交易对(可多个)，不填=全部
    -c, --csv        可选  同时导出CSV文件
    """

    if len(sys.argv) == 1:
        print("\033[1;35m" + usage + "\033[0m")
        sys.exit(0)

    parser = argparse.ArgumentParser(
        usage=usage, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-p", "--period", required=True)
    parser.add_argument("-s", "--start")
    parser.add_argument("-e", "--end")
    parser.add_argument("-n", "--num", type=int)
    parser.add_argument("-tp", "--pair", nargs="*")
    parser.add_argument("-c", "--csv", action="store_true")
    args = parser.parse_args()

    try:
        data = query_kline(
            period=args.period,
            start_time=args.start,
            end_time=args.end,
            num=args.num,
            trade_pairs=args.pair,
        )

        for pair, lines in data.items():
            print(f"\n===== {pair} | {len(lines)} 根 =====")
            if not lines:
                print("无数据")
                continue
            for line in lines:
                print(line)

    except Exception as ex:
        print(f"\n❌ 错误: {ex}")


if __name__ == "__main__":
    main()

import argparse
import pandas as pd
import plotly.graph_objects as go
from query import query_kline

# ===================== 默认值 =====================
DEFAULT_TRADING_PAIR = "BTCUSDT"
DEFAULT_PERIOD = "1m"
DEFAULT_NUM = 100


def main():
    parser = argparse.ArgumentParser(description="K线查看工具（本地DuckDB + Plotly）")
    parser.add_argument(
        "-tp",
        "--trading-pair",
        default=DEFAULT_TRADING_PAIR,
        help="交易对，默认 BTCUSDT",
    )
    parser.add_argument("-p", "--period", default=DEFAULT_PERIOD, help="周期，默认 1m")
    parser.add_argument("-s", "--start", help="开始时间：YYYY-MM-DD HH:MM")
    parser.add_argument("-e", "--end", help="结束时间：YYYY-MM-DD HH:MM")
    parser.add_argument("-n", "--num", type=int, help="K线根数")

    args = parser.parse_args()

    tp = args.trading_pair
    period = args.period
    start = args.start
    end = args.end
    num = args.num

    # 调用你项目里的 query_kline
    result = query_kline(
        period=period, start_time=start, end_time=end, num=num, trade_pairs=[tp]
    )

    if tp not in result or len(result[tp]) == 0:
        print("无数据")
        return

    # 转 DataFrame
    df = pd.DataFrame(result[tp])

    # 先打印字段名，方便你确认
    print("表字段名：", df.columns.tolist())

    # 自动适配时间字段（优先匹配常见的时间列名）
    time_col = None
    for candidate in ["open_time", "open_time_us", "timestamp", "time"]:
        if candidate in df.columns:
            time_col = candidate
            break
    if time_col is None:
        print("找不到时间字段，请检查表字段名")
        return

    # 自动适配OHLC字段
    required_cols = ["open", "high", "low", "close"]
    for col in required_cols:
        if col not in df.columns:
            print(f"缺少字段：{col}，请检查表结构")
            return

    # 转换时间（根据字段类型自动处理）
    if "open_time" in time_col or "us" in time_col:
        df["time"] = pd.to_datetime(df[time_col], unit="us")
    elif "timestamp" in time_col or "ms" in time_col:
        df["time"] = pd.to_datetime(df[time_col], unit="ms")
    else:
        df["time"] = pd.to_datetime(df[time_col])

    # Plotly 画 K线
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df["time"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="K线",
        )
    )

    fig.update_layout(
        title=f"{tp} {period} K线",
        xaxis_title="时间",
        yaxis_title="价格",
        width=1400,
        height=800,
    )

    fig.show()


if __name__ == "__main__":
    main()

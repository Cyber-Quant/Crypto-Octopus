# -*- coding: utf-8 -*-
"""
自动更新工具（干净架构版）
update → 下载 → 调用 insert 入库
不修改任何原有代码
"""

import argparse
from datetime import datetime, timedelta, timezone

# 下载器
from download_data import download_batch, SUPPORTED_PAIRS, SUPPORTED_PERIODS, DATA_DIR

# 直接调用你现成的入库函数
from insert_batch import insert_batch

# ===================== 配置 =====================
DB_LATEST_DATE = "2026-05-11"  # 后面替换成真查询
UTC_NOW = datetime.now(timezone.utc)
TARGET_DATE = (UTC_NOW - timedelta(days=1)).strftime("%Y-%m-%d")


# ===================== 工具函数 =====================
def get_db_latest_date(pair: str, period: str) -> str:
    """查询数据库最新日期"""
    return DB_LATEST_DATE


def date_diff_days(start: str, end: str) -> int:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (e - s).days


def subtract_months(dt: datetime, months: int) -> datetime:
    month = dt.month - 1 - months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return dt.replace(year=year, month=month, day=day)


def date_range(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


# ===================== 核心逻辑 =====================
def run_update(pairs, periods, backtrack=0):
    print("===== 自动更新开始 =====")
    print(f"目标日期（UTC昨天）: {TARGET_DATE}")

    # --------------------------
    # 历史回填模式
    # --------------------------
    if backtrack > 0:
        start_dt = subtract_months(UTC_NOW, backtrack)
        start_date = start_dt.strftime("%Y-%m-%d")
        print(f"历史回填: {start_date} → {TARGET_DATE}")

        for pair in pairs:
            for period in periods:
                days = date_diff_days(start_date, TARGET_DATE)
                if days > 31:
                    current = datetime.strptime(start_date, "%Y-%m-%d")
                    while current.strftime("%Y-%m-%d") <= TARGET_DATE:
                        month_str = current.strftime("%Y-%m")
                        download_batch([pair], [period], month_str, mode="monthly")
                        current = (current.replace(day=1) + timedelta(days=32)).replace(
                            day=1
                        )
                else:
                    for date_str in date_range(start_date, TARGET_DATE):
                        download_batch([pair], [period], date_str, mode="daily")

        # 下载完 → 调用入库
        print("\n✅ 下载完成，开始入库...")
        insert_batch(str(DATA_DIR))
        return

    # --------------------------
    # 正常更新模式
    # --------------------------
    for pair in pairs:
        for period in periods:
            latest = get_db_latest_date(pair, period)
            if latest >= TARGET_DATE:
                print(f"✅ {pair} {period} 已是最新")
                continue

            start_date = (
                datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            days = date_diff_days(start_date, TARGET_DATE)
            print(f"🔄 {pair} {period}: {start_date} → {TARGET_DATE}")

            if days > 31:
                current = datetime.strptime(start_date, "%Y-%m-%d")
                while current.strftime("%Y-%m-%d") <= TARGET_DATE:
                    month_str = current.strftime("%Y-%m")
                    download_batch([pair], [period], month_str, mode="monthly")
                    current = (current.replace(day=1) + timedelta(days=32)).replace(
                        day=1
                    )
            else:
                for date_str in date_range(start_date, TARGET_DATE):
                    download_batch([pair], [period], date_str, mode="daily")

    # 全部下载完 → 统一调用入库
    print("\n✅ 所有下载完成，开始自动入库...")
    insert_batch(str(DATA_DIR))
    print("\n🎉 全部完成：更新 + 入库")


# ===================== 命令行 =====================
def main():
    parser = argparse.ArgumentParser(description="自动更新 + 自动调用 insert")
    parser.add_argument("-tp", "--trading-pair", action="append")
    parser.add_argument("-p", "--period", action="append")
    parser.add_argument("-b", "--backtrack", type=int, default=0)
    args = parser.parse_args()

    pairs = args.trading_pair or SUPPORTED_PAIRS
    periods = args.period or SUPPORTED_PERIODS

    run_update(pairs, periods, args.backtrack)


if __name__ == "__main__":
    main()

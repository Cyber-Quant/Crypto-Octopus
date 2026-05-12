# -*- coding: utf-8 -*-
"""
数据自动更新工具（修复版）
- 智能补全所有缺失日期
- 支持日包/月包自动切换
- 支持历史回填
"""

import argparse
from datetime import datetime, timedelta, timezone

# 引入下载器 API
from download_data import download_batch, SUPPORTED_PAIRS, SUPPORTED_PERIODS

# ===================== 配置 =====================
DB_LATEST_DATE = "2026-05-01"  # 【这里后面你替换成真实数据库查询】
UTC_NOW = datetime.now(timezone.utc)
TARGET_DATE = (UTC_NOW - timedelta(days=1)).strftime("%Y-%m-%d")


# ===================== 工具函数 =====================
def get_db_latest_date(pair: str, period: str) -> str:
    """
    查询数据库：返回该 交易对+周期 的最新日期
    【后面你只需要改写这里，连接你的数据库即可】
    """
    print(f"📊 查询数据库: {pair} {period} -> 最新日期: {DB_LATEST_DATE}")
    return DB_LATEST_DATE


def date_diff_days(start: str, end: str) -> int:
    """计算日期相差天数"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (e - s).days


def subtract_months(dt: datetime, months: int) -> datetime:
    """原生实现：减去 N 个月"""
    month = dt.month - 1 - months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
    return dt.replace(year=year, month=month, day=day)


def date_range(start_date: str, end_date: str):
    """生成从 start 到 end 的所有日期（含首尾）"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


# ===================== 核心更新逻辑 =====================
def run_update(
    pairs: list, periods: list, backtrack_months: int = 0, force: bool = False
):
    print("===== 开始自动更新 =====")
    print(f"目标截止日期 (UTC 昨天): {TARGET_DATE}")
    print(f"交易对: {pairs}")
    print(f"周期  : {periods}")
    print(f"回填  : {backtrack_months} 个月\n")

    # 历史回填模式
    if backtrack_months > 0:
        start_dt = subtract_months(UTC_NOW, backtrack_months)
        start_date = start_dt.strftime("%Y-%m-%d")
        print(f"📜 历史回填模式: 从 {start_date} 开始下载")
        for pair in pairs:
            for period in periods:
                days = date_diff_days(start_date, TARGET_DATE)
                if days > 31:
                    # 月包模式：按月份下载
                    current = datetime.strptime(start_date, "%Y-%m-%d")
                    while current.strftime("%Y-%m-%d") <= TARGET_DATE:
                        month_str = current.strftime("%Y-%m")
                        download_batch([pair], [period], month_str, mode="monthly")
                        # 下个月
                        current = (current.replace(day=1) + timedelta(days=32)).replace(
                            day=1
                        )
                else:
                    # 日包模式：按日期下载
                    for date_str in date_range(start_date, TARGET_DATE):
                        download_batch([pair], [period], date_str, mode="daily")
        return

    # 正常更新模式
    for pair in pairs:
        for period in periods:
            latest = get_db_latest_date(pair, period)
            if latest >= TARGET_DATE:
                print(f"✅ {pair} {period} 已是最新，无需更新\n")
                continue

            print(f"🔄 需要更新: {latest} -> {TARGET_DATE}")
            start_date = (
                datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            days = date_diff_days(start_date, TARGET_DATE)

            if days <= 0:
                print(f"✅ {pair} {period} 已是最新，无需更新\n")
                continue

            if days > 31:
                # 月包模式：按月份下载
                print(f"📦 缺失超过31天，使用月包模式")
                current = datetime.strptime(start_date, "%Y-%m-%d")
                while current.strftime("%Y-%m-%d") <= TARGET_DATE:
                    month_str = current.strftime("%Y-%m")
                    download_batch([pair], [period], month_str, mode="monthly")
                    # 下个月
                    current = (current.replace(day=1) + timedelta(days=32)).replace(
                        day=1
                    )
            else:
                # 日包模式：按日期下载
                print(f"📅 缺失≤31天，使用日包模式")
                for date_str in date_range(start_date, TARGET_DATE):
                    download_batch([pair], [period], date_str, mode="daily")

    print("\n🎉 更新任务全部完成！")


# ===================== 命令行入口 =====================
def main():
    parser = argparse.ArgumentParser(
        description="""
===== Crypto Octopus 数据自动更新工具 =====
功能：
  1. 自动检查数据库 → 补全所有缺失日期
  2. 缺失 > 31 天 → 自动下载月包
  3. 缺失 ≤ 31 天 → 自动下载日包
  4. 支持历史回填（追加过去数月/数年数据）
""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # 交易对
    parser.add_argument(
        "-tp",
        "--trading-pair",
        action="append",
        help="指定交易对，可多个\n示例: -tp BTCUSDT -tp ETHUSDT\n不指定 = 全部",
    )

    # 周期
    parser.add_argument(
        "-p",
        "--period",
        action="append",
        help="指定周期: 1m / 1d，可多个\n不指定 = 全部",
    )

    # 历史回填
    parser.add_argument(
        "-b",
        "--backtrack",
        type=int,
        default=0,
        help="往前回填 N 个月历史数据\n示例: -b 6 (回填6个月), -b 12 (回填1年)",
    )

    # 强制
    parser.add_argument(
        "-f", "--force", action="store_true", help="强制重新下载，覆盖已存在文件"
    )

    args = parser.parse_args()

    # 最终参数
    pairs = args.trading_pair or SUPPORTED_PAIRS
    periods = args.period or SUPPORTED_PERIODS

    run_update(
        pairs=pairs, periods=periods, backtrack_months=args.backtrack, force=args.force
    )


if __name__ == "__main__":
    main()

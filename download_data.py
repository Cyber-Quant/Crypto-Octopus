# -*- coding: utf-8 -*-
"""
币安K线数据下载工具
✅ UTC 时间（确保数据存在）
✅ 代理 7893
✅ 已存在跳过
✅ 校验失败重下
✅ 漂亮用法
"""

import hashlib
import requests
import argparse
import urllib3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# 关闭SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置
from config import (
    SUPPORTED_PAIRS,
    SUPPORTED_PERIODS,
    DATA_DOWNLOAD_DIR,
    DOWNLOAD_TIMEOUT,
    DOWNLOAD_RETRY_TIMES,
    MAX_DOWNLOAD_WORKERS,
)

# -------------------------- 代理 --------------------------
proxies = {
    "http": "http://127.0.0.1:7893",
    "https": "http://127.0.0.1:7893",
}

# 币安数据地址
URL_TEMPLATES = {
    "daily": "https://data.binance.vision/data/spot/daily/klines/{pair}/{period}/{pair}-{period}-{date}.zip",
    "monthly": "https://data.binance.vision/data/spot/monthly/klines/{pair}/{period}/{pair}-{period}-{year}-{month}.zip",
}
CHECKSUM_SUFFIX = ".CHECKSUM"
DATA_DIR = Path(DATA_DOWNLOAD_DIR)


# 校验文件
def verify_checksum(zip_path: Path, checksum_path: Path) -> bool:
    if not zip_path.exists() or not checksum_path.exists():
        return False
    try:
        expected = checksum_path.read_text().split()[0].strip()
        sha = hashlib.sha256()
        with zip_path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha.update(chunk)
        return sha.hexdigest() == expected
    except Exception:
        return False


# 下载单个文件
def download_file(url: str, save_path: Path, desc: str) -> bool:
    for _ in range(DOWNLOAD_RETRY_TIMES):
        try:
            resp = requests.get(
                url,
                stream=True,
                timeout=DOWNLOAD_TIMEOUT,
                proxies=proxies,
                verify=False,
            )
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            with (
                save_path.open("wb") as f,
                tqdm(
                    total=total, unit="B", unit_scale=True, desc=desc, leave=False
                ) as bar,
            ):
                for chunk in resp.iter_content(8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
            return True
        except Exception as e:
            print(f"[重试] {str(e)}")
    return False


# 下载单个任务
def download_single(pair: str, period: str, date_str: str, mode: str = "daily") -> bool:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if mode == "monthly":
            dt = datetime.strptime(date_str[:7], "%Y-%m")
            year, month = dt.year, dt.month
            date_key = f"{year}-{month:02d}"
            url = URL_TEMPLATES["monthly"].format(
                pair=pair, period=period, year=year, month=month
            )
        else:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            date_key = date_str
            url = URL_TEMPLATES["daily"].format(pair=pair, period=period, date=date_key)
    except ValueError:
        print(f"❌ 日期错误: {date_str}")
        return False

    zip_path = DATA_DIR / Path(url).name
    chk_url = url + CHECKSUM_SUFFIX
    chk_path = DATA_DIR / (Path(url).name + CHECKSUM_SUFFIX)

    if zip_path.exists() and chk_path.exists():
        if verify_checksum(zip_path, chk_path):
            print(f"✅ 已存在: {zip_path.name}")
            return True
        else:
            zip_path.unlink(missing_ok=True)
            chk_path.unlink(missing_ok=True)

    print(f"⬇️  下载: {zip_path.name}")
    if not download_file(url, zip_path, f"ZIP {zip_path.name}"):
        return False
    if not download_file(chk_url, chk_path, f"CHK {chk_path.name}"):
        zip_path.unlink(missing_ok=True)
        return False

    if verify_checksum(zip_path, chk_path):
        print(f"✅ 完成: {zip_path.name}")
        return True
    else:
        print(f"❌ 校验失败: {zip_path.name}")
        zip_path.unlink(missing_ok=True)
        chk_path.unlink(missing_ok=True)
        return False


# 批量下载
def download_batch(pairs: list, periods: list, date_str: str, mode: str = "daily"):
    print(f"\n===== 开始下载任务 =====")
    print(f"交易对: {pairs}")
    print(f"周期  : {periods}")
    print(f"日期  : {date_str}")
    print(f"模式  : {mode}\n")

    # UTC 今天判断
    utc_now = datetime.now(timezone.utc)
    today_utc = utc_now.strftime("%Y-%m-%d")
    if date_str == today_utc:
        print("⚠️  警告：UTC 今天数据尚未生成！\n")

    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
        futures = [
            executor.submit(download_single, p, peri, date_str, mode)
            for p in pairs
            for peri in periods
        ]
        for f in futures:
            f.result()

    print("\n✅ 全部任务完成")


# 主函数
def main():
    # ====================== 核心：UTC 昨天 ======================
    utc_now = datetime.now(timezone.utc)
    default_date = (utc_now - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="===== 币安 K 线数据下载工具 =====\n默认：全部交易对 | 全部周期 | UTC 昨天 | 日包",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "symbols", nargs="*", default=SUPPORTED_PAIRS, help="交易对，默认全部"
    )
    parser.add_argument("-p", "--period", action="append", help="周期：1m/1d，默认全部")
    parser.add_argument(
        "-D", "--date", default=default_date, help="日期，默认 UTC 昨天"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "-d", "--daily", action="store_true", default=True, help="日包（默认）"
    )
    mode.add_argument("-m", "--monthly", action="store_true", help="月包")

    args = parser.parse_args()
    download_batch(
        pairs=args.symbols,
        periods=args.period or SUPPORTED_PERIODS,
        date_str=args.date,
        mode="monthly" if args.monthly else "daily",
    )


if __name__ == "__main__":
    main()

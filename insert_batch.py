import sys
import zipfile
import hashlib
from pathlib import Path
import duckdb
import uuid

DB_FILE = Path("data.duckdb")


def print_usage():
    print("用法:")
    print("  python insert_batch.py 目标目录")
    print("  示例: python insert_batch.py ./data")


def verify_checksum(zip_path: Path, checksum_path: Path) -> bool:
    try:
        zip_bytes = zip_path.read_bytes()
        actual_hash = hashlib.sha256(zip_bytes).hexdigest()
        expected_hash = checksum_path.read_text().strip().split()[0]
        return actual_hash.lower() == expected_hash.lower()
    except Exception:
        return False


def insert_batch(target_dir: str):
    root = Path(target_dir)
    if not root.exists() or not root.is_dir():
        print(f"❌ 目录不存在: {target_dir}")
        sys.exit(1)

    if not DB_FILE.exists():
        print(f"❌ 数据库不存在，请先运行 init_db.py")
        sys.exit(1)

    # 只处理带-1m-或-1d-的zip文件，避免无关文件干扰
    zip_files = [
        f
        for f in root.rglob("*.zip")
        if not f.name.endswith(".zip.CHECKSUM")
        and ("-1m-" in f.name or "-1d-" in f.name)
    ]
    checksum_map = {
        f.name.replace(".CHECKSUM", ""): f for f in root.rglob("*.zip.CHECKSUM")
    }

    conn = duckdb.connect(str(DB_FILE))
    print(f"✅ 开始批量处理目录: {root}")
    print("-" * 80)

    for zip_path in zip_files:
        zip_name = zip_path.name
        checksum_path = checksum_map.get(zip_name)

        if not checksum_path:
            print(f"⚠️  警告: 无校验文件 -> {zip_name}")
            continue

        if not verify_checksum(zip_path, checksum_path):
            print(f"⚠️  警告: 校验不匹配 -> {zip_name}")
            continue

        print(f"✅ 校验通过: {zip_name}")

        # 解压CSV
        csv_path = None
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    print(f"⚠️  警告: 压缩包内无CSV -> {zip_name}")
                    continue
                csv_name = csv_names[0]
                csv_path = root / csv_name
                zf.extract(csv_name, root)
        except Exception as e:
            print(f"⚠️  警告: 解压失败 -> {zip_name}: {str(e)}")
            continue

        # 解析交易对和周期
        if "-1m-" in zip_name:
            table = "kline_1m"
        elif "-1d-" in zip_name:
            table = "kline_1d"
        else:
            print(f"⚠️  警告: 不支持的周期 -> {zip_name}")
            if csv_path and csv_path.exists():
                csv_path.unlink(missing_ok=True)
            continue

        trade_pair = zip_name.split("-")[0]
        print(f"   交易对: {trade_pair}, 表: {table}")

        # 用安全的临时表名（无横杠）
        temp_table = f"tmp_import_{uuid.uuid4().hex[:8]}"
        try:
            # 1. 读取CSV到临时表（无表头）
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            conn.execute(
                f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM '{csv_path}'"
            )

            # 2. 重命名列，映射到标准字段
            conn.execute(f"""
                ALTER TABLE {temp_table} RENAME column00 TO open_time;
                ALTER TABLE {temp_table} RENAME column01 TO open;
                ALTER TABLE {temp_table} RENAME column02 TO high;
                ALTER TABLE {temp_table} RENAME column03 TO low;
                ALTER TABLE {temp_table} RENAME column04 TO close;
                ALTER TABLE {temp_table} RENAME column05 TO volume;
                ALTER TABLE {temp_table} RENAME column06 TO close_time;
                ALTER TABLE {temp_table} RENAME column07 TO quote_asset_volume;
                ALTER TABLE {temp_table} RENAME column08 TO number_of_trades;
                ALTER TABLE {temp_table} RENAME column09 TO taker_buy_base_volume;
                ALTER TABLE {temp_table} RENAME column10 TO taker_buy_quote_volume;
                ALTER TABLE {temp_table} RENAME column11 TO ignore;
            """)

            # 3. 去重插入
            conn.execute(f"""
                INSERT INTO {table}
                SELECT '{trade_pair}', * FROM {temp_table}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table}
                    WHERE trade_pair = '{trade_pair}'
                      AND open_time = {temp_table}.open_time
                )
            """)

            # 统计新增条数
            inserted_count = conn.execute(f"""
                SELECT COUNT(*) FROM {temp_table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table} k
                    WHERE k.trade_pair = '{trade_pair}'
                      AND k.open_time = t.open_time
                )
            """).fetchone()[0]

            print(
                f"   → 已入库: {csv_path.name} (新增 {inserted_count} 条，已存在的自动跳过)"
            )

        except Exception as e:
            print(f"⚠️  警告: 入库失败 {csv_path.name}: {str(e)}")
        finally:
            # 强制清理临时表和CSV
            conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            if csv_path and csv_path.exists():
                csv_path.unlink(missing_ok=True)

    conn.close()
    print("-" * 80)
    print("✅ 全部处理完成！")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 1:
        print_usage()
        sys.exit(1)
    insert_batch(args[0])

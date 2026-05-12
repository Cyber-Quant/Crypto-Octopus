import sys
from pathlib import Path
import duckdb

DB_FILE = Path("data.duckdb")


def init_db(force_rebuild: bool = False):
    # 强制重建：删除旧库
    if force_rebuild and DB_FILE.exists():
        print(f"🗑️ 强制重建：删除旧数据库 {DB_FILE}")
        DB_FILE.unlink()

    # 已存在，直接退出
    if DB_FILE.exists():
        print(f"✅ 数据库 {DB_FILE} 已存在")
        return

    # 只创建数据库文件，不建表
    print(f"🆕 创建数据库 {DB_FILE}")
    conn = duckdb.connect(str(DB_FILE))
    conn.close()
    print(f"✅ 数据库创建完成")


def print_usage():
    print("用法:")
    print("  python init_db.py          创建数据库（不存在则创建）")
    print("  python init_db.py --force/-f  强制重建数据库")


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) == 0:
        init_db(force_rebuild=False)
    elif len(args) == 1 and args[0] in ("--force", "-f"):
        init_db(force_rebuild=True)
    else:
        print_usage()
        sys.exit(1)

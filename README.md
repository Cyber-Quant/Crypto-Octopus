========================================
Crypto-Octopus - 使用文档
========================================

项目目录结构：
```
Crypto-Octopus/
  ├── init_db.py          # 初始化数据库
  ├── init_table.py       # 创建数据表（仅 1m、1d）
  ├── insert_batch.py     # 批量数据导入工具
  ├── download_data.py    # 币安K线数据下载工具（新增）
  ├── update.py           # 全自动数据更新/补全/入库（新增）
  ├── query.py            # 命令行查询 + 内部API
  ├── pyproject.toml      # 项目依赖
  └── data.duckdb         # 数据库（自动生成）
```

========================================
一、环境安装
========================================

1. 创建虚拟环境
```
python -m venv .venv
```
2. 激活虚拟环境

Windows:
```
.venv\Scripts\activate
```

Mac/Linux:
```
source .venv/bin/activate
```

3. 安装依赖（使用 pyproject.toml）
```
pip install -e .
```

========================================
二、数据库初始化
========================================

1. 初始化数据库文件
```
python init_db.py
```

2. 创建K线表
```
python init_table.py
```

仅创建两张表：
  - kline_1m
  - kline_1d

表结构：
| 字段名               | 类型    |
|----------------------|---------|
| open_time            | BIGINT  |
| open                 | DOUBLE  |
| high                 | DOUBLE  |
| low                  | DOUBLE  |
| close                | DOUBLE  |
| volume               | DOUBLE  |
| quote_volume         | DOUBLE  |
| count                | INTEGER |
| taker_base_volume    | DOUBLE  |
| taker_quote_volume   | DOUBLE  |
| trade_pair           | VARCHAR |
| ignore               | VARCHAR |


========================================
三、批量插入数据（insert_batch.py）
========================================

功能：
读取 ZIP 数据文件 + checksum 校验文件，批量写入数据库

使用规则：
1. 下载所有 K线 历史数据 ZIP 文件
2. 下载对应的 checksum 校验文件
3. 把所有文件（ZIP + checksum）全部放在 ./data 目录
4. 运行脚本指定目录

运行命令：
```bash
python insert_batch.py ./data
```

脚本自动处理：
- 自动扫描目录下所有 ZIP + checksum
- 自动校验文件完整性
- 自动识别周期 1m / 1d
- 自动去重插入
- 自动清理临时CSV文件


========================================
四、数据下载工具（download_data.py）
========================================

功能：
从币安公开数据下载地址，下载指定交易对、周期、日期的 K线 ZIP 文件及校验文件，自动做3-12秒随机延时，避免风控。

使用示例：
# 下载单文件（日数据）
```bash
python download_data.py -tp BTCUSDT -p 1m -d 2025-01-01
```

# 下载整月数据（月包）
```bash
python download_data.py -tp BTCUSDT -p 1m -m 2025-01
```

支持周期：
- 1m / 1d

API 函数：
```python
download_single(trade_pair, period, date_str, mode="daily")
```
   下载单个文件，mode 可选 "daily" / "monthly"
   自动下载 ZIP + CHECKSUM，校验并保存到 ./data 目录。

```python
download_batch(trade_pairs, periods, date_str, mode="daily")
```
   批量下载多个交易对、多个周期的指定日期/月份数据。


========================================
五、全自动数据更新/补全/入库（update.py）
========================================

功能：
自动查询 DuckDB 中最新日期，计算缺失数据范围，调用 download_data.py 下载，下载完成后自动调用 insert_batch.py 入库，全程闭环。

运行方式：
# 日常更新（补全昨天之前的所有缺失数据）
```
python update.py
```

# 仅更新指定交易对
```
python update.py -tp BTCUSDT -tp ETHUSDT
```

# 往前回填 2 个月历史数据
```
python update.py -b 2
```

参数说明：
  -tp / --trading-pair  指定交易对，可多次使用
  -p / --period         指定周期（1m/1d，默认全部）
  -b / --backtrack      往前回填的月数，默认 0（仅补到最新）

执行流程：
1. 查询数据库中各交易对/周期的最新日期
2. 计算缺失日期范围
3. 自动选择日包/月包下载
4. 调用 download_data.py 下载（自带随机延时防封）
5. 下载完成后自动调用 insert_batch.py ./data 入库
6. 自动去重、自动清理临时文件


========================================
六、命令行查询（query.py）
========================================

【重要】
存储只有 1m + 1d
但 查询支持全部周期！

支持周期：
1m, 3m, 5m, 15m, 30m,
1h, 2h, 4h, 6h, 8h, 12h,
1d, 3d, 1w, 1mo

所有周期自动从 1m 聚合生成。

参数：
-p / --period      必选（周期）
-s / --start       开始时间
-e / --end         结束时间
-n / --num         获取数量
-tp / --pair       交易对
-c / --csv         导出CSV

示例：
python query.py -p 1m
python query.py -p 3m
python query.py -p 1h
python query.py -p 4h
python query.py -p 1d
python query.py -p 1w

python query.py -p 1h -s 2026-01-01 -n 50 -tp BTCUSDT


========================================
七、Python API 内部调用（给代码使用）
========================================
直接在你的策略/脚本中调用。

支持所有周期。

示例：
```
from query import query_kline

data = query_kline(
    period="4h",
    start_time="2026-01-01",
    num=50,
    trade_pairs=["BTCUSDT"]
)

klines = data["BTCUSDT"]
```

返回格式：
```
{
  "BTCUSDT": [
    {
      "open_time": 1735689600000000,
      "open": 50000.0,
      "high": 50100.0,
      "low": 49950.0,
      "close": 50080.0
    }
  ]
}
```

========================================
八、常见问题
========================================

1. 数据库不存在
先运行：
```
python init_db.py
python init_table.py
```

3. 插入数据失败
把所有 ZIP + checksum 文件放在 ./data 目录，再运行
```
python insert_batch.py ./data
```

5. 支持哪些周期查询？
全部都支持：
1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1mo

6. 为什么只有两张表？
所有周期从 1m 自动聚合，结构最简、最稳定

7. 下载被币安限制？
download_data.py 自带 3-12 秒随机延时，请勿再额外加 sleep。

8. update.py 运行报错 AttributeError: 'Namespace' object has no attribute 'trading_pairs'
将 update.py 中 args.trading_pairs 改为 args.trading_pair（单数）即可。

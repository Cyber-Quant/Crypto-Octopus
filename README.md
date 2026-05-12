========================================
Crypto-Octopus - 使用文档
========================================

项目目录结构：
```markdown
Crypto-Octopus/
  ├── init_db.py          # 初始化数据库
  ├── init_table.py       # 创建数据表（仅 1m、1d）
  ├── insert_batch.py     # 批量数据导入工具
  ├── query.py            # 命令行查询 + 内部API
  ├── pyproject.toml      # 项目依赖
  └── data.duckdb         # 数据库（自动生成）
```

========================================
一、环境安装
========================================

1. 创建虚拟环境
```bash
python -m venv .venv
```

2. 激活虚拟环境

Windows:
```bash
.venv\Scripts\activate
```

Mac/Linux:
```bash
source .venv/bin/activate
```

3. 安装依赖（使用 pyproject.toml）
```bash
pip install -e .
```


========================================
二、数据库初始化
========================================

1. 初始化数据库文件
```bash
python init_db.py
```
2. 创建K线表
```bash
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
3. **把所有文件（ZIP + checksum）全部放在脚本所在目录**
4. **直接运行，不需要任何参数**

运行命令：
```bash
python insert_batch.py ${DATA_DIR}
```

脚本自动处理：
- 自动扫描当前目录下所有 ZIP + checksum
- 自动校验文件完整性
- 自动识别周期 1m / 1d
- 自动去重
- 批量写入数据库


========================================
四、命令行查询（query.py）
========================================

【重要】
存储只有 1m + 1d
但 **查询支持全部周期**！

支持周期：
1m, 3m, 5m, 15m, 30m,
1h, 2h, 4h, 6h, 8h, 12h,
1d, 3d, 1w, 1mo

所有周期自动从 1m 聚合生成。

---
参数：
-p / --period      必选（周期）
-s / --start       开始时间
-e / --end         结束时间
-n / --num         获取数量
-tp / --pair       交易对
-c / --csv         导出CSV

---
示例：
python query.py -p 1m
python query.py -p 3m
python query.py -p 1h
python query.py -p 4h
python query.py -p 1d
python query.py -p 1w

```bash
python query.py -p 1h -s 2026-01-01 -n 50 -tp BTCUSDT
```

========================================
五、Python API 内部调用（给代码使用）
========================================

不是 Web API！
直接在你的策略/脚本中调用。

支持所有周期。

示例：
```python
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
六、常见问题
========================================

1. 数据库不存在
先运行：
```
python init_db.py
python init_table.py
```

2. 插入数据失败
把所有 ZIP + checksum 文件放在脚本同一目录

3. 支持哪些周期查询？
全部都支持：
1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1mo

4. 为什么只有两张表？
所有周期从 1m 自动聚合，结构最简、最稳定

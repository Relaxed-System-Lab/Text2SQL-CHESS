# 在 Workflow 中添加 SQL 计时的实践指南

## 🎯 理解代码流程

你的项目中SQL执行主要发生在以下位置：

```
main.py
  └─> RunManager.run_tasks()
       └─> RunManager.worker(task)
            └─> team.stream()  # LangGraph workflow
                 └─> Various agents call execute_sql()
                      └─> database_utils/execution.py: execute_sql()
```

关键点：
- `execute_sql()` 在 `src/database_utils/execution.py` 定义
- 在多个地方被调用：`db_info.py`, `sql_parser.py`, `schema_generator.py`, `database_manager.py` 等
- 已被更新支持 `query_id` 参数进行自动计时

## 🔧 三个集成方案

### 方案A: 最小改动（推荐）

**只需修改 `src/main.py`**

```python
# main.py 顶部导入
from sql_execution_timer import SQLExecutionTimer
from database_utils.execution import set_sql_timer

def main():
    args = parse_arguments()
    dataset = load_dataset(args.data_path)

    # 添加这4行
    timer = SQLExecutionTimer()
    set_sql_timer(timer)
    
    # 原有代码不变
    run_manager = RunManager(args)
    run_manager.initialize_tasks(dataset)
    run_manager.run_tasks()
    run_manager.generate_sql_files()
    
    # 添加这3行用于导出
    timer.dump_to_csv()
    timer.dump_to_json()
    timer.print_summary()
```

**优点**:
- 只需修改main.py
- 所有execute_sql()自动计时
- 无需修改其他代码

**效果**:
- 所有SQL查询都会被计时（包括schema生成、检查等）
- 得到完整的执行时间统计

---

### 方案B: 精细控制

**在关键的agent/tool中添加query_id**

如果你想要更精细的控制，可以在调用 `execute_sql()` 时添加 `query_id`：

```python
# 示例1: 在database_utils/sql_parser.py中
from database_utils.execution import execute_sql

def check_sql_validity(db_path, sql):
    # 添加query_id参数
    result = execute_sql(
        db_path,
        f"EXPLAIN QUERY PLAN {sql}",
        "one",
        query_id="sql_validity_check"
    )
    return result

# 示例2: 在agents中执行生成的SQL
def execute_generated_sql(db_path, question_id, step_number, sql):
    # 使用具有描述性的query_id
    query_id = f"q{question_id}_s{step_number}_generated"
    result = execute_sql(
        db_path,
        sql,
        query_id=query_id
    )
    return result
```

**优点**:
- 精确控制哪些查询被计时
- query_id帮助追踪具体查询
- 易于后续分析

**缺点**:
- 需要修改多个文件

---

### 方案C: 高级定制

**创建wrapper函数简化使用**

```python
# src/database_utils/execution_with_timing.py

from database_utils.execution import execute_sql, get_sql_timer

class TimedQueryExecutor:
    """为execute_sql提供计时和日志的wrapper"""
    
    def __init__(self, question_id, db_id):
        self.question_id = question_id
        self.db_id = db_id
        self.step_count = 0
    
    def execute(self, db_path, sql, label="query", fetch="all"):
        """执行SQL并自动生成query_id"""
        self.step_count += 1
        query_id = f"q{self.question_id}_s{self.step_count}_{label}"
        
        return execute_sql(
            db_path=db_path,
            sql=sql,
            fetch=fetch,
            query_id=query_id
        )

# 使用方式
# 在agent中：
executor = TimedQueryExecutor(question_id=task.question_id, db_id=task.db_id)
result = executor.execute(db_path, sql, label="select_tables")
```

---

## 📊 具体示例

### 例1: 在 schema_generator.py 中添加计时

```python
# src/database_utils/schema_generator.py

from database_utils.execution import execute_sql

def get_table_columns(db_path, table_name):
    # 改这里 - 添加query_id
    col_info = execute_sql(
        db_path,
        f"PRAGMA table_info(`{table_name}`)",
        fetch="all",
        query_id=f"schema_table_columns_{table_name}"  # 添加
    )
    return col_info
```

### 例2: 在 sql_parser.py 中添加计时

```python
# src/database_utils/sql_parser.py

def validate_sql_syntax(db_path, sql):
    try:
        result = execute_sql(
            db_path,
            f"EXPLAIN QUERY PLAN {sql}",
            "one",
            query_id="sql_syntax_validation"  # 添加
        )
        return True
    except:
        return False
```

### 例3: 在 database_manager.py 中添加计时

```python
# src/runner/database_manager.py

def get_execution_status(self, sql):
    """获取SQL执行状态"""
    try:
        predicted_res = execute_sql(
            self.db_path,
            sql,
            query_id="execution_status_check"  # 添加
        )
        # ... 后续逻辑
    except Exception as e:
        pass
```

---

## 🎯 推荐的集成步骤

### 第1步: 基础集成（5分钟）
```python
# 在 src/main.py 中添加

from sql_execution_timer import SQLExecutionTimer
from database_utils.execution import set_sql_timer

def main():
    # ... 解析参数 ...
    
    # 初始化timer
    timer = SQLExecutionTimer(log_dir="./results/sql_timings")
    set_sql_timer(timer)
    
    # ... 运行任务 ...
    
    # 导出结果
    timer.dump_to_csv()
    timer.print_summary()
```

### 第2步: 运行并观察结果（5分钟）
```bash
bash run/run_main_ir_cg_ut.sh

# 查看输出
cat results/sql_timings/sql_execution_summary.json
cat results/sql_timings/sql_execution_records.csv
```

### 第3步: 分析数据（可选）
```python
import pandas as pd

df = pd.read_csv("results/sql_timings/sql_execution_records.csv")
print(f"平均执行时间: {df['duration_ms'].mean():.2f}ms")
print(f"最长查询:\n{df.nlargest(5, 'duration_ms')[['query_id', 'duration_ms']]}")
```

### 第4步: 精细化（如需要）
- 添加query_id到关键execute_sql()调用
- 创建自定义统计报告
- 集成到CI/CD监控

---

## 📈 分析示例

运行后，可以做以下分析：

```python
import pandas as pd
import json

# 读取数据
df = pd.read_csv("results/sql_timings/sql_execution_records.csv")

# 1. 总体统计
print(f"总查询数: {len(df)}")
print(f"成功: {df['success'].sum()}")
print(f"失败: {(~df['success']).sum()}")

# 2. 性能分析
print(f"\n性能统计:")
print(f"平均: {df['duration_ms'].mean():.2f}ms")
print(f"中位数: {df['duration_ms'].median():.2f}ms")
print(f"最大: {df['duration_ms'].max():.2f}ms")
print(f"最小: {df['duration_ms'].min():.2f}ms")

# 3. 找出最慢的查询
print(f"\n最慢的10个查询:")
print(df.nlargest(10, 'duration_ms')[['query_id', 'duration_ms', 'success']])

# 4. 分析失败查询
failed = df[~df['success']]
print(f"\n失败查询:")
print(failed[['query_id', 'error_msg']])

# 5. 时间分布
print(f"\n执行时间分布:")
print(df['duration_ms'].describe())
```

---

## 🔍 故障排除

### 问题1: 看不到任何query_id记录
**原因**: 没有设置全局timer或没有调用execute_sql
**解决**: 
1. 检查是否调用了 `set_sql_timer(timer)`
2. 检查execute_sql()是否被调用
3. 查看 `sql_execution_times.log` 确认是否有日志

### 问题2: CSV文件为空
**原因**: 没有任何SQL被执行
**解决**: 
1. 运行脚本，确保有SQL查询
2. 检查 `sql_execution_times.log`

### 问题3: 计时不准确
**原因**: Python的time模块精度有限
**解决**: 
- 查看毫秒级别的数据
- 多次运行取平均值

---

## 📝 最佳实践

1. **命名query_id的约定**:
   ```
   {query_type}_{question_id}_{step_number}
   
   示例:
   - schema_generation_q1_s1
   - sql_validation_q1_s2
   - generated_sql_q1_s3
   - evaluation_q1_s4
   ```

2. **定期导出数据**:
   ```python
   # 在每个checkpoint导出
   timer.dump_to_csv(f"checkpoint_{step}.csv")
   ```

3. **监控性能趋势**:
   ```python
   # 比较不同运行的性能
   df1 = pd.read_csv("run1_timings.csv")
   df2 = pd.read_csv("run2_timings.csv")
   
   print(f"平均性能差异: {(df2['duration_ms'].mean() - df1['duration_ms'].mean())/df1['duration_ms'].mean()*100:.1f}%")
   ```

---

## ✅ 集成检查清单

- [ ] 在 `main.py` 导入 `SQLExecutionTimer` 和 `set_sql_timer`
- [ ] 创建 timer 实例
- [ ] 调用 `set_sql_timer(timer)` 注册全局timer
- [ ] 在脚本末尾添加导出代码
- [ ] 修改 `run_main_ir_cg_ut.sh` 使用 `mini_dev.json`
- [ ] 测试运行脚本
- [ ] 查看 `results/sql_timings/` 目录下的输出
- [ ] 验证CSV文件中有数据

---

## 📞 快速参考

| 场景 | 代码 |
|------|------|
| 初始化 | `timer = SQLExecutionTimer()` |
| 设置全局 | `set_sql_timer(timer)` |
| 自动计时 | `execute_sql(..., query_id="my_id")` |
| 手动计时 | `timer.start(id); ...; timer.end(id)` |
| 导出CSV | `timer.dump_to_csv()` |
| 导出JSON | `timer.dump_to_json()` |
| 打印摘要 | `timer.print_summary()` |
| 获取统计 | `stats = timer.get_statistics()` |

---

**建议**: 先使用方案A（最小改动），运行后查看效果。如果需要更细粒度的控制，再考虑方案B或C。

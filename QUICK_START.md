# 🚀 30秒快速开始

## 修改 `src/main.py`

在 `main()` 函数中添加：

```python
# 顶部导入
from sql_execution_timer import SQLExecutionTimer
from database_utils.execution import set_sql_timer

def main():
    args = parse_arguments()
    dataset = load_dataset(args.data_path)
    
    # 添加这3行
    timer = SQLExecutionTimer()
    set_sql_timer(timer)
    
    # 原有代码
    run_manager = RunManager(args)
    run_manager.initialize_tasks(dataset)
    run_manager.run_tasks()
    run_manager.generate_sql_files()
    
    # 添加这3行
    timer.dump_to_csv()
    timer.dump_to_json()
    timer.print_summary()
```

## 运行

```bash
bash run/run_main_ir_cg_ut.sh
```

## 查看结果

```bash
cat results/sql_timings/sql_execution_summary.json
```

---

**完成！** ✅

更多细节见：[QUICK_REFERENCE.md](QUICK_REFERENCE.md)

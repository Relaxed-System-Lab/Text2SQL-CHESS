import argparse
import yaml
import json
import os
from datetime import datetime
from typing import Any, Dict, List

from runner.run_manager import RunManager
from sql_execution_timer import SQLExecutionTimer
from database_utils.execution import set_sql_timer

def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Run the pipeline with the specified configuration.")
    parser.add_argument('--data_mode', type=str, required=True, help="Mode of the data to be processed.")
    parser.add_argument('--data_path', type=str, required=True, help="Path to the data file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration file.")
    parser.add_argument('--num_workers', type=int, default=1, help="Number of workers to use.")
    parser.add_argument('--log_level', type=str, default='warning', help="Logging level.")
    parser.add_argument('--pick_final_sql', type=bool, default=False, help="Pick the final SQL from the generated SQLs.")
    args = parser.parse_args()

    args.run_start_time = datetime.now().isoformat()
    with open(args.config, 'r') as file:
        args.config=yaml.safe_load(file)
    
    return args

def load_dataset(data_path: str) -> List[Dict[str, Any]]:
    """
    Loads the dataset from the specified path.

    Args:
        data_path (str): Path to the data file.

    Returns:
        List[Dict[str, Any]]: The loaded dataset.
    """
    with open(data_path, 'r') as file:
        dataset = json.load(file)
    return dataset

def main():
    """
    Main function to run the pipeline with the specified configuration.
    """
    args = parse_arguments()
    dataset = load_dataset(args.data_path)

    sql_timer = SQLExecutionTimer(
        log_dir="./results/sql_timings",
        log_to_console=True  # 在控制台显示计时信息
    )
    # 全局注册timer（这样execute_sql可以自动使用它）
    set_sql_timer(sql_timer)

    run_manager = RunManager(args)
    run_manager.initialize_tasks(dataset)
    run_manager.run_tasks()
    run_manager.generate_sql_files()

    # 打印摘要到控制台
    print("\n" + "="*60)
    print("生成 SQL 执行时间报告...")
    print("="*60)
    sql_timer.print_summary()

    # # 导出为CSV（方便在Excel中打开和分析）
    # csv_path = sql_timer.dump_to_csv("sql_execution_records.csv")
    # # 导出为JSON（所有详细记录）
    # json_path = sql_timer.dump_to_json("sql_execution_records.json")
    # # 导出统计摘要
    # summary_path = sql_timer.dump_summary("sql_execution_summary.json")
    print("="*60 + "\n")

if __name__ == '__main__':
    main()

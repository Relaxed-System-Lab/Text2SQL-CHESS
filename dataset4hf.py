import os
import re
import csv

def parse_log_file(log_file_path):
    with open(log_file_path, 'r') as file:
        log_content = file.read()
    
    steps = []
    step_pattern = re.compile(r'##############################\s*(Human|AI) at step (.*?)\s*##############################')
    token_pattern = re.compile(r'######The token count is:\s*(\d+)######')
    content_pattern = re.compile(r'######The token count is:\s*\d+######\n\n(.*?)\n\n(?=##############################|$)', re.DOTALL)
    
    headers = list(step_pattern.finditer(log_content))
    
    for i, header in enumerate(headers):
        role = header.group(1)
        step_name = header.group(2).strip()
        
        start_index = header.end()
        end_index = headers[i+1].start() if i + 1 < len(headers) else len(log_content)
        section_text = log_content[start_index:end_index]
        
        token_match = token_pattern.search(section_text)
        tokens = int(token_match.group(1)) if token_match else 0
        
        content_match = content_pattern.search(section_text)
        content = content_match.group(1).strip() if content_match else ""
        
        if role == "Human":
            steps.append({
                "step": step_name,
                "input_content": content,
                "output_content": "",
                "input_length": tokens,
                "output_length": 0
            })
        else:  # role == "AI"
            if steps and steps[-1]["step"] == step_name and steps[-1]["output_length"] == 0:
                steps[-1]["output_content"] = content
                steps[-1]["output_length"] = tokens
            else:
                steps.append({
                    "step": step_name,
                    "input_content": "",
                    "output_content": content,
                    "input_length": 0,
                    "output_length": tokens
                })
    
    return steps

def collect_logs(logs_directory):
    logs_data = []
    request_id_counter = 1
    
    for log_file in os.listdir(logs_directory):
        if log_file.endswith("formula_1.log") or log_file.endswith("financial.log"):
            log_file_path = os.path.join(logs_directory, log_file)
            steps = parse_log_file(log_file_path)
            log_name = f'Text2SQLRequest_{request_id_counter}'
            logs_data.extend([{
                "Text2SQLRequest_id": log_name,
                "step_name": step["step"],
                "input_content": step["input_content"],
                "output_content": step["output_content"],
                "input_length": step["input_length"],
                "output_length": step["output_length"]
            } for step in steps])
            request_id_counter += 1
    
    return logs_data

def save_to_csv(data, output_file_path):
    fieldnames = ["Text2SQLRequest_id", "step_name", "input_content", "output_content", "input_length", "output_length"]
    with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in data:
            # Ensure proper escaping of special characters
            row["input_content"] = row["input_content"].replace('\n', '\\n').replace('\r', '\\r')
            row["output_content"] = row["output_content"].replace('\n', '\\n').replace('\r', '\\r')
            writer.writerow(row)

def main():
    logs_directories = [
        './results/dev/CHESS_IR_CG_UT/mixed_dev_1/2025-04-08T12:58:08.154132/logs',
        './results/dev/CHESS_IR_CG_UT/financial_dev/2025-04-06T13:35:21.873213/logs'
    ]
    
    all_logs_data = []
    for directory in logs_directories:
        all_logs_data.extend(collect_logs(directory))
    
    output_file_path = './text2sql_trace.csv'
    save_to_csv(all_logs_data, output_file_path)

if __name__ == "__main__":
    main()
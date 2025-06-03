import csv
import sqlite3
import json
import os
import sys
from pathlib import Path

def import_csv_to_sqlite(csv_dir):
    # Read configuration file (adapted for new directory structure)
    config_path = Path(__file__).parent.parent / 'config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Connect to SQLite database
    if not config.get('database_path'):
        raise ValueError("Database path not specified in config.json")
    # create database directory if it doesn't exist
    if not Path(config['database_path']).parent.exists():
        Path(config['database_path']).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config['database_path'])
    cursor = conn.cursor()
    total_files = 0
    total_records = 0

    if not csv_dir:
        csv_dir = Path(config.get('all_dir', 'public/all'))
    else:
        csv_dir = Path(csv_dir)
    index = 1

    while True:
        csv_path = csv_dir / f"{index}.csv"
        if not csv_path.exists():
            break
        print(f"Processing file: {csv_path}")

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # 清洗字段名：去除前后空格并转为小写
            fieldnames = [fld.strip().lower() for fld in reader.fieldnames]
            # 确保每个CSV都有link字段
            if 'link' not in fieldnames:
                raise ValueError(f"CSV file {csv_path} is missing required 'link' field")

            # 检查表是否存在
            table_exists = cursor.execute(f"""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='items'
            """).fetchone()

            has_data = False
            final_columns = []
            # 动态创建表（如果不存在）
            if not table_exists:
                # 确保link字段存在
                if 'link' not in fieldnames:
                    raise ValueError("CSV files must contain a 'link' field")

                # 显式指定link为主键并添加timestamp字段
                columns = []
                for fld in fieldnames:
                    if fld.lower() == 'timestamp':  # 兼容字段名大小写
                        columns.append(f'"{fld}" INTEGER')
                    else:
                        columns.append(f'"{fld}" TEXT')
                columns.append('PRIMARY KEY ("link")')
                columns = ', '.join(columns)
                cursor.execute(f"""
                    CREATE TABLE items (
                        {columns}
                    )
                """)
                # 初始化最终列名为CSV字段
                final_columns = fieldnames
            else:
                # 检查是否有现有数据
                has_data = cursor.execute("SELECT COUNT(*) FROM items").fetchone()[0] > 0

                # 获取数据库列（排除rowid）
                db_columns = [col[1].lower() for col in cursor.execute("PRAGMA table_info(items)") if col[1].lower() != 'rowid']

                # 当数据库有数据时，合并列
                if has_data:
                    # 取列并集
                    merged_columns = list(set(db_columns) | set(fieldnames))
                    # 确保link字段存在
                    if 'link' not in merged_columns:
                        raise ValueError("合并后的列必须包含link字段")
                    # 按数据库列顺序保持兼容性
                    final_columns = [col for col in db_columns if col in merged_columns] + \
                                   [col for col in merged_columns if col not in db_columns]
                else:
                    final_columns = fieldnames

                # 检查并添加CSV中有但数据库缺少的列
                for fld in fieldnames:
                    if fld not in db_columns:
                        cursor.execute(f'''
                            ALTER TABLE items
                            ADD COLUMN "{fld}" TEXT
                        ''')
                        print(f"Added new column: {fld}")

                # 提交表结构变更
                conn.commit()

            # 插入数据
            placeholders = ', '.join(['?'] * len(final_columns))
            insert_sql = f"""
                INSERT OR REPLACE INTO items ({','.join([f'"{col}"' for col in final_columns])})
                VALUES ({placeholders})
            """

            for row in reader:
                # 构建完整值列表（处理数据库有但CSV没有的列）
                # 处理URL字段并做空值处理
                def format_url(value, field):
                    if field in {'link', 'rss', 'home'} and value.endswith('/'):
                        return value.rstrip('/')
                    return value

                values = []
                for fld in fieldnames:
                    raw_value = row.get(fld.strip().lower(), '')
                    # 统一处理URL格式
                    if fld in {'link', 'rss', 'home'}:
                        processed_value = format_url(raw_value.strip(), fld)
                    else:
                        processed_value = raw_value.strip()
                    values.append(processed_value if processed_value else '')

                # 补充数据库有但CSV没有的列的空值
                if has_data and final_columns != fieldnames:
                    full_values = []
                    for col in final_columns:
                        if col in fieldnames:
                            full_values.append(values[fieldnames.index(col)])
                        else:
                            full_values.append('')  # 填充空字符串保持类型兼容
                    values = full_values

                cursor.execute(insert_sql, values)

            total_files += 1
            total_records += reader.line_num - 1  # 减去标题行
            index += 1
            # 每处理完一个文件提交一次
            conn.commit()

    conn.close()
    print(f"Import completed: {total_files} CSV files processed, {total_records} records imported")

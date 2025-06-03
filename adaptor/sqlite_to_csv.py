import time
from pathlib import Path
import os
import json
import csv
import sqlite3
from collections import defaultdict
import hashlib
import openai
import shutil


class Tag():
    tags = [
        # 技术类
        "编程开发",
        "系统架构",
        "人工智能",

        # 计算机基础
        "计算机科学",
        "网络通信",
        "数据技术",

        # 职业发展
        "职场管理",
        "效率工具",

        # 生活领域
        "理财投资",
        "健康生活",
        "旅行美食",
        "艺术文化",

        # 知识领域
        "人文历史",
        "哲学心理",
        "商业经济",
        "数学逻辑",

        # 内容形式
        "技术文档",
        "观点评论"
    ]


    @classmethod
    def generate_messages(cls, titles):
        seperator = "#"*10
        prompt = f"""请根据文章标题分析最相关的标签. 你会发挥自己的想象, 通过文章标题想象文章的内容.

<标签开始>
{cls._format_tags()}
<标签结束>

你需要按照2个步骤来完成这个任务:

<步骤1> 针对每个标题, 想象文章的内容，用最多20个字来描述想象的内容。
<步骤2> 根据想象的内容, 从上面的标签中选择最多3个最相关的标签.

步骤2的输出作为最终的结果.

注意: 你只能选择上面提供的标签, 不能添加新的标签.

<步骤1的返回开始>
<文章标题>: <想象的内容>
<步骤1的返回结束>

<步骤2的返回开始>
必须按照json格式返回结果. json包含的字段：
1. "<编号>": 文章标题的标签列表, 其中<编号>是从1开始的数字, 对应文章标题的顺序，其value是一个包含标签序号的列表, 最多3个标签. 禁止包含
<步骤2的返回结束>

{seperator}之间的内容是需要处理的标题。注意，不包含{seperator}自身的内容。

确保每一个输入的标题都被处理.

用户一次性给你输入{len(titles)}个标题, 确保每一个标题都被处理.>

注意: <步骤2输出>你只能按照json格式返回, 不要包含其他内容。

注意: 按照用户的指导执行<步骤1>或者<步骤2>。

待处理标题：
"""
        user_input = '\n'.join(f"{i}. {title}" for i,
                               title in enumerate(titles))
        return [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"""{seperator}
0. 机器学习的未来
1. 如何使用Python进行数据分析
{seperator}现在开始步骤<1>"""},
            {"role": "assistant", "content": f"""
机器学习的未来: 机器学习的最新发展和应用
如何使用Python进行数据分析: 使用Python进行数据分析的技巧和工具
"""},
            {"role": "user", "content": f"""现在开始步骤<2>"""},
            {"role": "assistant", "content": f"""
{{
"0": [2],
"1": [0，5]
}}
"""},
            {"role": "user", "content": f"{seperator}\n{user_input}\n{seperator}现在开始<步骤1>"}
        ]

    @classmethod
    def parse_response(cls, response, titles):
        """解析LLM响应，返回标签列表的列表"""
        results = [[] for _ in titles]
        tags_list = json.loads(response)

        for key, value in tags_list.items():
            if not key.isdigit():
                raise ValueError(f"响应中'{key}'字段不是数字")
            if int(key) >= len(titles):
                raise ValueError(
                    f"响应中'{key}'字段超出标题范围: {int(key)} >= {len(titles)}")
            if int(key) < 0:
                raise ValueError(
                    f"响应中'{key}'字段小于0: {int(key)} < 0")
            if not isinstance(value, list):
                raise ValueError(f"响应中'{key}'字段的值不是列表")

            tags = []
            for index in value:
                if not isinstance(index, int):
                    raise ValueError(f"响应中'{key}'字段的值不是整数")
                if index < 0 or index >= len(cls.tags):
                    raise ValueError(f"响应中'{key}'字段超出标签范围: {index}")
                tags.append(index)
            print(
                f"标题: {titles[int(key)]}, 标签: {', '.join(cls.tags[i] for i in tags)}")
            results[int(key)] = tags

        return results

    @classmethod
    def _format_tags(cls):
        """格式化标签列表为带序号的字符串"""
        return '\n'.join(
            f"{i}. {tag}"
            for i, tag in enumerate(cls.tags)
        )

    @classmethod
    def get_tags_for_titles(cls, titles, llm_callback, batch_size=10):
        """批量获取标签的主方法

        :param titles: 标题列表
        :param llm_callback: 接收prompt返回响应的回调函数
        :param batch_size: 每批处理数量（默认10）
        :return: 标签列表的列表，与输入标题一一对应
        """
        all_tags = []
        for i in range(0, len(titles), batch_size):
            error_message = ""
            for tried_idx in range(3):
                try:
                    batch = titles[i:i+batch_size]
                    if error_message:
                        messages.extend(
                            [{"role": "assistant", "content": response},
                             {"role": "user", "content": error_message + "\n重新生成<步骤2>。"}])
                    if tried_idx == 0:
                        messages = cls.generate_messages(batch)
                        response = llm_callback(messages)
                        messages.extend(
                            [{"role": "assistant", "content": response},
                             {"role": "user", "content": "现在开始<步骤2>"}])
                        response = llm_callback(messages)
                    else:
                        response = llm_callback(messages)
                    tags = cls.parse_response(response, batch)
                    all_tags.extend(tags)
                    break
                except (ValueError, json.JSONDecodeError) as e:
                    error_message = f"解析错误: {e}"
                    if tried_idx == 2:
                        raise
                    time.sleep(1)
        return all_tags


class BatchWriter:
    def __init__(self, output_dir, columns, batch_size, batch_file_name):
        self.output_dir = output_dir
        self.columns = columns
        self.batch_size = batch_size
        self.batch_file_name = batch_file_name
        self.current_batch = []
        self.batch_count = 0
        output_dir.mkdir(parents=True, exist_ok=True)

    def add_row(self, row):
        self.current_batch.append(row)
        if len(self.current_batch) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.current_batch:
            return
        self.batch_count += 1
        output_path = self.output_dir / f"{self.batch_count}.csv"
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.columns)
            writer.writerows(self.current_batch)
        self.current_batch = []

    def finalize(self):
        self.flush()
        # Ensure directory exists before writing batch file
        self.output_dir.mkdir(parents=True, exist_ok=True)
        batch_file_path = self.output_dir / self.batch_file_name
        with open(batch_file_path, 'w', encoding='utf-8') as f:
            f.write(f"{self.batch_count}")
        print(f"Created batch file: {batch_file_path}")  # Add debug log


def valid_date(date_str):
    """Check if the date string is in yyyy-mm-dd format"""
    try:
        time.strptime(date_str, "%Y-%m-%d")
        year, month, _ = map(int, date_str.split('-'))
        # Check if the year is in the range 2000 to current year + 1
        current_year = time.localtime().tm_year
        if not (2000 <= year <= (current_year + 1)):
            return False
        # Check if the month is in the range 1 to 12
        if not (1 <= month <= 12):
            return False
        return True
    except ValueError:
        return False


def process_date(date_str):
    """Convert yyyy-mm-dd format to yyyy/mm directory structure"""
    if not valid_date(date_str):
        print(f"Invalid date format: {date_str}")
        return "invalid_date"

    try:
        # Strict date format validation
        if len(date_str) != 10 or date_str[4] != '-' or date_str[7] != '-':
            raise ValueError(f"Invalid date format: {date_str}")

        year = date_str[:4]
        month = date_str[5:7]

        return f"{year}{month}"  # Generate hierarchical directory structure

    except Exception as e:
        print(f"Date processing error for {date_str}: {str(e)}")
        return "invalid_date"


class GroupWriter(BatchWriter):
    def __init__(self, base_dir, columns, batch_size, key_extractor, batch_file_name):
        super().__init__(base_dir, columns, batch_size, batch_file_name)
        self.key_extractor = key_extractor
        self.groups = defaultdict(lambda: {'batch': [], 'count': 0})
        self.group_count = 0
        self.field_cache = {col: i for i, col in enumerate(columns)}

    def get_group_key(self, row):
        try:
            return self.key_extractor(row)
        except (IndexError, KeyError, ValueError) as e:
            print(f"Error extracting group key: {e}")
            return None

    def add_row(self, group_key, row):
        if not group_key:
            return

        group = self.groups[group_key]
        group['batch'].append(row)
        if len(group['batch']) >= self.batch_size:
            self.flush_group(group_key)

    def flush_group(self, group_key):
        group = self.groups[group_key]
        if not group['batch']:
            return

        group['count'] += 1
        group_dir = self.output_dir / group_key
        group_dir.mkdir(parents=True, exist_ok=True)

        output_path = group_dir / f"{group['count']}.csv"
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.columns)
            writer.writerows(group['batch'])

        batch_file = group_dir / self.batch_file_name
        with open(batch_file, 'w', encoding='utf-8') as f:
            f.write(f"{group['count']}")

        group['batch'] = []
        self.group_count += 1

    def finalize(self):
        for group_key in list(self.groups.keys()):
            self.flush_group(group_key)


class MemberWriter(BatchWriter):
    def __init__(self, output_dir, columns, batch_size, batch_file_name):
        super().__init__(output_dir, columns, batch_size, batch_file_name)
        self.seen_homes = {}
        self.unique_count = 0
        self.home_idx = columns.index('home')  # Pre-store home field index
        self.date_idx = columns.index('date')  # Pre-store date field index

    def get_home_url(self, row):
        try:
            return row[self.home_idx]
        except IndexError:
            print(f"Home field missing in row: {row[:3]}...")
            return None

    def add_row(self, row):
        home_url = row[self.home_idx]
        if home_url and home_url not in self.seen_homes:
            self.seen_homes[home_url] = row
            self.unique_count += 1
        # update the latest date
        if home_url in self.seen_homes:
            if row[self.date_idx] > self.seen_homes[home_url][self.date_idx]:
                self.seen_homes[home_url] = row

    def finalize(self):
        sorted_homes = sorted(self.seen_homes.values(),
                              key=lambda x: x[self.date_idx], reverse=True)
        for row in sorted_homes:
            super().add_row(row)
        return super().finalize()


def export_sqlite_to_csv():
    # Read config file
    config_path = Path(__file__).parent.parent / 'config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 新增LLM配置参数校验
    llm_config = config.get('llm', {})
    if not llm_config.get('api_key'):
        raise ValueError("Missing LLM API key in config.json")

    # Get config parameters
    db_path = config['database_path']
    all_dir = Path(config['export']['all_dir'])
    date_dir = Path(config['export']['date_dir'])
    batch_size = config['export']['batch_size']
    batch_file_name = config['export']['batch_file']
    source_dir = Path(config['export']['source_dir'])
    member_dir = Path(config['export']['member_dir'])

    # clear dirs
    for dir_path in [all_dir, date_dir, source_dir, member_dir]:
        if dir_path.exists():
            print(f"Clearing directory: {dir_path}")
            shutil.rmtree(dir_path)

    # Create output directories
    all_dir.mkdir(parents=True, exist_ok=True)
    date_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)
    member_dir.mkdir(parents=True, exist_ok=True)

    # Connect to database and check table exists
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 添加tags字段（如果不存在）
    cursor.execute("PRAGMA table_info(items)")
    columns_info = cursor.fetchall()
    if not any(col[1] == 'tags' for col in columns_info):
        print("Adding tags column to database...")
        cursor.execute("ALTER TABLE items ADD COLUMN tags TEXT DEFAULT ''")
        conn.commit()

    # Check table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
    if not cursor.fetchone():
        raise ValueError(
            f"Table 'items' not found in database {db_path}, please import data first")

    # Get total count first
    cursor.execute("SELECT COUNT(*) FROM items")
    total_count = cursor.fetchone()[0]

    # Get column names
    cursor.execute("PRAGMA table_info(items)")
    # 获取更新后的列信息（包含tags字段）
    cursor.execute("PRAGMA table_info(items)")
    columns = [desc[1] for desc in cursor.fetchall()]

    # Stream records with pagination
    page_size = 200
    # align offset to batch size
    page_size = (page_size // batch_size) * batch_size
    offset = 0
    # Initialize batch writers
    writers = {
        'all': BatchWriter(all_dir, columns, batch_size, config['export']['batch_file']),
        'date': GroupWriter(date_dir, columns, batch_size, lambda row: process_date(row[columns.index('date')]), config['export']['batch_file']),
        'source': GroupWriter(source_dir, columns, batch_size, lambda row: hashlib.md5(row[columns.index('home')].encode()).hexdigest() if row[columns.index('home')] else None, config['export']['batch_file']),
        'member': MemberWriter(member_dir, columns, batch_size, config['export']['batch_file'])
    }

    # Stream processing
    while offset < total_count:
        cursor.execute(f"""
            SELECT * FROM items
            ORDER BY date DESC, link COLLATE NOCASE ASC
            LIMIT {page_size} OFFSET {offset}
        """)

        # 收集所有标题用于批量生成标签
        all_rows = list(cursor)

        rows_no_tags, tags_list = [], []
        # 批量生成标签（需要实现llm_callback）
        try:
            # raise ValueError("模拟错误")  # 模拟错误
            rows_no_tags = {
                row[columns.index('title')]: row
                for row in all_rows if not row[columns.index('tags')]
            }
            rows_no_tags = list(rows_no_tags.values())

            # 仅对需要处理的标题生成标签
            # tags_list = Tag.get_tags_for_titles(
            #     [row[columns.index('title')] for row in rows_no_tags],
            #     llm_callback=lambda messages: call_llm_api(
            #         messages, config['llm']),
            #     batch_size=100
            # )
            tags_list = ['' for _ in rows_no_tags]  # 模拟标签生成

            for row, tags in zip(rows_no_tags, tags_list):
                print(
                    f"Title: {row[columns.index('title')]}, Tags: {', '.join(Tag.tags[i] for i in tags)}")

            # 更新数据库中的tags字段
            update_sql = "UPDATE items SET tags = ? WHERE title = ?"
            # 精确匹配需要更新的数据
            update_data = [
                (','.join(Tag.tags[i]
                 for i in tags), row[columns.index('title')])
                for row, tags in zip(rows_no_tags, tags_list)
            ]
            cursor.executemany(update_sql, update_data)
            conn.commit()

        except Exception as e:
            print(f"标签生成失败: {str(e)}")
            tags_list = [[] for _ in range(len(all_rows))]
            conn.rollback()

        # 创建标题到标签的映射
        title_tags_map = {
            row[columns.index('title')]: tags
            for row, tags in zip(rows_no_tags, tags_list)
        }

        for row in all_rows:
            # 转换row为可修改的list
            row = list(row)
            title = row[columns.index('title')]
            # 只更新有对应标签的记录
            if title in title_tags_map and 'tags' in columns:
                tag_index = columns.index('tags')
                row[tag_index] = ','.join(Tag.tags[i]
                                          for i in title_tags_map[title])

            # Process all records
            if not valid_date(row[columns.index('date')]):
                print(f"Invalid date format in row: {row[:3]}...")
                continue

            writers['all'].add_row(row)

            # Process date grouping
            if (date_key := writers['date'].get_group_key(row)):
                writers['date'].add_row(date_key, row)

            # Process source grouping
            if (source_key := writers['source'].get_group_key(row)):
                writers['source'].add_row(source_key, row)

            # Process member grouping
            writers['member'].add_row(row)

        offset += page_size

    # Debug print actual fetched data
    # print(f"Debug: First 3 records after processing - {all_records[:3] if all_records else 'No records'}")
    print(f"Debug: Columns list - {columns}")

    print(f"Database debug info:")
    print(f"- Total records: {total_count}")
    print(f"- Table columns: {', '.join(columns)}")

    # Final statistics
    print(f"Export completed.")
    print(f"Final batch counts:")
    print(f"- All files: {writers['all'].batch_count}")
    print(f"- Date groups: {writers['date'].group_count}")
    print(f"- Source groups: {writers['source'].group_count}")
    print(f"- Member records: {writers['member'].unique_count}")

    # Finalizing all writers
    print("\nFinalizing file writes...")
    for name, writer in writers.items():
        print(f"Finalizing {name} writer...")
        writer.finalize()

    # Close database connection
    conn.close()
    print("All data persisted to disk")

    # Generate stats.json
    stats = {
        "batch": config['export']['batch_size'],
        "urls": {
            "source": [],
            "all": int((all_dir / batch_file_name).read_text()),
            "member": int((member_dir / batch_file_name).read_text()),
            "date": []
        }
    }

    # Collect source stats
    for source_path in source_dir.glob('*'):
        if source_path.is_dir():
            batch_file = source_path / batch_file_name
            if batch_file.exists():
                stats['urls']['source'].append([
                    source_path.name,
                    int(batch_file.read_text())
                ])

    # Collect date stats
    date_stats = {}
    for date_path in date_dir.glob('*'):
        if date_path.is_dir() and len(date_path.name) == 6:
            year = date_path.name[:4]
            month = date_path.name[4:]
            batch_file = date_path / batch_file_name
            if batch_file.exists():
                if year not in date_stats:
                    date_stats[year] = []
                date_stats[year].append([
                    month,
                    int(batch_file.read_text())
                ])

    # Convert date stats to required format
    stats['urls']['date'] = [[year, months]
                             for year, months in date_stats.items()]

    # Write stats.json
    stats_file = Path(config['export']['stats_file'])
    stats_min_file = Path(config['export']['stats_min_file'])
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    with open(stats_min_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False)

    # Generate stats.json with improved error handling
    stats = {
        "batch": config['export'].get('batch_size', 50),
        "urls": {
            "source": [],
            "all": 0,
            "member": 0,
            "date": []
        }
    }

    # Helper function to read batch files safely
    def read_batch(path: Path) -> int:
        try:
            return int(path.read_text().strip()) if path.exists() else 0
        except ValueError:
            return 0

    # Populate all and member counts
    stats['urls']['all'] = read_batch(all_dir / batch_file_name)
    stats['urls']['member'] = read_batch(member_dir / batch_file_name)

    # Populate source stats
    for source_path in source_dir.glob('*'):
        if source_path.is_dir():
            batch_value = read_batch(source_path / batch_file_name)
            if batch_value > 0:
                stats['urls']['source'].append([source_path.name, batch_value])

    # Populate date stats with validation
    date_stats = {}
    for date_path in date_dir.glob('*'):
        if date_path.is_dir() and len(date_path.name) == 6 and date_path.name.isdigit():
            year = date_path.name[:4]
            month = date_path.name[4:]
            batch_value = read_batch(date_path / batch_file_name)
            if batch_value > 0:
                if year not in date_stats:
                    date_stats[year] = {}
                date_stats[year][month] = batch_value

    # Convert date stats to required nested format
    stats['urls']['date'] = [
        [year, [[month, count] for month, count in months.items()]]
        for year, months in date_stats.items()
    ]

    # Write stats.json
    stats_file = Path(config['export'].get('stats_file', 'public/stats.json'))
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    # compress the json file
    with open(stats_min_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False)

    print(f"Stats file generated: {stats_file}")


def call_llm_api(messages, config):
    """调用LLM API并处理响应，包含完善的错误处理和重试机制"""
    client = openai.Client(
        api_key=config['api_key'],
        base_url=config['api_endpoint'],
    )

    max_retries = 3
    backoff_factor = 1.5

    for attempt in range(max_retries):
        try:
            print(
                f"LLM API请求: {json.dumps(messages, ensure_ascii=False, indent=2)}")
            response = client.chat.completions.create(
                model=config.get('model', 'gpt-3.5-turbo'),
                messages=messages,
                temperature=0.2
            )

            # 正确解析新版SDK的响应格式
            if response.choices and response.choices[0].message.content:
                tags = response.choices[0].message.content.strip()
                print(f"LLM API响应: {tags}")
                return tags

            raise RuntimeError("API返回空响应内容")

        except Exception as e:
            print(f"LLM API请求失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print("达到最大重试次数，抛出异常")
                raise RuntimeError("超过最大重试次数")

    raise RuntimeError("超过最大重试次数")


if __name__ == "__main__":
    export_sqlite_to_csv()

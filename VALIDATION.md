# stats.min.json 数据验证

## 概述

本文档介绍 stats.min.json 文件的自动验证机制。

## 功能

- **每日自动验证**: 使用 GitHub Actions 每天自动运行验证
- **数据质量检查**: 检测 stats.min.json 中的日期格式问题
- **自动创建 Issue**: 发现问题时自动创建 GitHub Issue
- **防止重复**: 检查是否已有相同问题的 Issue，避免重复创建

## 验证规则

### 年份格式
- 必须是4位数字（如 `2025`）
- 必须在合理范围内（1970-2100）
- **错误示例**: `1-01`, `202`, `20251`

### 月份格式
- 必须是2位数字（如 `01`, `12`）
- 必须在有效范围内（01-12）
- **错误示例**: `1`, `13`, `00`

## 文件说明

### validate_stats.py

Python 脚本，用于验证 stats.min.json 文件的数据质量。

**使用方法**:
```bash
python validate_stats.py <path_to_stats.min.json>
```

**返回值**:
- 退出码 0: 验证通过
- 退出码 1: 验证失败

**示例**:
```bash
# 验证本地文件
python validate_stats.py ./public/stats.min.json

# 输出示例（验证失败）
✗ Validation failed: Issues found in stats.min.json

Issues detected:
1. Date entry issue - Invalid year format: '1-01' (expected 4-digit year like '2025')
2. Date entry issue - Year '2025', Month '13' is out of valid range (01-12)
```

### .github/workflows/validate-stats.yml

GitHub Actions 工作流，每天自动运行验证。

**触发方式**:
- 定时: 每天 UTC 00:00 自动运行
- 手动: 在 GitHub Actions 页面手动触发

**工作流程**:
1. 检出 master 分支（获取验证脚本）
2. 检出 public 分支（获取 stats.min.json）
3. 运行验证脚本
4. 如果验证失败：
   - 检查是否已有相同类型的开放 Issue
   - 如果没有，创建新 Issue
   - 如果已有，在该 Issue 下添加评论更新

## 问题修复

当收到验证失败的 Issue 时，按以下步骤修复：

1. **查看 Issue 中的错误详情**
   - Issue 会列出所有检测到的问题
   - 每个问题都会说明具体的错误位置

2. **检查源数据**
   - 查看 CSV 文件中的 `date` 字段
   - 确保日期格式为 `YYYY-MM-DD`

3. **修复代码（如需要）**
   - 检查 `fetch_utils.py` 中的 `split_date()` 函数
   - 检查 `merge_utils.py` 中的 `merge_date()` 函数
   - 确保日期解析逻辑正确

4. **重新生成数据**
   - 运行 `python action.py` 重新生成数据
   - 提交并推送到 public 分支

5. **验证修复**
   - 在 GitHub Actions 页面手动触发验证工作流
   - 或等待下一次定时运行

### 已实施的修复措施

为了防止日期格式错误（如 "1-01" 这样的年份），代码已添加以下改进：

1. **`fetch_utils.py` 中的 `split_date()` 函数**（第 129-160 行）：
   - 添加了日期格式验证，确保日期字符串长度至少为 10 个字符且格式为 `YYYY-MM-DD`
   - 在字符串切片前验证分隔符位置
   - 验证提取的年份（1970-2100 范围）和月份（01-12 范围）是否有效
   - 对于格式错误的日期，记录警告并跳过该条记录，而不是生成错误的年月目录

2. **`merge_utils.py` 中的 `merge_date()` 函数**（第 145-181 行）：
   - 添加了目录名长度验证，确保为 6 个字符（YYYYMM 格式）
   - 验证提取的年份和月份是否为有效数字
   - 验证年份和月份的合理范围
   - 对于格式错误的目录，记录警告并跳过，避免将无效数据写入 stats.min.json

这些改进确保了：
- 只有符合标准格式的日期数据才会被处理
- 异常数据会被识别并记录，但不会中断处理流程
- stats.min.json 中不会出现格式错误的年份或月份信息

## 问题标签

验证工作流创建的 Issue 会自动添加以下标签：
- `stats-validation-error`: 标识这是数据验证错误
- `automated`: 标识这是自动创建的 Issue

## 常见问题

### Q: 为什么会出现 "1-01" 这样的年份？

A: 这种问题已经通过添加日期格式验证得到修复。之前的问题是：
- CSV 文件中的日期格式不标准（如长度不足、缺少分隔符等）
- `fetch_utils.py` 第137行的日期切片逻辑没有验证输入格式
- `merge_utils.py` 第161-162行的年月提取逻辑没有验证目录名格式

现在的代码会：
- 在切片前验证日期字符串的长度和格式
- 验证提取的年份（1970-2100）和月份（01-12）是否在有效范围内
- 跳过格式错误的数据并记录警告，而不是生成错误的输出
- 确保只有符合标准的日期数据才会被写入 stats.min.json

### Q: 如何手动运行验证？

A: 可以在本地运行：
```bash
# 假设已经检出了 public 分支到 ./public 目录
python validate_stats.py ./public/stats.min.json
```

### Q: 验证脚本会修改数据吗？

A: 不会。验证脚本只读取文件进行检查，不会修改任何数据。

## 相关文件

- `validate_stats.py` - 验证脚本
- `.github/workflows/validate-stats.yml` - GitHub Actions 工作流
- `fetch_utils.py` - 包含日期分割逻辑
- `merge_utils.py` - 包含日期合并逻辑

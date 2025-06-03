from .csv_to_sqlite import import_csv_to_sqlite
from .sqlite_to_csv import export_sqlite_to_csv
import argparse

def main(args = None):
    if not args:
        parser = argparse.ArgumentParser(description='数据转换工具')
        subparsers = parser.add_subparsers(dest='command')

        # 导入命令
        import_parser = subparsers.add_parser('import', help='导入CSV到数据库')
        import_parser.add_argument("csv_dir", help="包含CSV文件的目录路径")

        # 导出命令
        export_parser = subparsers.add_parser('export', help='从数据库导出CSV')

        args = parser.parse_args()

    if args.command == 'import':
        import_csv_to_sqlite(args.csv_dir)
        export_sqlite_to_csv()  # 导入完成后自动导出
    elif args.command == 'export':
        export_sqlite_to_csv()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
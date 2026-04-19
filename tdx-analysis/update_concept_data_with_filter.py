#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
更新概念板块数据并过滤北证股票 - 兼容concept_tool.py格式
"""

import sqlite3
import os
import sys
import datetime
from collections import defaultdict

# 数据库文件路径
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "concept_weekly.db")
# 概念板块文件路径
CONCEPT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "概念板块.txt")

def create_connection(db_file):
    """创建数据库连接"""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"[ERR ] 数据库连接失败: {e}")
        return None

def is_valid_stock(stock_code):
    """
    判断是否为有效的股票代码
    只保留: 688、30、60、00开头的股票
    """
    if stock_code.startswith('688'):  # 科创板
        return True
    elif stock_code.startswith('30'):  # 创业板
        return True
    elif stock_code.startswith('60'):  # 沪市主板
        return True
    elif stock_code.startswith('00'):  # 深市主板
        return True
    else:
        return False

def is_st_stock(stock_name):
    """
    判断是否为ST股票
    包括ST、*ST、ST*等情况
    """
    if stock_name is None or stock_name == '':
        return False
    # 检查是否包含ST (包括ST、*ST、ST*等情况)
    stock_name_upper = stock_name.upper()
    return 'ST' in stock_name_upper

# 需要过滤的板块关键词
FILTER_SECTOR_KEYWORDS = ['创投', '转债', '一带一路', '粤港澳']

def should_filter_sector(sector_name):
    """
    判断是否需要过滤该板块
    根据板块名称中的关键词过滤
    """
    if sector_name is None or sector_name == '':
        return False
    for keyword in FILTER_SECTOR_KEYWORDS:
        if keyword in sector_name:
            return True
    return False

def import_concept_sectors_with_filter(conn, concept_file):
    """
    导入概念板块数据并过滤北证股票
    兼容concept_tool.py的格式和逻辑
    """
    c = conn.cursor()
    sector_names = {}
    sectors = defaultdict(list)
    stock_names = {}
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    total_lines = 0
    skipped_bj = 0
    skipped_st = 0
    valid_lines = 0
    
    print(f"[INFO] 开始读取概念板块文件: {concept_file}")
    
    try:
        with open(concept_file, 'r', encoding='gbk') as f:
            for line_num, line in enumerate(f, 1):
                total_lines += 1
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split(',')
                if len(parts) < 3:
                    print(f"[WARN] 第{line_num}行格式错误: {line}")
                    continue
                
                s_code = parts[0].strip()
                s_name = parts[1].strip()
                st_code = parts[2].strip()
                st_name = parts[3].strip() if len(parts) >= 4 else ''
                
                # 检查是否为有效股票（只保留688、30、60、00开头的）
                if not is_valid_stock(st_code):
                    skipped_bj += 1
                    if skipped_bj <= 5:  # 只显示前5个示例
                        print(f"[INFO] 跳过非指定类型股票: {st_code} {st_name} (板块: {s_name})")
                    continue
                
                # 检查是否为ST股票
                if is_st_stock(st_name):
                    skipped_st += 1
                    if skipped_st <= 5:  # 只显示前5个示例
                        print(f"[INFO] 跳过ST股票: {st_code} {st_name} (板块: {s_name})")
                    continue
                
                # 检查是否需要过滤该板块
                if should_filter_sector(s_name):
                    continue
                
                sector_names[s_code] = s_name
                sectors[s_code].append(st_code)
                stock_names[st_code] = st_name
                valid_lines += 1
                
                # 每处理1000行显示一次进度
                if valid_lines % 1000 == 0:
                    print(f"[INFO] 已处理 {valid_lines} 行有效数据...")
        
        print(f"[ OK ] 文件读取完成")
        print(f"       总行数: {total_lines}")
        print(f"       有效数据: {valid_lines}")
        print(f"       跳过非指定类型股票: {skipped_bj}")
        print(f"       跳过ST股票: {skipped_st}")
        
        if skipped_bj > 5:
            print(f"       （还有 {skipped_bj-5} 只非指定类型股票未显示）")
        if skipped_st > 5:
            print(f"       （还有 {skipped_st-5} 只ST股票未显示）")
        
    except FileNotFoundError:
        print(f"[ERR ] 找不到概念板块文件: {concept_file}")
        return False
    except Exception as e:
        print(f"[ERR ] 读取文件失败: {e}")
        return False
    
    # 清空旧数据
    print(f"[INFO] 清空旧数据...")
    try:
        c.execute("DELETE FROM t_sector")
        c.execute("DELETE FROM t_sector_stock")
        c.execute("DELETE FROM t_stock")
        conn.commit()
        print(f"[ OK ] 旧数据已清空")
    except sqlite3.Error as e:
        print(f"[ERR ] 清空数据失败: {e}")
        conn.rollback()
        return False
    
    # 插入板块数据
    print(f"[INFO] 插入板块数据...")
    sector_values = [(s_code, s_name, now_str) for s_code, s_name in sector_names.items()]
    try:
        c.executemany('''
        INSERT INTO t_sector (sector_code, sector_name, created_at)
        VALUES (?, ?, ?)
        ''', sector_values)
        print(f"[ OK ] 插入 {len(sector_values)} 个板块")
    except sqlite3.Error as e:
        print(f"[ERR ] 插入板块数据失败: {e}")
        conn.rollback()
        return False
    
    # 插入股票数据
    print(f"[INFO] 插入股票数据...")
    stock_values = []
    for st_code, st_name in stock_names.items():
        # 判断市场
        if st_code.startswith('6'):  # 包括60开头的沪市主板和688开头的科创板
            market = 'SH'
        elif st_code.startswith('0'):  # 深市主板
            market = 'SZ'
        elif st_code.startswith('3'):  # 创业板
            market = 'SZ'
        else:
            market = 'OTHER'  # 理论上不会出现，因为已经过滤了
        
        stock_values.append((st_code, st_name, market, now_str))
    
    try:
        c.executemany('''
        INSERT INTO t_stock (stock_code, stock_name, market, updated_at)
        VALUES (?, ?, ?, ?)
        ''', stock_values)
        print(f"[ OK ] 插入 {len(stock_values)} 只股票")
    except sqlite3.Error as e:
        print(f"[ERR ] 插入股票数据失败: {e}")
        conn.rollback()
        return False
    
    # 插入板块-股票关系
    print(f"[INFO] 插入板块-股票关系...")
    relation_values = []
    for s_code, stock_list in sectors.items():
        for st_code in stock_list:
            st_name = stock_names.get(st_code, '')
            relation_values.append((s_code, st_code, st_name))
    
    try:
        c.executemany('''
        INSERT INTO t_sector_stock (sector_code, stock_code, stock_name)
        VALUES (?, ?, ?)
        ''', relation_values)
        print(f"[ OK ] 插入 {len(relation_values)} 条板块-股票关系")
    except sqlite3.Error as e:
        print(f"[ERR ] 插入板块-股票关系失败: {e}")
        conn.rollback()
        return False
    
    conn.commit()
    return True

def verify_update(conn):
    """验证更新结果"""
    c = conn.cursor()
    
    print(f"\n[INFO] 验证更新结果...")
    
    try:
        # 统计板块数量
        c.execute("SELECT COUNT(*) FROM t_sector")
        sector_count = c.fetchone()[0]
        
        # 统计股票数量
        c.execute("SELECT COUNT(*) FROM t_stock")
        stock_count = c.fetchone()[0]
        
        # 统计板块-股票关系数量
        c.execute("SELECT COUNT(*) FROM t_sector_stock")
        relation_count = c.fetchone()[0]
        
        # 检查非指定类型股票（非688、30、60、00开头）
        # 创建一个SQL查询，找出所有不符合要求的股票
        c.execute("""SELECT COUNT(*) FROM t_stock 
                    WHERE NOT (
                        stock_code LIKE '688%' OR 
                        stock_code LIKE '30%' OR 
                        stock_code LIKE '60%' OR 
                        stock_code LIKE '00%'
                    )""")
        invalid_stock_count = c.fetchone()[0]
        
        # 统计有效股票数量
        c.execute("""SELECT COUNT(*) FROM t_stock 
                    WHERE stock_code LIKE '688%' OR 
                          stock_code LIKE '30%' OR 
                          stock_code LIKE '60%' OR 
                          stock_code LIKE '00%'""")
        valid_stock_count = c.fetchone()[0]
        
        # 检查ST股票数量
        c.execute("""SELECT COUNT(*) FROM t_stock 
                    WHERE UPPER(stock_name) LIKE '%ST%'""")
        st_stock_count = c.fetchone()[0]
        
        # 统计板块平均股票数
        c.execute("""
        SELECT AVG(stock_count) as avg_stocks_per_sector
        FROM (
            SELECT sector_code, COUNT(*) as stock_count 
            FROM t_sector_stock 
            GROUP BY sector_code
        )
        """)
        result = c.fetchone()
        avg_stocks = result[0] if result and result[0] is not None else 0
        
        # 检查是否有股票属于多个板块
        c.execute("""
        SELECT stock_code, COUNT(*) as sector_count
        FROM t_sector_stock 
        GROUP BY stock_code 
        HAVING COUNT(*) > 1
        ORDER BY sector_count DESC
        LIMIT 5
        """)
        multi_sector_stocks = c.fetchall()
        
        print(f"[ OK ] 验证完成:")
        print(f"       板块数量: {sector_count}")
        print(f"       股票数量: {stock_count}")
        print(f"       有效股票数量: {valid_stock_count} (688/30/60/00开头)")
        print(f"       ST股票数量: {st_stock_count} (应为0)")
        print(f"       板块-股票关系: {relation_count}")
        print(f"       板块平均股票数: {avg_stocks:.1f}")
        print(f"       非指定类型股票数量: {invalid_stock_count} (应为0)")
        
        if invalid_stock_count == 0:
            print(f"       [OK] 过滤成功，只保留688/30/60/00开头的股票")
        else:
            print(f"       [WARN] 仍有 {invalid_stock_count} 只非指定类型股票")
        
        if st_stock_count == 0:
            print(f"       [OK] ST股票过滤成功")
        else:
            print(f"       [WARN] 仍有 {st_stock_count} 只ST股票")
        
        if multi_sector_stocks:
            print(f"       多板块股票示例 (前5个):")
            for stock_code, sector_count in multi_sector_stocks:
                c.execute("SELECT stock_name FROM t_stock WHERE stock_code = ?", (stock_code,))
                stock_name = c.fetchone()
                stock_name = stock_name[0] if stock_name else "未知"
                print(f"         {stock_code} {stock_name}: {sector_count} 个板块")
        
        return invalid_stock_count == 0
        
    except sqlite3.Error as e:
        print(f"[ERR ] 验证失败: {e}")
        return False

def main():
    """主函数"""
    print("=== 概念板块数据更新脚本 ===\n")
    print("功能: 1. 更新概念板块数据")
    print("      2. 只保留指定类型股票 (688/30/60/00开头)")
    print("      3. 自动过滤ST股票")
    print("      4. 自动过滤指定板块")
    print("      5. 兼容现有concept_tool.py数据库格式")
    print("")
    print("股票过滤规则:")
    print("  1. 类型过滤:")
    print("    - 688开头: 科创板")
    print("    - 30开头:  创业板")
    print("    - 60开头:  沪市主板")
    print("    - 00开头:  深市主板")
    print("    - 其他开头的股票将被过滤掉")
    print("")
    print("  2. ST股票过滤:")
    print("    - 自动过滤名称中包含ST的股票")
    print("    - 包括: ST、*ST、ST*等")
    print("    - 避免高风险ST股票进入数据库")
    print("")
    print("  3. 板块过滤:")
    print(f"    - 自动过滤包含以下关键词的板块: {', '.join(FILTER_SECTOR_KEYWORDS)}")
    print("    - 这些板块不会被导入数据库\n")
    
    # 检查文件是否存在
    if not os.path.exists(CONCEPT_FILE):
        print(f"[ERR ] 找不到概念板块文件: {CONCEPT_FILE}")
        return False
    
    # 创建数据库连接
    conn = create_connection(DB_FILE)
    if not conn:
        return False
    
    try:
        # 更新数据
        success = import_concept_sectors_with_filter(conn, CONCEPT_FILE)
        
        if not success:
            print(f"\n[ERR ] 数据更新失败")
            return False
        
        # 验证更新结果
        verification_passed = verify_update(conn)
        
        if verification_passed:
            print(f"\n[ OK ] 数据更新完成！")
            print(f"       数据库已更新: {DB_FILE}")
            print(f"       概念板块文件: {CONCEPT_FILE}")
            print(f"       已成功过滤，只保留688/30/60/00开头的股票")
            print(f"       已成功过滤所有ST股票")
            print(f"       已过滤板块关键词: {', '.join(FILTER_SECTOR_KEYWORDS)}")
            return True
        else:
            print(f"\n[WARN] 数据更新完成，但验证发现问题")
            return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = main()
    if not success:
        print(f"\n[ERR ] 数据更新失败")
        sys.exit(1)
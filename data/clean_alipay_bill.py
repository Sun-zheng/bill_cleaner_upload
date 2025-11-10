#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
支付宝账单清洗脚本

该脚本用于清洗和整理支付宝账单数据，将其转换为标准化格式。
"""

import os
import sys
import csv
import logging
import pandas as pd
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'log', 'alipay_cleaning.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_log_directory():
    """
    设置日志目录
    """
    log_dir = os.path.join(os.path.dirname(__file__), 'log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        logger.info(f"创建日志目录: {log_dir}")

def detect_file_encoding(file_path):
    """
    检测文件编码
    """
    encodings = ['utf-8', 'gbk', 'utf-16', 'latin1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                # 读取前几行来测试编码
                for _ in range(5):
                    f.readline()
            logger.info(f"检测到文件编码: {encoding}")
            return encoding
        except UnicodeDecodeError:
            continue
    
    logger.warning("无法确定文件编码，默认使用utf-8")
    return 'utf-8'

def clean_alipay_bill(file_path, output_dir):
    """
    清洗支付宝账单
    
    Args:
        file_path: 输入文件路径
        output_dir: 输出目录路径
    
    Returns:
        bool: 处理成功返回True，失败返回False
    """
    setup_log_directory()
    logger.info(f"开始处理支付宝账单文件: {file_path}")
    
    try:
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"创建输出目录: {output_dir}")
        
        # 检测文件编码
        encoding = detect_file_encoding(file_path)
        
        # 读取文件扩展名
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # 根据文件类型选择读取方法
        if file_ext in ['.csv']:
            # 对于CSV文件，可能需要跳过前几行的账单说明信息
            with open(file_path, 'r', encoding=encoding) as f:
                lines = f.readlines()
            
            # 找到表头所在行
            header_index = None
            for i, line in enumerate(lines):
                if '交易时间' in line and '交易类型' in line and '交易对方' in line:
                    header_index = i
                    break
            
            if header_index is not None:
                # 提取有效数据
                valid_lines = lines[header_index:]
                
                # 使用pandas读取数据
                from io import StringIO
                df = pd.read_csv(StringIO(''.join(valid_lines)), encoding=encoding)
                logger.info(f"成功读取CSV文件，共 {len(df)} 条记录")
            else:
                # 尝试直接读取
                df = pd.read_csv(file_path, encoding=encoding)
                logger.info(f"直接读取CSV文件，共 {len(df)} 条记录")
        
        elif file_ext in ['.xls', '.xlsx']:
            # 对于Excel文件
            df = pd.read_excel(file_path)
            logger.info(f"成功读取Excel文件，共 {len(df)} 条记录")
        
        else:
            logger.error(f"不支持的文件格式: {file_ext}")
            return False
        
        # 清理列名（去除空格和换行符）
        df.columns = df.columns.str.strip().str.replace('\n', '')
        
        # 检查必要的列是否存在
        required_columns = ['交易时间', '交易对方', '金额']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"缺少部分必要列: {', '.join(missing_columns)}")
            # 尝试找到相似的列名
            for missing_col in missing_columns:
                similar_cols = [col for col in df.columns if missing_col in col or col in missing_col]
                if similar_cols:
                    logger.info(f"可能的替代列 '{missing_col}': {', '.join(similar_cols)}")
        
        # 处理日期列
        if '交易时间' in df.columns:
            try:
                # 尝试转换日期格式
                df['交易时间'] = pd.to_datetime(df['交易时间'], errors='coerce')
                logger.info("成功转换交易时间列")
            except Exception as e:
                logger.error(f"转换交易时间列时出错: {str(e)}")
        
        # 处理金额列
        if '金额' in df.columns:
            try:
                # 移除货币符号和逗号
                df['金额'] = df['金额'].astype(str).str.replace('[¥,\s]', '', regex=True)
                # 转换为数字
                df['金额'] = pd.to_numeric(df['金额'], errors='coerce')
                logger.info("成功转换金额列")
            except Exception as e:
                logger.error(f"转换金额列时出错: {str(e)}")
        
        # 处理收支方向
        if '收/支' not in df.columns and ('收支类型' in df.columns or '类型' in df.columns):
            if '收支类型' in df.columns:
                df['收/支'] = df['收支类型']
            elif '类型' in df.columns:
                df['收/支'] = df['类型']
            logger.info("添加收/支列")
        
        # 清理空行
        original_len = len(df)
        df.dropna(how='all', inplace=True)
        cleaned_rows = original_len - len(df)
        if cleaned_rows > 0:
            logger.info(f"删除空行: {cleaned_rows} 行")
        
        # 清理重复行
        original_len = len(df)
        df.drop_duplicates(inplace=True)
        deduplicated_rows = original_len - len(df)
        if deduplicated_rows > 0:
            logger.info(f"删除重复行: {deduplicated_rows} 行")
        
        # 按交易时间排序
        if '交易时间' in df.columns:
            df.sort_values(by='交易时间', inplace=True)
            logger.info("按交易时间排序")
        
        # 生成输出文件名
        base_name = os.path.basename(file_path)
        file_name = f"alipay_{os.path.splitext(base_name)[0]}_processed.csv"
        output_path = os.path.join(output_dir, file_name)
        
        # 保存处理后的数据
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"处理完成，保存到: {output_path}")
        
        # 生成处理报告
        generate_report(file_path, output_path, df, cleaned_rows, deduplicated_rows)
        
        return True
        
    except Exception as e:
        logger.error(f"处理支付宝账单时出错: {str(e)}", exc_info=True)
        return False

def generate_report(input_file, output_file, df, cleaned_rows, deduplicated_rows):
    """
    生成处理报告
    """
    report_dir = os.path.dirname(output_file)
    report_path = os.path.join(report_dir, 'alipay_bill_cleaning_report.md')
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 支付宝账单清洗报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## 处理信息\n\n")
            f.write(f"- 输入文件: {os.path.basename(input_file)}\n")
            f.write(f"- 输出文件: {os.path.basename(output_file)}\n")
            f.write(f"- 原始记录数: {len(df) + cleaned_rows + deduplicated_rows}\n")
            f.write(f"- 清理后记录数: {len(df)}\n")
            f.write(f"- 删除空行数: {cleaned_rows}\n")
            f.write(f"- 删除重复行数: {deduplicated_rows}\n\n")
            
            f.write("## 数据概览\n\n")
            f.write("### 包含的列\n\n")
            for col in df.columns:
                f.write(f"- {col}\n")
            
            f.write("\n### 前5行数据\n\n")
            # 转换为Markdown表格格式
            if len(df) > 0:
                sample_df = df.head().fillna('')
                # 写入表头
                f.write('| ' + ' | '.join(sample_df.columns) + ' |\n')
                f.write('| ' + ' | '.join(['---'] * len(sample_df.columns)) + ' |\n')
                # 写入数据行
                for _, row in sample_df.iterrows():
                    row_str = '| ' + ' | '.join([str(val) for val in row]) + ' |\n'
                    f.write(row_str)
            else:
                f.write("无数据\n")
            
            f.write("\n### 统计信息\n\n")
            # 计算收支统计
            if '收/支' in df.columns and '金额' in df.columns:
                # 确保金额列是数字类型
                if pd.api.types.is_numeric_dtype(df['金额']):
                    income = df[df['收/支'].str.contains('收入|转入|收款', na=False)]['金额'].sum()
                    expense = df[df['收/支'].str.contains('支出|转出|付款', na=False)]['金额'].sum()
                    f.write(f"- 总收入: {income:.2f}\n")
                    f.write(f"- 总支出: {expense:.2f}\n")
                    f.write(f"- 净收支: {(income - expense):.2f}\n")
        
        logger.info(f"生成处理报告: {report_path}")
        return True
    except Exception as e:
        logger.error(f"生成处理报告时出错: {str(e)}")
        return False

def main():
    """
    主函数，用于独立运行时测试
    """
    if len(sys.argv) < 3:
        print("用法: python clean_alipay_bill.py <输入文件路径> <输出目录路径>")
        return 1
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"错误: 输入文件不存在: {input_file}")
        return 1
    
    result = clean_alipay_bill(input_file, output_dir)
    return 0 if result else 1

if __name__ == "__main__":
    sys.exit(main())
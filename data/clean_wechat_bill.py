#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
微信账单清洗脚本

该脚本用于清洗和整理微信支付账单数据，将其转换为标准化格式。
"""

import os
import sys
import re
import logging
import pandas as pd
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'log', 'wechat_cleaning.log')),
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

def clean_wechat_bill(file_path, output_dir):
    """
    清洗微信账单
    
    Args:
        file_path: 输入文件路径
        output_dir: 输出目录路径
    
    Returns:
        bool: 处理成功返回True，失败返回False
    """
    setup_log_directory()
    logger.info(f"开始处理微信账单文件: {file_path}")
    
    try:
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"创建输出目录: {output_dir}")
        
        # 检测文件编码
        encoding = detect_file_encoding(file_path)
        
        # 读取文件扩展名
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext != '.csv':
            logger.error(f"微信账单通常为CSV格式，当前文件格式: {file_ext}")
            return False
        
        # 微信账单CSV文件通常有一些说明行需要跳过
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        
        # 找到表头所在行（通常包含"交易时间"、"交易类型"等关键字）
        header_index = None
        for i, line in enumerate(lines):
            if '交易时间' in line and '交易类型' in line:
                header_index = i
                break
        
        if header_index is not None:
            # 提取有效数据
            valid_lines = lines[header_index:]
            logger.info(f"跳过前 {header_index} 行说明文本")
            
            # 使用pandas读取数据
            from io import StringIO
            df = pd.read_csv(StringIO(''.join(valid_lines)), encoding=encoding)
            logger.info(f"成功读取微信账单文件，共 {len(df)} 条记录")
        else:
            # 尝试直接读取
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"直接读取微信账单文件，共 {len(df)} 条记录")
        
        # 清理列名（去除空格和换行符）
        df.columns = df.columns.str.strip().str.replace('\n', '')
        
        # 检查必要的列是否存在
        required_columns = ['交易时间', '交易类型', '金额(元)']
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
                # 微信账单的日期格式通常为"YYYY-MM-DD HH:MM:SS"
                df['交易时间'] = pd.to_datetime(df['交易时间'], errors='coerce')
                logger.info("成功转换交易时间列")
            except Exception as e:
                logger.error(f"转换交易时间列时出错: {str(e)}")
        
        # 处理金额列
        if '金额(元)' in df.columns:
            try:
                # 移除货币符号和逗号
                df['金额(元)'] = df['金额(元)'].astype(str).str.replace('[¥,\s]', '', regex=True)
                # 转换为数字
                df['金额(元)'] = pd.to_numeric(df['金额(元)'], errors='coerce')
                # 为了与其他账单保持一致，重命名为"金额"
                df = df.rename(columns={'金额(元)': '金额'})
                logger.info("成功转换金额列")
            except Exception as e:
                logger.error(f"转换金额列时出错: {str(e)}")
        
        # 处理收支方向
        if '收/支' in df.columns:
            # 统一收支表示
            df['收/支'] = df['收/支'].str.replace('支出', '支出').str.replace('收入', '收入')
            logger.info("标准化收/支列")
        
        # 处理商品名称列（有些版本称为"商品"）
        if '商品' in df.columns and '商品名称' not in df.columns:
            df = df.rename(columns={'商品': '商品名称'})
            logger.info("将'商品'列重命名为'商品名称'")
        
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
        
        # 添加平台标识列
        df['平台'] = '微信'
        logger.info("添加平台标识列")
        
        # 生成输出文件名
        base_name = os.path.basename(file_path)
        file_name = f"wechat_{os.path.splitext(base_name)[0]}_processed.csv"
        output_path = os.path.join(output_dir, file_name)
        
        # 保存处理后的数据
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"处理完成，保存到: {output_path}")
        
        # 生成处理报告
        generate_report(file_path, output_path, df, cleaned_rows, deduplicated_rows)
        
        return True
        
    except Exception as e:
        logger.error(f"处理微信账单时出错: {str(e)}", exc_info=True)
        return False

def generate_report(input_file, output_file, df, cleaned_rows, deduplicated_rows):
    """
    生成处理报告
    """
    report_dir = os.path.dirname(output_file)
    report_path = os.path.join(report_dir, 'wechat_bill_cleaning_report.md')
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 微信账单清洗报告\n\n")
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
                    income = df[df['收/支'].str.contains('收入', na=False)]['金额'].sum()
                    expense = df[df['收/支'].str.contains('支出', na=False)]['金额'].sum()
                    f.write(f"- 总收入: {income:.2f}\n")
                    f.write(f"- 总支出: {expense:.2f}\n")
                    f.write(f"- 净收支: {(income - expense):.2f}\n")
            
            # 交易类型统计
            if '交易类型' in df.columns:
                f.write("\n### 交易类型分布\n\n")
                type_counts = df['交易类型'].value_counts()
                for transaction_type, count in type_counts.items():
                    f.write(f"- {transaction_type}: {count}\n")
        
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
        print("用法: python clean_wechat_bill.py <输入文件路径> <输出目录路径>")
        return 1
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"错误: 输入文件不存在: {input_file}")
        return 1
    
    result = clean_wechat_bill(input_file, output_dir)
    return 0 if result else 1

if __name__ == "__main__":
    sys.exit(main())
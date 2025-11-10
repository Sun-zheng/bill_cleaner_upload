#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
飞书多维表格批量数据上传工具

该脚本用于将处理后的账单数据上传到飞书多维表格中，支持支付宝、微信和京东账单的批量上传。
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('data', 'log', 'feishu_upload.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 尝试导入配置
config = {}
try:
    # 如果存在config.py文件，则导入
    if os.path.exists('config.py'):
        from config import FEISHU_CONFIG
        config = FEISHU_CONFIG
    else:
        # 否则使用默认配置
        config = {
            'app_token': '',  # 需要用户手动设置
            'table_id': '',   # 需要用户手动设置
            'user_access_token': '',  # 需要用户手动设置
            'base_url': 'https://open.feishu.cn/open-apis'
        }
        logger.warning("未找到config.py文件，使用默认配置")
except Exception as e:
    logger.error(f"导入配置时出错: {str(e)}")
    sys.exit(1)

def create_dirs():
    """
    创建必要的目录
    """
    log_dir = os.path.join('data', 'log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        logger.info(f"创建日志目录: {log_dir}")

def check_config():
    """
    检查配置是否完整
    """
    required_keys = ['app_token', 'table_id', 'user_access_token']
    missing_keys = [key for key in required_keys if not config.get(key)]
    
    if missing_keys:
        logger.error(f"缺少必要的配置: {', '.join(missing_keys)}")
        logger.error("请先配置config.py文件中的FEISHU_CONFIG")
        return False
    return True

def get_bill_files(directory):
    """
    获取指定目录下的所有处理后的账单文件
    """
    bill_files = []
    
    try:
        for filename in os.listdir(directory):
            # 筛选处理后的账单文件
            if filename.endswith('_processed.csv') or filename.endswith('_merged.csv'):
                file_path = os.path.join(directory, filename)
                bill_files.append((filename, file_path))
                logger.info(f"找到账单文件: {filename}")
    except Exception as e:
        logger.error(f"获取账单文件列表时出错: {str(e)}")
    
    return bill_files

def read_csv_file(file_path):
    """
    读取CSV文件内容
    """
    try:
        import csv
        data = []
        
        # 尝试不同的编码
        encodings = ['utf-8', 'gbk', 'utf-16']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        data.append(row)
                logger.info(f"成功读取文件 {file_path}，使用编码: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if not data:
            logger.error(f"无法读取文件 {file_path}，所有编码尝试均失败")
        
        return data
    except Exception as e:
        logger.error(f"读取CSV文件时出错: {str(e)}")
        return []

def upload_to_feishu(bill_data, filename):
    """
    将账单数据上传到飞书多维表格
    """
    if not bill_data:
        logger.warning(f"没有数据需要上传: {filename}")
        return False
    
    url = f"{config['base_url']}/bitable/v1/apps/{config['app_token']}/tables/{config['table_id']}/records"
    headers = {
        'Authorization': f'Bearer {config["user_access_token"]}',
        'Content-Type': 'application/json'
    }
    
    # 批量上传，每次最多上传100条数据
    batch_size = 100
    total_records = len(bill_data)
    uploaded_records = 0
    
    try:
        for i in range(0, total_records, batch_size):
            batch = bill_data[i:i+batch_size]
            
            # 转换数据格式
            records = []
            for row in batch:
                fields = {}
                for key, value in row.items():
                    # 处理特殊字符和空值
                    if isinstance(value, str):
                        value = value.strip()
                    fields[key] = value if value else ""
                
                records.append({'fields': fields})
            
            payload = {
                'records': records
            }
            
            response = requests.post(url, headers=headers, json=payload)
            result = response.json()
            
            if result.get('code') == 0:
                uploaded_records += len(batch)
                logger.info(f"成功上传 {len(batch)} 条记录，累计: {uploaded_records}/{total_records}")
            else:
                logger.error(f"上传失败: {result.get('msg', '未知错误')}")
                logger.error(f"错误详情: {json.dumps(result, ensure_ascii=False)}")
                return False
    except Exception as e:
        logger.error(f"上传数据时出错: {str(e)}")
        return False
    
    logger.info(f"文件 {filename} 上传完成，共上传 {uploaded_records} 条记录")
    return True

def generate_upload_report(upload_results):
    """
    生成上传报告
    """
    report_path = os.path.join('data', 'output', 'feishu_upload_report.md')
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 飞书多维表格上传报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## 上传结果\n\n")
            f.write("| 文件名 | 状态 | 说明 |\n")
            f.write("|--------|------|------|\n")
            
            for filename, result in upload_results.items():
                status = "成功" if result else "失败"
                explanation = "上传完成" if result else "上传失败，请查看日志"
                f.write(f"| {filename} | {status} | {explanation} |\n")
            
            f.write("\n## 配置信息\n\n")
            f.write(f"- 多维表格App Token: {config['app_token']}\n")
            f.write(f"- 表格ID: {config['table_id']}\n")
            
        logger.info(f"生成上传报告: {report_path}")
        return True
    except Exception as e:
        logger.error(f"生成上传报告时出错: {str(e)}")
        return False

def main():
    """
    主函数
    """
    logger.info("===== 开始飞书多维表格批量数据上传 =====")
    
    # 创建必要的目录
    create_dirs()
    
    # 检查配置
    if not check_config():
        return 1
    
    # 获取输出目录
    output_dir = os.path.join('data', 'output')
    if not os.path.exists(output_dir):
        logger.error(f"输出目录不存在: {output_dir}")
        return 1
    
    # 获取账单文件列表
    bill_files = get_bill_files(output_dir)
    
    if not bill_files:
        logger.warning("没有找到需要上传的账单文件")
        return 0
    
    logger.info(f"找到 {len(bill_files)} 个需要上传的账单文件")
    
    # 上传文件
    upload_results = {}
    for filename, file_path in bill_files:
        logger.info(f"开始上传文件: {filename}")
        
        # 读取文件内容
        bill_data = read_csv_file(file_path)
        
        # 上传数据
        result = upload_to_feishu(bill_data, filename)
        upload_results[filename] = result
    
    # 生成上传报告
    generate_upload_report(upload_results)
    
    # 检查是否有上传失败的文件
    if not all(upload_results.values()):
        logger.error("部分文件上传失败，请查看日志")
        return 1
    
    logger.info("===== 所有文件上传完成 =====")
    return 0

if __name__ == "__main__":
    sys.exit(main())
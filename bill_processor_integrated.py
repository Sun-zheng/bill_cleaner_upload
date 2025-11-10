#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
账单处理集成脚本

该脚本整合了支付宝、微信和京东账单的清洗和合并功能，提供统一的处理入口。
"""

import os
import sys
import time
import logging
from datetime import datetime

# 添加data目录到路径，以便导入清洗脚本
sys.path.append(os.path.join(os.path.dirname(__file__), 'data'))

# 导入各平台账单清洗模块
from clean_alipay_bill import clean_alipay_bill
from clean_wechat_bill import clean_wechat_bill
from clean_jingdong_bill import clean_jingdong_bill
from clean_merge_bill import merge_bills

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('data', 'log', 'integrated_processing.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """
    创建必要的目录结构
    """
    directories = [
        os.path.join('data', 'input'),
        os.path.join('data', 'output'),
        os.path.join('data', 'log')
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"创建目录: {directory}")

def read_config():
    """
    读取配置信息
    注意：这里使用默认配置，实际应用中应从配置文件读取
    """
    config = {
        'alipay_input_dir': os.path.join('data', 'input'),
        'alipay_output_dir': os.path.join('data', 'output'),
        'wechat_input_dir': os.path.join('data', 'input'),
        'wechat_output_dir': os.path.join('data', 'output'),
        'jingdong_input_dir': os.path.join('data', 'input'),
        'jingdong_output_dir': os.path.join('data', 'output'),
        'merged_output_dir': os.path.join('data', 'output'),
    }
    return config

def process_alipay_bills(config):
    """
    处理支付宝账单
    """
    logger.info("开始处理支付宝账单...")
    try:
        # 查找支付宝账单文件
        alipay_files = [f for f in os.listdir(config['alipay_input_dir']) 
                       if f.startswith('alipay') and (f.endswith('.csv') or f.endswith('.xls') or f.endswith('.xlsx'))]
        
        if not alipay_files:
            logger.warning("未找到支付宝账单文件")
            return None
        
        # 处理每个支付宝账单文件
        for file in alipay_files:
            file_path = os.path.join(config['alipay_input_dir'], file)
            logger.info(f"处理文件: {file_path}")
            result = clean_alipay_bill(file_path, config['alipay_output_dir'])
            if result:
                logger.info(f"支付宝账单处理成功: {file}")
            else:
                logger.error(f"支付宝账单处理失败: {file}")
                return False
        
        logger.info("支付宝账单处理完成")
        return True
    except Exception as e:
        logger.error(f"处理支付宝账单时出错: {str(e)}")
        return False

def process_wechat_bills(config):
    """
    处理微信账单
    """
    logger.info("开始处理微信账单...")
    try:
        # 查找微信账单文件
        wechat_files = [f for f in os.listdir(config['wechat_input_dir']) 
                       if f.startswith('微信支付账单') and f.endswith('.csv')]
        
        if not wechat_files:
            logger.warning("未找到微信账单文件")
            return None
        
        # 处理每个微信账单文件
        for file in wechat_files:
            file_path = os.path.join(config['wechat_input_dir'], file)
            logger.info(f"处理文件: {file_path}")
            result = clean_wechat_bill(file_path, config['wechat_output_dir'])
            if result:
                logger.info(f"微信账单处理成功: {file}")
            else:
                logger.error(f"微信账单处理失败: {file}")
                return False
        
        logger.info("微信账单处理完成")
        return True
    except Exception as e:
        logger.error(f"处理微信账单时出错: {str(e)}")
        return False

def process_jingdong_bills(config):
    """
    处理京东账单
    """
    logger.info("开始处理京东账单...")
    try:
        # 查找京东账单文件
        jingdong_files = [f for f in os.listdir(config['jingdong_input_dir']) 
                         if (f.startswith('京东') or f.startswith('京东商城')) and \
                            (f.endswith('.csv') or f.endswith('.xls') or f.endswith('.xlsx'))]
        
        if not jingdong_files:
            logger.warning("未找到京东账单文件")
            return None
        
        # 处理每个京东账单文件
        for file in jingdong_files:
            file_path = os.path.join(config['jingdong_input_dir'], file)
            logger.info(f"处理文件: {file_path}")
            result = clean_jingdong_bill(file_path, config['jingdong_output_dir'])
            if result:
                logger.info(f"京东账单处理成功: {file}")
            else:
                logger.error(f"京东账单处理失败: {file}")
                return False
        
        logger.info("京东账单处理完成")
        return True
    except Exception as e:
        logger.error(f"处理京东账单时出错: {str(e)}")
        return False

def generate_processing_report(config, results):
    """
    生成处理报告
    """
    report_path = os.path.join(config['merged_output_dir'], 'integrated_processing_report.md')
    
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 账单集成处理报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## 处理结果\n\n")
            f.write("| 平台 | 状态 |\n")
            f.write("|------|------|\n")
            
            for platform, result in results.items():
                status = "成功" if result else "失败" if result is not None else "未处理（无文件）"
                f.write(f"| {platform} | {status} |\n")
            
            f.write("\n## 说明\n\n")
            f.write("- 处理成功的账单已保存至 `data/output` 目录\n")
            f.write("- 详细日志请查看 `data/log` 目录\n")
            
        logger.info(f"生成处理报告: {report_path}")
        return True
    except Exception as e:
        logger.error(f"生成处理报告时出错: {str(e)}")
        return False

def main():
    """
    主函数
    """
    start_time = time.time()
    logger.info("===== 开始集成账单处理 =====")
    
    # 设置目录结构
    setup_directories()
    
    # 读取配置
    config = read_config()
    
    # 处理各平台账单
    results = {
        '支付宝': None,
        '微信': None,
        '京东': None
    }
    
    results['支付宝'] = process_alipay_bills(config)
    results['微信'] = process_wechat_bills(config)
    results['京东'] = process_jingdong_bills(config)
    
    # 检查是否有成功处理的账单文件
    has_successful_process = any(result for result in results.values() if result)
    
    # 如果有成功处理的账单，执行合并
    if has_successful_process:
        merge_result = merge_bills(config['merged_output_dir'])
        results['账单合并'] = merge_result
    else:
        logger.warning("没有成功处理的账单，跳过合并步骤")
        results['账单合并'] = None
    
    # 生成处理报告
    generate_processing_report(config, results)
    
    end_time = time.time()
    logger.info(f"===== 账单处理完成，耗时: {end_time - start_time:.2f} 秒 =====")
    
    # 检查是否有处理失败的情况
    if any(result is False for result in results.values()):
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
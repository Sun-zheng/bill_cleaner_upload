#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
飞书配置读取工具

该模块提供了读取和验证飞书相关配置的功能，用于账单上传和数据处理。
"""

import os
import logging
import configparser
from typing import Dict, Optional, Any

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FeishuConfig:
    """
    飞书配置类，负责读取和验证飞书API所需的配置信息
    """
    
    def __init__(self, config_file: str = None):
        """
        初始化配置类
        
        Args:
            config_file: 配置文件路径，如果为None则尝试使用默认路径
        """
        self.config_file = config_file or self._get_default_config_path()
        self.config_data = {}
        self.config_parser = configparser.ConfigParser()
        self._load_config()
    
    def _get_default_config_path(self) -> str:
        """
        获取默认配置文件路径
        
        Returns:
            str: 默认配置文件路径
        """
        # 尝试从当前目录和父目录查找config.py
        possible_paths = [
            os.path.join(os.getcwd(), 'config.py'),
            os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), 'config.py')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"找到默认配置文件: {path}")
                return path
        
        logger.warning("未找到默认配置文件，将使用空配置")
        return os.path.join(os.getcwd(), 'config.py')
    
    def _load_config(self) -> None:
        """
        加载配置文件
        """
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"配置文件不存在: {self.config_file}")
                return
            
            # 使用execfile方式加载Python配置文件
            config_dict = {}
            with open(self.config_file, 'r', encoding='utf-8') as f:
                exec(f.read(), config_dict)
            
            # 提取配置信息
            if 'FEISHU_CONFIG' in config_dict:
                self.config_data = config_dict['FEISHU_CONFIG']
                logger.info("成功加载飞书配置")
            else:
                logger.warning("配置文件中未找到FEISHU_CONFIG")
                
        except Exception as e:
            logger.error(f"加载配置文件时出错: {str(e)}")
    
    def get_app_info(self) -> Dict[str, str]:
        """
        获取飞书应用信息
        
        Returns:
            Dict[str, str]: 应用信息字典，包含app_id和app_secret
        """
        app_info = {
            'app_id': self.config_data.get('APP_ID', ''),
            'app_secret': self.config_data.get('APP_SECRET', '')
        }
        
        # 验证应用信息是否完整
        if not all(app_info.values()):
            missing_fields = [k for k, v in app_info.items() if not v]
            logger.warning(f"飞书应用信息不完整，缺少字段: {', '.join(missing_fields)}")
        
        return app_info
    
    def get_bitable_info(self) -> Dict[str, str]:
        """
        获取飞书多维表格信息
        
        Returns:
            Dict[str, str]: 多维表格信息字典，包含app_token和table_id
        """
        bitable_info = {
            'app_token': self.config_data.get('BITABLE_APP_TOKEN', ''),
            'table_id': self.config_data.get('BITABLE_TABLE_ID', '')
        }
        
        # 验证多维表格信息是否完整
        if not all(bitable_info.values()):
            missing_fields = [k for k, v in bitable_info.items() if not v]
            logger.warning(f"多维表格信息不完整，缺少字段: {', '.join(missing_fields)}")
        
        return bitable_info
    
    def get_api_config(self) -> Dict[str, Any]:
        """
        获取API相关配置
        
        Returns:
            Dict[str, Any]: API配置字典
        """
        return {
            'api_url': self.config_data.get('API_URL', 'https://open.feishu.cn/open-apis'),
            'timeout': self.config_data.get('API_TIMEOUT', 30),
            'retry_count': self.config_data.get('API_RETRY_COUNT', 3),
            'retry_interval': self.config_data.get('API_RETRY_INTERVAL', 2)
        }
    
    def get_proxy_config(self) -> Optional[Dict[str, str]]:
        """
        获取代理配置
        
        Returns:
            Optional[Dict[str, str]]: 代理配置字典，如果没有配置则返回None
        """
        proxy_config = self.config_data.get('PROXY_CONFIG', {})
        
        if not proxy_config:
            return None
        
        # 验证代理配置格式
        required_proxy_keys = ['http', 'https']
        if not all(key in proxy_config for key in required_proxy_keys):
            logger.warning(f"代理配置格式不正确，应包含: {', '.join(required_proxy_keys)}")
        
        return proxy_config
    
    def validate_config(self) -> bool:
        """
        验证配置是否完整
        
        Returns:
            bool: 配置是否完整有效
        """
        # 验证应用信息
        app_info = self.get_app_info()
        if not all(app_info.values()):
            return False
        
        # 验证多维表格信息
        bitable_info = self.get_bitable_info()
        if not all(bitable_info.values()):
            return False
        
        logger.info("配置验证通过")
        return True
    
    def update_config(self, key: str, value: Any) -> bool:
        """
        更新配置（运行时更新，不会写入文件）
        
        Args:
            key: 配置键名
            value: 配置值
            
        Returns:
            bool: 更新是否成功
        """
        try:
            self.config_data[key] = value
            logger.info(f"更新配置: {key} = {value}")
            return True
        except Exception as e:
            logger.error(f"更新配置时出错: {str(e)}")
            return False

def get_feishu_config(config_file: str = None) -> FeishuConfig:
    """
    获取飞书配置实例
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        FeishuConfig: 飞书配置实例
    """
    return FeishuConfig(config_file)

def validate_feishu_config(config_file: str) -> bool:
    """
    验证飞书配置文件
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        bool: 配置是否有效
    """
    config = FeishuConfig(config_file)
    return config.validate_config()

def main():
    """
    主函数，用于测试配置读取
    """
    config = get_feishu_config()
    
    print("\n应用信息:")
    print(config.get_app_info())
    
    print("\n多维表格信息:")
    print(config.get_bitable_info())
    
    print("\nAPI配置:")
    print(config.get_api_config())
    
    print("\n代理配置:")
    print(config.get_proxy_config() or "无代理配置")
    
    print(f"\n配置验证结果: {'通过' if config.validate_config() else '失败'}")

if __name__ == "__main__":
    main()
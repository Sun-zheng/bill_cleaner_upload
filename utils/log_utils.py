#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志处理工具

该模块提供了统一的日志配置和处理功能，用于整个项目的日志记录。
"""

import os
import sys
import logging
import datetime
import traceback
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

class LogUtils:
    """
    日志工具类，提供统一的日志配置和管理功能
    """
    
    _default_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    _detailed_format = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    
    def __init__(self, 
                 logger_name: str = None,
                 log_dir: str = None,
                 log_file: str = None,
                 level: int = logging.INFO,
                 max_bytes: int = 10*1024*1024,  # 10MB
                 backup_count: int = 5,
                 use_rotating: bool = True,
                 rotating_when: str = 'midnight',
                 rotating_interval: int = 1):
        """
        初始化日志工具
        
        Args:
            logger_name: 日志器名称
            log_dir: 日志目录
            log_file: 日志文件名
            level: 日志级别
            max_bytes: 单个日志文件最大字节数（用于RotatingFileHandler）
            backup_count: 保留的备份文件数
            use_rotating: 是否使用RotatingFileHandler（否则使用TimedRotatingFileHandler）
            rotating_when: TimedRotatingFileHandler的时间单位（S/M/H/D/W0-W6/midnight）
            rotating_interval: 轮转间隔
        """
        self.logger_name = logger_name or __name__
        self.log_dir = log_dir or self._get_default_log_dir()
        self.log_file = log_file or self._get_default_log_file()
        self.level = level
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.use_rotating = use_rotating
        self.rotating_when = rotating_when
        self.rotating_interval = rotating_interval
        
        # 确保日志目录存在
        self._ensure_log_dir_exists()
        
        # 初始化日志器
        self.logger = self._setup_logger()
    
    def _get_default_log_dir(self) -> str:
        """
        获取默认日志目录
        
        Returns:
            str: 默认日志目录路径
        """
        # 尝试创建logs目录在当前工作目录
        log_dir = os.path.join(os.getcwd(), 'logs')
        
        # 如果没有权限，尝试在临时目录创建
        if not os.access(os.path.dirname(log_dir), os.W_OK):
            log_dir = os.path.join(os.environ.get('TEMP', '/tmp'), 'bill_cleaner_logs')
        
        return log_dir
    
    def _get_default_log_file(self) -> str:
        """
        获取默认日志文件名
        
        Returns:
            str: 默认日志文件名
        """
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        return f"bill_cleaner_{timestamp}.log"
    
    def _ensure_log_dir_exists(self) -> None:
        """
        确保日志目录存在
        """
        if not os.path.exists(self.log_dir):
            try:
                os.makedirs(self.log_dir, exist_ok=True)
                print(f"创建日志目录: {self.log_dir}")
            except Exception as e:
                print(f"无法创建日志目录: {e}")
    
    def _setup_logger(self) -> logging.Logger:
        """
        设置日志器
        
        Returns:
            logging.Logger: 配置好的日志器实例
        """
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.level)
        
        # 避免重复添加处理器
        if logger.handlers:
            logger.handlers.clear()
        
        # 创建格式化器
        formatter = logging.Formatter(self._detailed_format)
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 添加文件处理器
        try:
            log_file_path = os.path.join(self.log_dir, self.log_file)
            
            if self.use_rotating:
                # 使用大小轮转
                file_handler = RotatingFileHandler(
                    log_file_path,
                    maxBytes=self.max_bytes,
                    backupCount=self.backup_count,
                    encoding='utf-8'
                )
            else:
                # 使用时间轮转
                file_handler = TimedRotatingFileHandler(
                    log_file_path,
                    when=self.rotating_when,
                    interval=self.rotating_interval,
                    backupCount=self.backup_count,
                    encoding='utf-8'
                )
            
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            logger.warning(f"无法设置文件日志: {e}")
        
        return logger
    
    def get_logger(self) -> logging.Logger:
        """
        获取日志器实例
        
        Returns:
            logging.Logger: 日志器实例
        """
        return self.logger
    
    def set_level(self, level: int) -> None:
        """
        设置日志级别
        
        Args:
            level: 日志级别
        """
        self.level = level
        self.logger.setLevel(level)
    
    def capture_exception(self, e: Exception, extra: Dict[str, Any] = None) -> None:
        """
        捕获异常并记录详细的异常信息
        
        Args:
            e: 异常对象
            extra: 额外信息
        """
        error_info = {
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc()
        }
        
        if extra:
            error_info.update(extra)
        
        self.logger.error(f"捕获异常: {error_info}")
    
    def log_with_context(self, level: int, message: str, context: Dict[str, Any] = None) -> None:
        """
        记录带上下文信息的日志
        
        Args:
            level: 日志级别
            message: 日志消息
            context: 上下文信息
        """
        if context:
            context_str = " ".join([f"{k}={v}" for k, v in context.items()])
            full_message = f"{message} [{context_str}]"
        else:
            full_message = message
        
        self.logger.log(level, full_message)
    
    def debug(self, message: str, **kwargs) -> None:
        """
        记录调试日志
        """
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """
        记录信息日志
        """
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """
        记录警告日志
        """
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """
        记录错误日志
        """
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """
        记录严重错误日志
        """
        self.logger.critical(message, **kwargs)

# 全局日志实例
_global_logger = None

def get_logger(
    name: str = "bill_cleaner",
    log_dir: str = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    获取全局日志实例
    
    Args:
        name: 日志名称
        log_dir: 日志目录
        level: 日志级别
        
    Returns:
        logging.Logger: 日志器实例
    """
    global _global_logger
    
    if _global_logger is None or _global_logger.logger_name != name:
        _global_logger = LogUtils(
            logger_name=name,
            log_dir=log_dir,
            level=level
        )
    
    return _global_logger.logger

def init_logger(
    name: str = "bill_cleaner",
    log_dir: str = None,
    log_file: str = None,
    level: int = logging.INFO,
    use_rotating: bool = True
) -> LogUtils:
    """
    初始化日志工具
    
    Args:
        name: 日志名称
        log_dir: 日志目录
        log_file: 日志文件名
        level: 日志级别
        use_rotating: 是否使用大小轮转
        
    Returns:
        LogUtils: 日志工具实例
    """
    global _global_logger
    
    _global_logger = LogUtils(
        logger_name=name,
        log_dir=log_dir,
        log_file=log_file,
        level=level,
        use_rotating=use_rotating
    )
    
    return _global_logger

def log_exception(e: Exception, message: str = "发生异常", **kwargs) -> None:
    """
    记录异常信息
    
    Args:
        e: 异常对象
        message: 错误消息
        **kwargs: 额外信息
    """
    logger = get_logger()
    logger.error(f"{message}: {str(e)}", exc_info=True, extra=kwargs)

def log_info(message: str, **kwargs) -> None:
    """
    记录信息日志的快捷函数
    """
    logger = get_logger()
    logger.info(message, **kwargs)

def log_warning(message: str, **kwargs) -> None:
    """
    记录警告日志的快捷函数
    """
    logger = get_logger()
    logger.warning(message, **kwargs)

def log_error(message: str, **kwargs) -> None:
    """
    记录错误日志的快捷函数
    """
    logger = get_logger()
    logger.error(message, **kwargs)

# 默认日志配置
if _global_logger is None:
    _global_logger = LogUtils(logger_name="bill_cleaner")

def main():
    """
    主函数，用于测试日志工具
    """
    # 测试基本日志功能
    log_utils = init_logger(name="test_logger", level=logging.DEBUG)
    logger = log_utils.get_logger()
    
    logger.debug("这是一条调试日志")
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
    
    # 测试异常捕获
    try:
        1 / 0
    except Exception as e:
        log_utils.capture_exception(e, extra={"test_key": "test_value"})
    
    # 测试带上下文的日志
    log_utils.log_with_context(logging.INFO, "带上下文的日志", {"context_key": "context_value"})
    
    print("日志测试完成，请查看logs目录下的日志文件")

if __name__ == "__main__":
    main()
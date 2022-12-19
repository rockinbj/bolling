import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler
import os

from paraConfig import LOG_FILE_LEVEL, LOG_CONSOLE_LEVEL

# 定义log文件和路径
logPath = "log"
logName = "log.current"
logFile = os.path.join(logPath, logName)

# 定义logger总管和总管的级别
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

# 定义logger的记录格式
fmt = '%(asctime)s|%(name)s:%(lineno)4d|%(processName)-9s|%(levelname)-8s %(message)s'
fmt = logging.Formatter(fmt)

# 定义屏幕输出handler
hdlConsole = logging.StreamHandler()
hdlConsole.setLevel(LOG_CONSOLE_LEVEL)
hdlConsole.setFormatter(fmt)

# 定义文件输出handler
hdlFile = ConcurrentRotatingFileHandler(logFile, maxBytes=1024*1024*10, backupCount=30, encoding="utf-8")
hdlFile.setLevel(LOG_FILE_LEVEL)
hdlFile.setFormatter(fmt)

# 将handler添加到logger总管
logger.addHandler(hdlConsole)
logger.addHandler(hdlFile)

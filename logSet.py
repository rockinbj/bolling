import logging, logging.handlers
import os

# 定义log文件和路径
logPath = "log"
logName = "log.current"
logFile = os.path.join(logPath, logName)

# 定义logger总管和总管的级别
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

# 定义logger的记录格式
fmt = '%(asctime)s|%(name)s:%(lineno)4d|%(processName)s|%(levelname)-8s %(message)s'
fmt = logging.Formatter(fmt)

# 定义屏幕输出handler
hdlConsole = logging.StreamHandler()
hdlConsole.setLevel(logging.DEBUG)
hdlConsole.setFormatter(fmt)

# 定义文件输出handler
hdlFile = logging.handlers.TimedRotatingFileHandler(logFile, when="midnight", backupCount=30, encoding="utf8")
hdlFile.setLevel(logging.DEBUG)
hdlFile.setFormatter(fmt)

# 将handler添加到logger总管
logger.addHandler(hdlConsole)
logger.addHandler(hdlFile)

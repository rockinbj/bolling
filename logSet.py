import logging, logging.handlers
import os

logPath = "log"
logName = "test.log"
logFile = os.path.join(logPath, logName)

logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)

fmt = '%(asctime)s|%(name)s:%(lineno)4d|%(threadName)s|%(levelname)-8s %(message)s'
fmt = logging.Formatter(fmt)

hdlConsole = logging.StreamHandler()
hdlConsole.setLevel(logging.DEBUG)
hdlConsole.setFormatter(fmt)

hdlFile = logging.handlers.TimedRotatingFileHandler(logFile, when="midnight", backupCount=30)
hdlFile.setLevel(logging.DEBUG)
hdlFile.setFormatter(fmt)


logger.addHandler(hdlConsole)
logger.addHandler(hdlFile)

import datetime as dt
from functools import partial
from multiprocessing import Pool, current_process

import ccxt
import numpy as np
import pandas as pd
from termcolor import cprint

import exchangeConfig
from functions import *
from logSet import *
from paraConfig import *
from symbolConfig import *

pd.set_option('display.max_rows', 10)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)

logger = logging.getLogger("app.main")


def singalCall(symbolConfig, exId, markets):

    exCfg = getattr(exchangeConfig, exId+"_CONFIG")
    exchange = getattr(ccxt, exId.lower())(exCfg)

    symbol = symbolConfig["symbol"]
    current_process().name = symbol  # 修改线程名称，日志中用来区分
    market = markets[symbol]
    level = symbolConfig["level"]
    strategy = symbolConfig["strategy"]
    para = symbolConfig["para"]
    if strategy=="real_signal_simple_bolling":
        amount = para[0] + 10
    else:
        amount = 10
    
    logger.info(f"{symbol} reading...")
    klinesHistory = getKlines(exchange, symbol, level, amount)
    logger.info(f"{symbol} 获取 {level} 历史k线 {len(klinesHistory)} 根")

    while True:

        symbolInfo = getSymbolInfo(exchange, symbol, market)
        logger.info(f"{symbol} 当前状态:\n{symbolInfo}")

        nextTime = nextStartTime(level, ahead_seconds=AHEAD_SEC)
        logger.info(f"{symbol} 等待当前k线收盘，新k线开始时间 {nextTime}")
        time.sleep(max(0, (nextTime - dt.datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if dt.datetime.now() > nextTime:
                break
        logger.info(f"{symbol} Here we go!")

        klinesNew = getKlines(exchange, symbol, level, NEW_KLINE_NUM)
        logger.info(f"{symbol} 获取 {level} 最新k线 {len(klinesNew)} 根")
        
        klines = pd.concat([klinesHistory.sort_values("openTimeGmt8"),
                            klinesNew.sort_values("openTimeGmt8")],
                            ignore_index=True)
        klines.drop_duplicates(subset=["openTimeGmt8"], keep="last", inplace=True)
        klinesHistory = klines[-amount:]


        symbolInfo = getSignal(symbolInfo, strategy, klines, para)
        signal = symbolInfo.at[symbol, "信号动作"]
        logger.info(f"{symbol} 交易信号: {signal}")
        if signal is not np.nan:
            orderList = placeOrder(exchange, symbolInfo, symbolConfig, market)
            if orderList:
                sendAndPrintInfo(f"{symbol} 订单成交:\n{orderList}")
                symbolInfo = getSymbolInfo(exchange, symbol, market)
                logger.info(f"{symbol} 更新成交后的状态:\n{symbolInfo}")
    
        logger.info(f"{'==='*5}{symbol} 本轮结束{'==='*5}")
        time.sleep(SLEEP_LONG)


def main():

    exId = EXCHANGE
    exCfg = getattr(exchangeConfig, exId+"_CONFIG")
    ex = ccxt.binance(exCfg)
    mks = ex.loadMarkets()

    multiCall = partial(singalCall, exId=exId, markets=mks)
    # pool = Pool(len(SYMBOLS_CONFIG))
    with Pool(len(SYMBOLS_CONFIG)) as pool:
        r = pool.map_async(multiCall, SYMBOLS_CONFIG)
        
        while True:
            sendReport(ex, mks, REPORT_INTERVAL)
            time.sleep(1)
                

if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        logger.exception(e)

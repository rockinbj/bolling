from functools import partial
from multiprocessing import Pool, cpu_count

import ccxt
import pandas as pd
import numpy as np
from termcolor import cprint

import exchangeConfig
from configs import *
from functions import *


def singalCall(symbolConfig, exId, markets):
    exCfg = getattr(exchangeConfig, exId+"_CONFIG")
    exchange = getattr(ccxt, exId.lower())(exCfg)

    symbol = symbolConfig["symbol"]
    market = markets[symbol]
    level = symbolConfig["level"]
    strategy = symbolConfig["strategy"]
    para = symbolConfig["para"]
    if strategy=="real_signal_simple_bolling":
        amount = para[0] + 10
    else:
        amount = 10
      
    klinesHistory = getKlines(exchange, symbol, level, amount)
    print(f"{symbol} 获取历史k线 {len(klinesHistory)} 根")

    while True:
        symbolInfo = getSymbolInfo(exchange, symbol, market)
        print(f"{symbol} 当前状态:\n{symbolInfo}")

        nextTime = nextStartTime(level, ahead_seconds=AHEAD_SEC)
        print(f"{symbol} 等待当前k线收盘，新k线开始时间 {nextTime}")
        time.sleep(max(0, (nextTime - dt.datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if dt.datetime.now() > nextTime:
                break
        cprint(f"{symbol} Here we go!\n", "blue")

        klinesNew = getKlines(exchange, symbol, level, NEW_KLINE_NUM)
        print(f"{symbol} 获取最新k线 {len(klinesNew)} 根")
        
        klines = pd.concat([klinesHistory.sort_values("openTimeGmt8"),
                            klinesNew.sort_values("openTimeGmt8")],
                            ignore_index=True)
        klines.drop_duplicates(subset=["openTimeGmt8"], keep="last", inplace=True)


        symbolInfo = getSignal(symbolInfo, strategy, klines, para)
        signal = symbolInfo.at[symbol, "信号动作"]
        cprint(f"{symbol} 交易信号: {signal}", "green")
        if signal is not np.nan:
            orderStatus = placeOrder(exchange, symbolInfo, symbolConfig, market)
            if orderStatus:
                sendAndPrint(f"{symbol} 下单成功:\n{orderStatus}")
                symbolInfo = getSymbolInfo(exchange, symbol, market)
                print(f"{symbol} 更新成交后的状态:\n{symbolInfo}")
    
        print(f"{'==='*5}{symbol} 本轮结束{'==='*5}\n\n")
        sendReport(symbolInfo)
        time.sleep(SLEEP_LONG)


def main():

    exId = EXCHANGE
    exCfg = getattr(exchangeConfig, exId+"_CONFIG")
    ex = ccxt.binance(exCfg)
    mks = ex.loadMarkets()

    multiCall = partial(singalCall, exId=exId, markets=mks)
    pool = Pool(min(len(SYMBOLS_CONFIG), cpu_count()))
    pool.map(multiCall, SYMBOLS_CONFIG)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError:
        exit()
    except ccxt.RequestTimeout:
        print("timeout")
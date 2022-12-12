import datetime as dt
import time
from traceback import format_exc

import ccxt
import pandas as pd
import requests
from tenacity import *
from termcolor import cprint, colored

import signals
from paraConfig import *
from symbolConfig import *
from logSet import *

pd.set_option('display.max_rows', 200)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)

logger = logging.getLogger("app.func")


@retry(stop=stop_after_attempt(2), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=lambda retry_state: logger.exception("sendMixin() Failed."))
def sendMixin(msg, _type="PLAIN_TEXT"):
    token = MIXIN_TOKEN
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"
    value = {
        'category': _type,
        'data': msg,
        }
    
    r = requests.post(url, data=value, timeout=2).json()
    if r["success"] is False:
        logger.warning(f"Mixin failure: {r.text}")


def sendAndPrintInfo(msg):
    logger.info(msg)
    sendMixin(msg)


def sendAndPrintError(msg):
    logger.error(msg)
    sendMixin(msg)


def sendAndRaise(msg):
    logger.error(msg)
    sendMixin(msg)
    raise RuntimeError(msg)


def sendReport(exchange, symbolMarkets, interval=REPORT_INTERVAL):
    symbolInfoList = pd.DataFrame()
    nowMinute = dt.datetime.now().minute
    nowSecond = dt.datetime.now().second
    if (nowMinute%interval==0) and (nowSecond==47):
        for symbolConfig in SYMBOLS_CONFIG:
            symbol = symbolConfig["symbol"]
            mkt = symbolMarkets[symbol]
            symbolInfo = getSymbolInfo(exchange, symbol, mkt)
            symbolInfoList = pd.concat([symbolInfoList, symbolInfo])
        
        symbolInfoList.fillna("-", inplace=True)
        siDict = symbolInfoList.to_dict(orient="index")
        msg = ""
        for symbol in siDict.keys():
            msg += f"{symbol}:\n"
            for k,v in siDict[symbol].items():
                msg += f"    {k}: {v}\n"

        sendMixin(f"{'=='*3}持仓报告{'=='*3}\n\n{msg}")
    

def retryCallback(retry_state):
    name = getattr(retry_state.fn, '__name__')
    retryTimes = retry_state.attempt_number
    paras = retry_state.args
    errorStr = retry_state.outcome
    msg = f"失败退出:\n{name}()重试{retryTimes}次无效，币种线程退出。请检查。\n传入参数：{paras}\n报错信息：\n{errorStr}"
    msg = colored(msg, "red")
    sendAndPrintError(msg)
    logger.exception(msg)
    retry_state.outcome.result()


def secondsToNext(exchange, level):
    levelSeconds = exchange.parseTimeframe(level.lower())
    now = int(time.time())
    seconds = levelSeconds - (now % levelSeconds)
    
    return seconds


def nextStartTime(level, ahead_seconds=3):
    # ahead_seconds为预留秒数，
    # 当离开始时间太近，本轮可能来不及下单，因此当离开始时间的秒数小于预留秒数时，
    # 就直接顺延至下一轮开始
    if level.endswith('m') or level.endswith('h'):
        pass
    elif level.endswith('T'):
        level = level.replace('T', 'm')
    elif level.endswith('H'):
        level = level.replace('H', 'h')
    else:
        sendAndRaise("level格式错误。程序退出。")

    ti = pd.to_timedelta(level)
    now_time = dt.datetime.now()
    # now_time = dt.datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = dt.timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    return target_time


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getSymbolMarket(exchange, symbol):
    mks = exchange.loadMarkets()
    return mks[symbol]


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getKlines(exchange, symbol, level, amount):
    now = int(time.time() * 1000)
    levelMs = exchange.parseTimeframe(level.lower()) * 1000

    kEnd = now - levelMs
    kSince = kEnd - levelMs * amount

    klineAll = pd.DataFrame()
    while True:

        klines = exchange.fetchOHLCV(symbol, level, since=kSince, limit=1500)
        klines = pd.DataFrame(klines, columns=[
            "openTime", "open", "high", "low", "close", "volume"])
        
        klineAll = pd.concat([klineAll, klines], ignore_index=True)

        if klines["openTime"].iloc[-1] > kEnd:
            break
        else:
            kSince = klines["openTime"].iloc[-1]
            time.sleep(SLEEP_SHORT)

    klineAll.drop_duplicates(subset="openTime", keep="last", inplace=True)
    klineAll.sort_values(by="openTime", inplace=True)
    klineAll["openTimeGmt8"] = pd.to_datetime(klineAll["openTime"], unit="ms") + dt.timedelta(hours=8)
    klineAll = klineAll[["openTimeGmt8", "open", "high", "low", "close", "volume"]]
    klineAll = klineAll[:-1]
    
    return klineAll


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getAccount(exchange):
    account = exchange.fapiPrivateGetAccount()
    return account


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getPosition(exchange, symbolMarket):
    position = exchange.fapiPrivateGetPositionRisk({
        "symbol": symbolMarket["id"]
    })
    return position


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getBalance(exchange, quote="USDT"):
    return exchange.fetchBalance()["free"][quote.upper()]


def getPrecision(symbolMarket):
    prePrice = symbolMarket["precision"]["price"]
    preAmount = symbolMarket["precision"]["amount"]
    return prePrice, preAmount


def getMinNotional(exchange, symbolMarket):
    symbolId = symbolMarket["id"]


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def hasPosition(exchange, symbolMarket):
    symbolId = symbolMarket["id"]
    r = exchange.fapiPrivateGetPositionrisk({"symbol":symbolId})
    r = pd.DataFrame(r)
    r = r.astype({"positionAmt":"float"})
    r.drop(r.loc[r["positionAmt"]==0].index, inplace=True)
    
    return False if r.empty else True


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getSymbolInfo(exchange, symbol, symbolMarket):
    # cols = [
    #     "账户权益", "账户收益", "账户余额",
    #     "交易对", "持仓方向", "持仓收益", "持仓收益率",
    #     "持仓数量", "持仓价值",
    #     "持仓均价", "当前价格", "爆仓价格",
    #     "杠杆倍数", "担保模式",
    #     "价格精度", "数量精度",
    #     "信号价格", "信号时间", "信号方向", "信号动作"
    # ]

    symbolId = symbolMarket["id"]
    account = getAccount(exchange)
    symbolInfo = pd.DataFrame()    
    symbolInfo.loc[symbol, "账户权益"] = round(float(account["totalWalletBalance"]), 4)
    symbolInfo.loc[symbol, "账户收益"] = round(float(account["totalUnrealizedProfit"]), 4)
    symbolInfo.loc[symbol, "账户余额"] = round(float(account["availableBalance"]), 4)
    symbolInfo[["账户权益", "账户收益", "账户余额"]] = symbolInfo[["账户权益", "账户收益", "账户余额"]].astype("float64")
    symbolInfo.loc[symbol, "交易对"] = symbolId
    symbolInfo.loc[symbol, "持仓方向"] = 0
    symbolInfo["持仓方向"] = symbolInfo["持仓方向"].astype("int64")
    symbolInfo.loc[symbol, "信号动作"] = None
    symbolInfo["信号动作"] = symbolInfo["信号动作"].astype("object")
    symbolInfo.at[symbol, "信号动作"] = []
    
    precisionPrice, precisionAmount = getPrecision(symbolMarket)
    symbolInfo.loc[symbol, "价格精度"] = precisionPrice
    symbolInfo.loc[symbol, "数量精度"] = precisionAmount
    symbolInfo[["价格精度", "数量精度"]] = symbolInfo[["价格精度", "数量精度"]].astype("int64")

    if hasPosition(exchange, symbolMarket):
        position1 = pd.DataFrame(account["positions"])
        position1["positionAmt"] = position1["positionAmt"].astype("float64")
        position1.drop(position1.loc[position1["positionAmt"]==0].index, inplace=True)
        position1.set_index(["symbol"], inplace=True)
        position1.index.name = None
        symbolInfo.loc[symbol, "持仓方向"] = 1 if position1.at[symbolId, "positionAmt"] > 0 else -1
        
        position2 = pd.DataFrame(getPosition(exchange, symbolMarket))
        position2["positionAmt"] = position2["positionAmt"].astype("float64")
        position2.drop(position2.loc[position2["positionAmt"]==0].index, inplace=True)
        position2.set_index(["symbol"], inplace=True)
        position2.index.name = None

        symbolInfo.loc[symbol, "持仓收益"] = round(float(position2.at[symbolId, "unRealizedProfit"]), 4)
        roe = float(position1.at[symbolId, "unrealizedProfit"]) / float(position1.at[symbolId, "initialMargin"])
        roe = f"{round(roe*100,2)}%"
        symbolInfo.loc[symbol, "持仓收益率"] = roe
        symbolInfo.loc[symbol, "持仓数量"] = position2.at[symbolId, "positionAmt"]
        symbolInfo.loc[symbol, "持仓价值"] = round(abs(float(position2.at[symbolId, "notional"])), 4)
        symbolInfo.loc[symbol, "持仓均价"] = position2.at[symbolId, "entryPrice"]
        symbolInfo.loc[symbol, "当前价格"] = round(float(position2.at[symbolId, "markPrice"]), precisionPrice)
        symbolInfo.loc[symbol, "爆仓价格"] = round(float(position2.at[symbolId, "liquidationPrice"]), precisionPrice)
        symbolInfo.loc[symbol, "杠杆倍数"] = position2.at[symbolId, "leverage"]
        symbolInfo["杠杆倍数"] = symbolInfo["杠杆倍数"].astype("int64")
        symbolInfo.loc[symbol, "担保模式"] = position2.at[symbolId, "marginType"]

    return symbolInfo


def getSignal(symbolInfo, signalName, klines, paras):
    symbol = symbolInfo.index[0]
    now = symbolInfo.at[symbol, "持仓方向"]
    new = getattr(signals, signalName)(klines, paras)
    logger.debug(f"{symbol} signal: now: {now}, new:{new}")
    if now==0 and new==1:
        signal = [1]
    elif now==0 and new==-1:
        signal = [2]
    elif now==1 and new==0:
        signal = [3]
    elif now==-1 and new==0:
        signal = [4]
    elif now==1 and new==-1:
        signal = [3, 2]
    elif now==-1 and new==1:
        signal = [4, 1]
    else:
        signal = []
    # print(f"signal: {signal}")
    if signal:
        symbolInfo.at[symbol, "信号方向"] = int(new)
        symbolInfo["信号方向"] = symbolInfo["信号方向"].astype("int64")
        symbolInfo.at[symbol, "信号动作"] = signal
        symbolInfo.at[symbol, "信号时间"] = dt.datetime.utcnow()
    return symbolInfo


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def setMarginType(exchange, symbolId, _type=1):
    if _type==1:
        t = "CROSSED"
    elif _type==2:
        t = "ISOLATED"
    
    p = {
        "symbol": symbolId,
        "marginType": t,
    }

    try:
        exchange.fapiPrivatePostMargintype(p)
    except ccxt.MarginModeAlreadySet:
        pass


def getOrderPrice(symbolInfo, symbolConfig, signalAction):
    symbol = symbolInfo.index[0]
    slippage = symbolConfig["slippage"]
    price = symbolInfo.at[symbol, "信号价格"]
    precision = symbolInfo.at[symbol, "价格精度"]
    
    if signalAction in [1, 4]:
        orderPrice = price * (1 + slippage)
    elif signalAction in [2, 3]:
        orderPrice = price * (1 - slippage)
    
    orderPrice = int(orderPrice * (10**precision)) / (10**precision)
    # print(f"symbol:{symbol}, slippage:{slippage}, price:{price}, pre:{precision}, oP:{orderPrice}")
    return orderPrice


def getOrderSize(symbolInfo, symbolConfig, symbolMarket):
    symbol = symbolInfo.index[0]
    minNotional = symbolMarket["limits"]["cost"]["min"]

    hasPosition = True if "持仓数量" in symbolInfo.columns else False

    if hasPosition:
        return abs(symbolInfo.at[symbol, "持仓数量"])
    
    else:
        balance = symbolInfo.at[symbol, "账户余额"]
        volatility = symbolConfig["volatility"]
        balance = balance * volatility

        price = symbolInfo.at[symbol, "信号价格"]
        minSize = minNotional / price
        
        weight = symbolConfig["weight"]
        leverage = symbolConfig["leverage"]

        # 最小下单金额为5u
        size = max(balance * leverage * weight / price, 5*leverage/price)
        precision = symbolInfo.at[symbol, "数量精度"]
        size = int(size * (10**precision)) / (10**precision)
        # print(f"symbol:{symbol}, volatility:{volatility}, price:{price}, pre:{precision}, size:{size}, min:{round(0.1**precision, precision)}, minNtl:{minNotional}, minSize:{minSize}")
        if precision==0:
            size = int(size)
            return max(size, 1, int(minSize))
        else:
            return max(size, round(0.1**precision, precision), round(minSize, precision))


@retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
        retry_error_callback=retryCallback)
def getOrderStatus(exchange, symbolId, orderId):
    return exchange.fapiPrivateGetOrder({
        "symbol": symbolId,
        "orderId": orderId,
    })["status"]


# @retry(stop=stop_after_attempt(3), wait=wait_fixed(SLEEP_SHORT), reraise=True,
#         retry=retry_if_not_exception_type(RuntimeError),
#         retry_error_callback=retryCallback)                  
def placeOrder(exchange, symbolInfo, symbolConfig, symbolMarket):
    symbol = symbolInfo.index[0]
    symbolId = symbolInfo.at[symbol, "交易对"]
    signals = symbolInfo.at[symbol, "信号动作"]

    setMarginType(exchange, symbolId, _type=1)
    exchange.setLeverage(symbolConfig["leverage"], symbol)

    orderList = []
    for s in signals:

        price = exchange.fetchTicker(symbol)["last"]
        symbolInfo.at[symbol, "信号价格"] = price

        if len(signals) == 2 and s in [1, 2]:
            symbolInfo.at[symbol, "账户余额"] = getBalance(exchange, quote="USDT")

        p = {
            "symbol": symbolId,
            "side": 'BUY' if s in [1, 4] else 'SELL',
            "positionSide": 'LONG' if s in [1, 3] else 'SHORT',
            "type": "LIMIT",
            "price": getOrderPrice(symbolInfo, symbolConfig, s),
            "quantity": getOrderSize(symbolInfo, symbolConfig, symbolMarket),
            "newClientOrderId": f"Rock{exchange.milliseconds()}",
            "workingType": "MARK_PRICE",
            "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
        }
        logger.debug(f"本次下单参数: {p}")

        try:
            orderInfo = exchange.fapiPrivatePostOrder(p)
            orderId = orderInfo["orderId"]
        except Exception as e:
            logger.exception(e)
            sendAndRaise(f"报错订单信息：{p}\n报错信息：{format_exc()}")

        time.sleep(SLEEP_SHORT)
                
        for i in range(MAX_TRY):
            orderStatue = exchange.fapiPrivateGetOrder({
                "symbol": symbolId,
                "orderId": orderId,
            })
            if orderStatue["status"] == "FILLED":
                orderList.append(orderStatue)
                break
            else:
                if i == MAX_TRY - 1:
                    sendAndRaise("订单状态一直未成交FILLED，程序退出，请检查。")
                time.sleep(SLEEP_SHORT)
    return orderList


if __name__ == "__main__":
    from paraConfig import *
    ex = ccxt.binance(
        {
            "options": {
                "defaultType":"future",
            },
            "apiKey": "cNiKjnf1uwkmjKqsYwI2mjQ61OyAQFA2vEEMWurMXRxb0IwciDQfj7Jmaam8qeJ2",
            "secret": "mfXIJz8gDBY0EOe83ZAI9kQYH0KqSoQTTkVpz1PDad6uc13RCHdhCYmSDacau9Uo",
        }
    )
    symbol = "DOGE/USDT"
    level = "1m"
    mkt = ex.loadMarkets()[symbol]
    symbolInfo = getSymbolInfo(ex, symbol, mkt)
    symbolConfig = {
        "symbol": "DOGE/USDT",
        "weight": 0.3,
        "leverage": 3,
        # "strategy": "real_signal_none",
        "strategy": "real_signal_random",
        # "strategy": "real_signal_simple_bolling",
        "level": "1m",
        "para": [395, 1.4],
        "slippage": 0.02,
        "volatility": 0.02,
    }

    sendMixin("# test")


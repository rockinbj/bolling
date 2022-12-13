import random
import pandas as pd
from logSet import *

pd.set_option('display.max_rows', 200)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)

logger = logging.getLogger("app.signal")


# 将None作为信号返回
def real_signal_none(df, para):
    """
    发出空交易信号
    :param df:
    :param para:
    :return:
    """

    return None


# 随机生成交易信号
def real_signal_random(df,  para=[200,2]):
    """
    随机发出交易信号
    :param df:
    :param para:
    :return:
    """

    r = random.random()
    if r <= 0.4:
        return 1
    elif r <= 0.8:
        return -1
    elif r <= 1:
        return 0
    else:
        return None


# 简单布林实盘信号
def real_signal_simple_bolling(df, para=[200, 2]):
    """
    实盘产生布林线策略信号的函数，和历史回测函数相比，计算速度更快。
    布林线中轨：n天收盘价的移动平均线
    布林线上轨：n天收盘价的移动平均线 + m * n天收盘价的标准差
    布林线上轨：n天收盘价的移动平均线 - m * n天收盘价的标准差
    当收盘价由下向上穿过上轨的时候，做多；然后由上向下穿过中轨的时候，平仓。
    当收盘价由上向下穿过下轨的时候，做空；然后由下向上穿过中轨的时候，平仓。
    :param df:  原始数据
    :param para:  参数，[n, m]
    :return:
    """

    # ===策略参数
    # n代表取平均线和标准差的参数
    # m代表标准差的倍数
    n = int(para[0])
    m = para[1]

    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(n).mean()  # 此处只计算最后几行的均线值，因为没有加min_period参数
    median = df.iloc[-1]['median']
    median2 = df.iloc[-2]['median']
    # 计算标准差
    df['std'] = df['close'].rolling(n).std(ddof=0)  # ddof代表标准差自由度，只计算最后几行的均线值，因为没有加min_period参数
    std = df.iloc[-1]['std']
    std2 = df.iloc[-2]['std']
    # 计算上轨、下轨道
    upper = median + m * std
    lower = median - m * std
    upper2 = median2 + m * std2
    lower2 = median2 - m * std2

    # ===寻找交易信号
    signal = None
    close = df.iloc[-1]['close']
    close2 = df.iloc[-2]['close']
    # 找出做多信号
    if (close > upper) and (close2 <= upper2):
        signal = 1
    # 找出做空信号
    elif (close < lower) and (close2 >= lower2):
        signal = -1
    # 找出做多平仓信号
    elif (close < median) and (close2 >= median2):
        signal = 0
    # 找出做空平仓信号
    elif (close > median) and (close2 <= median2):
        signal = 0

    return signal


# 布林延迟开仓
def real_signal_BollingDelay(df, para):
    # para:
    # [maLength, times, percent]
    # [400, 2, 3]
    # 产生开仓信号，并且，上轨或者下轨距离中轨的距离要小于percent，才开仓
    
    maLength = para[0]
    times = para[1]
    percent = para[2] / 100

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    df["ma"] = df["close"].rolling(maLength).mean()
    df["stdDev"] = df["close"].rolling(maLength).std(ddof=0)
    df["upper"] = df["ma"] + times * df["stdDev"]
    df["lower"] = df["ma"] - times * df["stdDev"]
    df["dif"] = abs(df["close"] / df["ma"] - 1)

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = df["close"].shift(1) <= df["upper"].shift(1)
    condLong2 = df["close"] > df["upper"]
    df.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = df["close"].shift(1) >= df["ma"].shift(1)
    condCoverLong2 = df["close"] < df["ma"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = df["close"].shift(1) >= df["lower"].shift(1)
    condShort2 = df["close"] < df["lower"]
    df.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = df["close"].shift(1) <= df["ma"].shift(1)
    condCoverShort2 = df["close"] > df["ma"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    # 填充signal的空白
    # df["signal"].fillna(method="ffill", inplace=True)
    # df["signal"].fillna(value=0, inplace=True)
    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]

    # 修改开仓信号，增加延迟开仓的约束
    df["signal2"] = df["signal"]
    # 复制一个新的信号列，并复制填充，形成一个信号向下连续的信号列
    df["signal2"].fillna(method="ffill", inplace=True)
    # 把原信号列中1、-1的信号清空，等待用复制列的信号反填充回来
    df.loc[df["signal"]!=0, "signal"] = None
    # 只有在满足percent约束且是1、-1信号时，才将复制信号列反填充回来
    # 由于新的信号列经过了ffill的填充，也就实现了向下复制信号的效果
    cond1 = df["signal2"] == 1
    cond2 = df["signal2"] == -1
    df.loc[(cond1 | cond2)&(df["dif"]<=percent), "signal"] = df["signal2"]

    df['signal'].fillna(method='ffill', inplace=True)
    # df['signal'].fillna(value=0, inplace=True)

    logger.debug(f"signal:\n{df}")
    
    return df.iloc[-1]["signal"]

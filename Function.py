import math
import time
import traceback
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import requests
from termcolor import cprint

import Signals
from Config import *
from Signals import *


# =====okex交互函数
# ===通过ccxt、交易所接口获取合约账户余额
def ccxt_fetch_future_account(exchange, max_try_amount=MAX_TRY):
    """
    :param exchange:
    :param max_try_amount:
    :return:

    本程序使用okex5中"获取资金账户余额"、"查看持仓信息"接口，获取账户USDT的余额与持仓信息。
    使用ccxt函数：private_get_account_balance() 与 private_get_account_positions()
    """
    for _ in range(max_try_amount):
        try:

            balance_of = float(exchange.fetchBalance()["USDT"]["free"])
            return balance_of
        except Exception as e:
            print('通过ccxt的通过futures_get_accounts获取所有合约账户余额，失败，稍后重试：\n', e)
            traceback.print_exc()
            time.sleep(medium_sleep_time)

    _ = '通过ccxt的通过futures_get_accounts获取合约账户余额，失败次数过多，程序Raise Error'
    send_dingding_and_raise_error(_)


# ===通过ccxt、交易所接口获取合约账户持仓信息
def ccxt_fetch_future_position(exchange, symbol_config, max_try_amount=MAX_TRY):
    """
    :param exchange:
    :param max_try_amount:
    :return:
    """
    postions = pd.DataFrame()
    for symbol in symbol_config:

        for _ in range(max_try_amount):
            try:
                # 获取持仓数据
                df = pd.DataFrame(exchange.fapiPrivate_get_positionrisk({"symbol": symbol_config[symbol]["instrument_id"]}))
                df['instrument_id'] = df['symbol']
                df["symbol"] = symbol
                
                # 币安没有单独的账户持仓api，他会返回所有symbol的所有方向的持仓状态，因此需要删掉实际上没有仓位的返回值
                # 必须先去除空仓位的行，再整理数据。因为整理数据时会把index设为symbol，所有行的index是相同的，drop会删除所有行
                df = df.astype({"positionAmt":"float"})  # 返回的持仓量是字符串，先转换成float
                df.drop(df.loc[df["positionAmt"]==0].index, axis=0, inplace=True)

                # 此时只剩下一条真正有仓位的行，用symbol做index
                df.set_index("symbol", drop=True, inplace=True)
                df.index.name = None

                postions = pd.concat([postions, df])
                break
            except Exception as e:
                print('通过ccxt的通过futures_get_position获取所有合约的持仓信息，失败，稍后重试。失败原因：\n', e)
                traceback.print_exc()
                time.sleep(medium_sleep_time)

            _ = '通过ccxt的通过futures_get_position获取所有合约的持仓信息，失败次数过多，程序Raise Error'
            send_dingding_and_raise_error(_)
    
    return postions


# ===通过ccxt获取K线数据
def ccxt_fetch_candle_data(exchange, symbol, time_interval, limit, max_try_amount=MAX_TRY):
    """
    本程序使用ccxt的fetch_ohlcv()函数，获取最新的K线数据，用于实盘
    :param exchange:
    :param symbol:
    :param time_interval:
    :param limit:
    :param max_try_amount:
    :return:
    """
    for _ in range(max_try_amount):
        try:
            # 获取数据
            data = exchange.fetchOHLCV(symbol=symbol, timeframe=time_interval, limit=limit)
            # data = exchange.publicGetMarketCandles({
            #     'instId': symbol,
            #     'bar': time_interval,
            #     'limit': limit,
            # })['data']
            # 整理数据
            df = pd.DataFrame(data, dtype=float)
            df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                               3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
            df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
            df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
            df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]
            return df
        except Exception as e:
            print('获取fetch_ohlcv获取合约K线数据，失败，稍后重试。失败原因：\n', e)
            traceback.print_exc()
            time.sleep(short_sleep_time)

    _ = '获取fetch_ohlcv合约K线数据，失败次数过多，程序Raise Error'
    send_dingding_and_raise_error(_)
    

# =====趋势策略相关函数
# 根据账户信息、持仓信息，更新symbol_info
def update_symbol_info(exchange, symbol_info, symbol_config):
    """
    本函数通过private_get_account_balance()获取账户信息，private_get_account_positions()获取账户持仓信息，并用这些信息更新symbol_config
    :param exchange:
    :param symbol_info:
    :param symbol_config:
    :return:
    """
    # 初始化当前持仓方向，默认为没有持仓
    symbol_info['当前持仓方向'] = 0

    # 通过交易所接口获取整个合约账户的USDT余额，因为用全仓所以共用USDT余额
    symbol_info['账户余额'] = ccxt_fetch_future_account(exchange)

    # 获取币种精度数据
    symbol_info[["价格精度", "数量精度"]] = getPrecision(exchange, symbol_config)

    # 通过交易所接口获取合约账户持仓信息
    future_position = ccxt_fetch_future_position(exchange, symbol_config)

    # 将持仓信息和symbol_info合并
    if not future_position.empty:
        # 去除无关持仓：账户中可能存在其他合约的持仓信息，这些合约不在symbol_config中，将其删除。
        # instrument_id_list = [symbol_config[x]['instrument_id'] for x in symbol_config.keys()]
        # future_position = future_position[future_position.instrument_id.isin(instrument_id_list)]

        # 从future_position中获取原始数据
        symbol_info['持仓价值'] = future_position['notional']
        symbol_info['杠杆倍数'] = future_position['leverage']
        symbol_info['标记价格'] = future_position['markPrice']
        symbol_info['爆仓价格'] = future_position['liquidationPrice']
        symbol_info['持仓数量'] = future_position['positionAmt']
        symbol_info['持仓均价'] = future_position['entryPrice']
        # symbol_info['持仓收益率'] = future_position['percentage']
        symbol_info['持仓收益'] = future_position['unRealizedProfit']
        symbol_info["保证金模式"] = future_position["marginType"]

        # 当账户是买卖模式的时候,接口返回的持仓数量负数为做空,正数为做多
        symbol_info['pos'] = future_position['positionAmt']
        symbol_info.loc[symbol_info['pos'] < 0, '当前持仓方向'] = -1
        symbol_info.loc[symbol_info['pos'] > 0, '当前持仓方向'] = 1
        del symbol_info['pos']

        # 检验是否同时持有多头和空头, 买卖模式不会存在同时多头和空头,这里理论来说可以去掉
        # if len(future_position[future_position.duplicated('instrument_id')]) > 1:
        #     print(symbol_info['instrument_id'], '当前账户同时存在多仓和空仓，请平掉其中至少一个仓位后再运行程序，程序exit')
        #     exit()

    return symbol_info


# 获取需要的K线数据，并检测质量。
def get_candle_data(exchange, symbol_config, time_interval, run_time, max_try_amount, candle_num, symbol):
    """
    使用ccxt_fetch_candle_data(函数)，获取指定交易对最新的K线数据，并且监测数据质量，用于实盘。
    :param exchange:
    :param symbol_config:
    :param time_interval:
    :param run_time:
    :param max_try_amount:
    :param symbol:
    :param candle_num:
    :return:
    尝试获取K线数据，并检验质量
    """
    # 标记开始时间
    # start_time = datetime.now()
    # print('开始获取K线数据：', symbol, '开始时间：', start_time)

    # 获取数据合约的相关参数
    instrument_id = symbol_config[symbol]["instrument_id"]  # 合约id
    signal_price = None

    # 尝试获取数据
    for i in range(max_try_amount):
        # 获取symbol该品种最新的K线数据
        df = ccxt_fetch_candle_data(exchange, instrument_id, time_interval, limit=candle_num)
        if df.empty:
            continue  # 再次获取

        # 判断是否包含最新一根的K线数据。例如当time_interval为15分钟，run_time为14:15时，即判断当前获取到的数据中是否包含14:15这根K线
        # 【其实这段代码可以省略】
        if time_interval.endswith('m') or time_interval.endswith('M'):
            _ = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval[:-1])))]
        elif time_interval.endswith('h') or time_interval.endswith('H'):
            _ = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(hours=int(time_interval[:-1])))]
        else:
            print('time_interval不以m或者h结尾，出错，程序exit')
            exit()
        if _.empty:
            print('获取数据不包含最新的数据，重新获取')
            time.sleep(short_sleep_time)
            continue  # 再次获取

        else:  # 获取到了最新数据
            signal_price = df.iloc[-1]['close']  # 该品种的最新价格
            df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]  # 去除run_time周期的数据
            # print('结束获取K线数据', symbol, '结束时间：', datetime.now())
            # print(df.tail(2))
            return symbol, df, signal_price

    print('获取candle_data数据次数超过max_try_amount，数据返回空值')
    return symbol, pd.DataFrame(), signal_price


# 串行获取K线数据
def single_threading_get_data(exchange, symbol_info, symbol_config, time_interval, run_time, candle_num,
                              max_try_amount=MAX_TRY):
    """
    串行逐个获取所有交易对的K线数据，速度较慢
    若获取数据失败，返回空的dataframe。
    :param exchange:
    :param symbol_info:
    :param symbol_config:
    :param time_interval:
    :param run_time:
    :param candle_num:
    :param max_try_amount:
    :return:
    """
    # 函数返回的变量
    symbol_candle_data = {}
    for symbol in symbol_config.keys():
        symbol_candle_data[symbol] = pd.DataFrame()

    # 逐个获取symbol对应的K线数据
    for symbol in symbol_config.keys():
        _, symbol_candle_data[symbol], symbol_info.at[symbol, '信号价格'] = get_candle_data(exchange, symbol_config,
                                                                                            time_interval, run_time,
                                                                                            max_try_amount, candle_num,
                                                                                            symbol)

    return symbol_candle_data


# 根据最新数据，计算最新的signal
def calculate_signal(symbol_info, symbol_config, symbol_candle_data):
    """
    计算交易信号
    :param symbol_info:
    :param symbol_config:
    :param symbol_candle_data:
    :return:
    """

    # 输出变量
    symbol_signal = {}

    # 逐个遍历交易对
    for symbol in symbol_config.keys():

        # 赋值相关数据
        df = symbol_candle_data[symbol].copy()  # 最新数据
        now_pos = symbol_info.at[symbol, '当前持仓方向']  # 当前当前持仓方向
        # avg_price = symbol_info.at[symbol, '持仓均价']  # 当前持仓均价

        # 需要计算的目标仓位
        target_pos = None

        # 根据策略计算出目标交易信号
        if not df.empty:  # 当原始数据不为空的时候
            target_pos = getattr(Signals, symbol_config[symbol]['strategy_name'])(df, para=symbol_config[symbol]['para'])
        symbol_info.at[symbol, '目标持仓方向'] = target_pos

        # 根据目标仓位和实际仓位，计算实际操作，"1": "开多"，"2": "开空"，"3": "平多"， "4": "平空"
        if now_pos == 1 and target_pos == 0:  # 平多
            symbol_signal[symbol] = [3]
        elif now_pos == -1 and target_pos == 0:  # 平空
            symbol_signal[symbol] = [4]
        elif now_pos == 0 and target_pos == 1:  # 开多
            symbol_signal[symbol] = [1]
        elif now_pos == 0 and target_pos == -1:  # 开空
            symbol_signal[symbol] = [2]
        elif now_pos == 1 and target_pos == -1:  # 平多，开空
            symbol_signal[symbol] = [3, 2]
        elif now_pos == -1 and target_pos == 1:  # 平空，开多
            symbol_signal[symbol] = [4, 1]

        # 如果有信号，就计算信号时间
        if symbol in symbol_signal:
            symbol_info.at[symbol, '信号时间'] = datetime.now()  # 计算产生信号的时间

    return symbol_signal


# 币安下合约单
def binance_future_place_order(exchange, symbol_info, symbol_config, symbol_signal, max_try_amount, symbol):
    """
    :param exchange:
    :param symbol_info:
    :param symbol_config:
    :param symbol_signal:
    :param max_try_amount:
    :param symbol:
    :return:
    """

    order_id_list = []
    # 按照交易信号下单
    for order_type in symbol_signal[symbol]:
        num = 0
        while True:
            try:
                instrument_id = symbol_config[symbol]["instrument_id"]

                # 只在开仓之前设置杠杆、设置模式、获取下单价格，平仓之前不用
                if order_type in [1, 2]:
                
                    # 为当前币种设置成全仓模式，如果已经是全仓模式，binance api会报错，所有用try忽略
                    try:
                        response = exchange.fapiPrivate_post_margintype({
                            'symbol': instrument_id,
                            'marginType': 'CROSSED',
                        })
                        print(f'设置全仓模式结果：{response["msg"]}')
                    except ccxt.MarginModeAlreadySet:
                        pass
                    
                    # 设置杠杆倍数
                    response = exchange.set_leverage(symbol_config[symbol]["leverage"], symbol)
                    print(f'{symbol}的杠杆倍数为{response["leverage"]}')

                    # 获取下单价格，只下限价单。如果用市价单，下单瞬间可能出现价格巨大波动，造成市价单成本过高
                    symbol_info.at[symbol, "信号价格"] = float(exchange.fapiPublic_get_ticker_price({"symbol": instrument_id})["price"])

                # 当只要开仓或者平仓时，直接下单操作即可。但当本周期即需要平仓，又需要开仓时，需要在平完仓之后，
                # 重新评估下账户资金，然后根据账户资金计算开仓账户然后开仓。下面这行代码即处理这个情形。
                # "长度为2的判定"定位【平空，开多】或【平多，开空】两种情形，"下单类型判定"定位 处于开仓的情形。
                if len(symbol_signal[symbol]) == 2 and order_type in [1, 2]:  # 当两个条件同时满足时，说明当前处于平仓后，需要再开仓的阶段。
                    # time.sleep(medium_sleep_time)  # 休息一下，防止之平仓后，账户没有更新
                    symbol_info["账户余额"] = ccxt_fetch_future_account(exchange)
                    # 上次下单结束之后，已经等了一会，所以更新信号价格
                    symbol_info.at[symbol, "信号价格"] = float(exchange.fapiPublic_get_ticker_price({"symbol": instrument_id})["price"])

                # 确定下单参数
                params = {
                    "symbol": instrument_id,
                    "side": 'BUY' if order_type in [1, 4] else 'SELL',
                    "positionSide": 'LONG' if order_type in [1, 3] else 'SHORT',
                    "type": "LIMIT",
                    "price": cal_order_price(symbol, symbol_info, order_type),
                    "quantity": cal_order_size(symbol, symbol_config, symbol_info),
                    "newClientOrderId": f"Rock{exchange.milliseconds()}",
                    "workingType": "MARK_PRICE",
                    "timeInForce": "GTC",  # 必须参数"有效方式":GTC - Good Till Cancel 成交为止
                }
                print("本次下单参数：", params)

                try:
                    print('开始下单：', datetime.now())
                    order_info = exchange.fapiPrivate_post_order(params)
                    ordId = order_info['orderId']
                    print(order_info)
                    print('下单完成：', datetime.now(), "休息1秒")
                    time.sleep(1)

                    # 获取订单信息
                    state = exchange.fapiPrivate_get_order({
                        "symbol": instrument_id,
                        "orderId": ordId,
                    })["status"]
                except ccxt.InsufficientFunds:
                    num += 1
                    if num < 3:
                        send_dingding_msg("下单失败，账户余额不足，准备重试")
                        time.sleep(medium_sleep_time)
                        continue
                    else:
                        send_dingding_and_raise_error("下单失败，账户余额不足，重试无效，退出")
                
                # 判断是否成交,如果没有成交撤销挂单,重新获取最新价格下单
                # CANCELED：撤单成功  NEW：等待成交 PARTIALLY_FILLED：部分成交 FILLED：完全成交
                if state == 'NEW':
                    print('订单超过三秒未成交,重新获取价格下单')
                    exchange.fapiPrivate_delete_order(
                        {'symbol': symbol, 'orderId': ordId})
                    if num >= max_try_amount:
                        send_dingding_msg('下单未成交次数超过max_try_amount，终止下单，程序不退出')
                        break
                    num += 1
                    time.sleep(2)
                    continue
                order_id_list.append(ordId)
                break

            except Exception as e:
                traceback.print_exc()
                print(symbol, '下单失败，稍等后继续尝试')
                time.sleep(short_sleep_time)
                max_try_amount -= 1
                if max_try_amount <= 0:
                    print('下单失败次数超过max_try_amount，终止下单，程序退出')
                    send_dingding_and_raise_error('下单失败次数超过max_try_amount，终止下单，程序退出')
                    # exit() 若在子进程中（Pool）调用okex_future_place_order，触发exit会产生孤儿进程

    return symbol, order_id_list


# 串行下单
def single_threading_place_order(exchange, symbol_info, symbol_config, symbol_signal, max_try_amount=MAX_TRY):
    """
    :param exchange:
    :param symbol_info:
    :param symbol_config:
    :param symbol_signal:
    :param max_try_amount:
    :return:
    串行使用okex_future_place_order()函数，下单

    函数返回值案例：
                         symbol      信号价格                       信号时间
    4476028903965698  eth-usdt  227.1300 2020-03-01 11:53:00.580063
    4476028904156161  xrp-usdt    0.2365 2020-03-01 11:53:00.580558
    """
    # 函数输出变量
    symbol_order = pd.DataFrame()

    # 如果有交易信号的话
    if symbol_signal:
        # 遍历有交易信号的交易对
        for symbol in symbol_signal.keys():
            # 下单
            _, order_id_list = binance_future_place_order(exchange, symbol_info, symbol_config, symbol_signal, max_try_amount, symbol)

            # 记录
            for order_id in order_id_list:
                symbol_order.loc[order_id, 'symbol'] = symbol
                # 从symbol_info记录下单相关信息
                symbol_order.loc[order_id, '信号价格'] = symbol_info.loc[symbol, '信号价格']
                symbol_order.loc[order_id, '信号时间'] = symbol_info.loc[symbol, '信号时间']

    return symbol_order


# 获取成交数据
def update_order_info(exchange, symbol_config, symbol_order, max_try_amount=MAX_TRY):
    """
    根据订单号，检查订单信息，获得相关数据
    :param exchange:
    :param symbol_config:
    :param symbol_order:
    :param max_try_amount:
    :return:

    函数返回值案例：
                             symbol      信号价格                       信号时间  订单状态 开仓方向 委托数量 成交数量    委托价格    成交均价                      委托时间
    4476028903965698  eth-usdt  227.1300 2020-03-01 11:53:00.580063  完全成交   开多  100  100  231.67  227.29  2020-03-01T03:53:00.896Z
    4476028904156161  xrp-usdt    0.2365 2020-03-01 11:53:00.580558  完全成交   开空  100  100  0.2317  0.2363  2020-03-01T03:53:00.906Z
    """

    # 下单数据不为空
    if symbol_order.empty is False:
        # 这个遍历下单id
        for order_id in symbol_order.index:
            time.sleep(medium_sleep_time)  # 每次获取下单数据时sleep一段时间
            order_info = None
            # 根据下单id获取数据
            for i in range(max_try_amount):
                try:
                    para = {
                        'instId': symbol_config[symbol_order.at[order_id, 'symbol']]["instrument_id"],
                        'ordId': order_id
                    }
                    order_info = exchange.private_get_trade_order(para)
                    break
                except Exception as e:
                    traceback.print_exc()
                    print('根据订单号获取订单信息失败，稍后重试')
                    time.sleep(medium_sleep_time)
                    if i == max_try_amount - 1:
                        send_dingding_msg("重试次数过多，获取订单信息失败，程序退出")
                        raise ValueError('重试次数过多，获取订单信息失败，程序退出')

            if order_info:
                symbol_order.at[order_id, "订单状态"] = okex_order_state[order_info['data'][0]["state"]]
                symbol_order.at[order_id, "开仓方向"] = okex_order_type[order_info['data'][0]["posSide"]]
                symbol_order.at[order_id, "委托数量"] = order_info['data'][0]["sz"]
                symbol_order.at[order_id, "成交数量"] = order_info['data'][0]["accFillSz"]
                symbol_order.at[order_id, "委托价格"] = order_info['data'][0]["px"]
                symbol_order.at[order_id, "成交均价"] = order_info['data'][0]["avgPx"]
                symbol_order.at[order_id, "委托时间"] = pd.to_datetime(order_info['data'][0]["cTime"], unit='ms')
            else:
                print('根据订单号获取订单信息失败次数超过max_try_amount，发送钉钉')

    return symbol_order


# =====辅助功能函数
# ===下次运行时间，和课程里面讲的函数是一样的
def next_run_time(time_interval, ahead_seconds=5):
    """
    根据time_interval，计算下次运行的时间，下一个整点时刻。
    目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间的间隙
    :return: 下次运行的时间
    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00
    10m  当前时间为：12:38:51  返回时间为：12:40:00
    5m  当前时间为：12:33:51  返回时间为：12:35:00

    5m  当前时间为：12:34:51  返回时间为：12:40:00

    30m  当前时间为：21日的23:33:51  返回时间为：22日的00:00:00

    30m  当前时间为：14:37:51  返回时间为：14:56:00

    1h  当前时间为：14:37:51  返回时间为：15:00:00

    """
    if time_interval.endswith('m') or time_interval.endswith('h'):
        pass
    elif time_interval.endswith('T'):
        time_interval = time_interval.replace('T', 'm')
    elif time_interval.endswith('H'):
        time_interval = time_interval.replace('H', 'h')
    else:
        print('time_interval格式不符合规范。程序exit')
        exit()

    ti = pd.to_timedelta(time_interval)
    now_time = datetime.now()
    # now_time = datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time，可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0 and (target_time - now_time).seconds >= ahead_seconds:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    return target_time


def fetch_binance_symbol_history_candle_data(exchange, symbol, time_interval, max_len, max_try_amount=MAX_TRY):
    """
    获取某个币种在binance交易所所有能获取的历史数据，每次最多获取1500根。
    :param exchange:
    :param symbol:
    :param time_interval:
    :param max_len:
    :param max_try_amount:
    :return:

    函数核心逻辑：
    1.找到最早那根K线的开始时间，以此为参数获取数据
    2.获取数据的最后一行数据，作为新的k线开始时间，继续获取数据
    3.如此循环直到最新的数据
    """
    
    # 获取当前时间
    now_milliseconds = int(time.time() * 1e3)

    # 每根K线的间隔时间
    time_interval_int = int(time_interval[:-1])  # 若15m，则time_interval_int = 15；若2h，则time_interval_int = 2
    if time_interval.endswith('m'):
        time_segment = time_interval_int * 60 * 1000  # 15分钟 * 每分钟60s
    elif time_interval.endswith('H'):
        time_segment = time_interval_int * 60 * 60 * 1000  # 2小时 * 每小时60分钟 * 每分钟60s

    # 计算开始和结束的时间
    end = now_milliseconds - time_segment
    since = end - max_len * time_segment

    # 循环获取历史数据
    all_kline_data = []
    while True:
        kline_data = []

        # 获取K线使，要多次尝试
        for i in range(max_try_amount):
            try:
                kline_data = exchange.fetchOHLCV(symbol, time_interval, since=since, limit=1500)
                break
            except Exception as e:
                traceback.print_exc()
                time.sleep(medium_sleep_time)
                if i == (max_try_amount - 1):
                    _ = '【获取需要交易币种的历史数据】阶段，fetch_okex_symbol_history_candle_data函数中，' \
                        '使用ccxt的fetch_ohlcv获取K线数据失败，程序Raise Error'
                    send_dingding_and_raise_error(_)

        if kline_data:
            all_kline_data += kline_data
            if int(kline_data[-1][0]) > end:
                break
            since = kline_data[-1][0]  # 更新since，为下次循环做准备
        else:
            print('【获取需要交易币种的历史数据】阶段，fetch_ohlcv失败次数过多，程序exit，请检查原因。')
            exit()

    # 对数据进行整理
    df = pd.DataFrame(all_kline_data, dtype=float)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=8)
    df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]

    # 删除重复的数据
    df.drop_duplicates(subset=['candle_begin_time_GMT8'], keep='last', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 为了保险起见，去掉最后一行最新的数据
    df = df[:-1]

    return df


# ===依据时间间隔, 自动计算并休眠到指定时间
def sleep_until_run_time(time_interval, ahead_time=2):
    """
    根据next_run_time()函数计算出下次程序运行的时候，然后sleep至该时间
    :param time_interval:
    :param ahead_time:
    :return:
    """
    # 计算下次运行时间
    run_time = next_run_time(time_interval, ahead_time)
    print(f"等待当前k线收盘之后开始计算策略信号，收盘时间: {run_time}")
    # sleep
    time.sleep(max(0, (run_time - datetime.now()).seconds))

    while True:  # 在靠近目标时间时
        if datetime.now() > run_time:
            break

    return run_time


# ===在每个循环的末尾，编写报告并且通过订订发送
def dingding_report_every_loop(symbol_info, symbol_signal, run_time, robot_id_secret=""):
    """
    :param symbol_info:
    :param symbol_signal:
    :param symbol_order:
    :param run_time:
    :param robot_id_secret:
    :return:
    """
    content = ''

    # 持仓信息
    symbol_info_str = ['\n\n' + str(x) + '\n' + y.to_string() for x, y in symbol_info.iterrows()]
    content += '# =====持仓信息' + ''.join(symbol_info_str) + '\n\n'

    # 订单信息
    if symbol_signal:
        content += '# =====订单信息' + ''.join(str(symbol_signal)) + '\n\n'

    # 发送，每间隔30分钟或者有交易的时候，发送一次
    if run_time.minute % 30 == 0 or symbol_signal:
        send_dingding_msg(content)


# 获取价格和数量精度
def getPrecision(exchange, symbolConfig):
    mkts = exchange.load_markets()
    precisions = pd.DataFrame(index=symbolConfig.keys(), columns=["price", "amount"])

    for symbol in symbolConfig.keys():
       mkt = mkts[symbol] 
       precision = mkt["precision"]  # {'amount': 3, 'price': 2, 'base': 8, 'quote': 8}
       precisions.at[symbol, "price"] = precision["price"]
       precisions.at[symbol, "amount"] = precision["amount"]
    
    return precisions
    


# ===为了达到成交的目的，计算实际委托价格会向上或者向下浮动一定比例默认为0.02
def cal_order_price(symbol, symbolInfo, order_type, ratio=PRICE_SLIPPAGE):
    price = float(symbolInfo.at[symbol, "信号价格"])
    precision = int(symbolInfo.at[symbol, "价格精度"])
    if order_type in [1, 4]:
        price = price * (1 + ratio)
    elif order_type in [2, 3]:
        price = price * (1 - ratio)
    
    price = int(price * (10 ** precision)) / (10 ** precision)
    return price


# ===计算实际开仓张数
def cal_order_size(symbol, symbolConfig, symbolInfo, volatility_ratio=MAX_ORDER_PERCENT):
    """
    根据实际持仓以及杠杆数，计算实际开仓张数
    :param symbol:
    :param symbol_info:
    :param leverage:
    :param volatility_ratio:
    :return:
    """
    precision = int(symbolInfo.at[symbol, "数量精度"])
    leverage = symbolConfig[symbol]["leverage"]
    weight = symbolConfig[symbol]["weight"]
    balance = float(symbolInfo.loc[symbol, "账户余额"])
    price = float(symbolInfo.at[symbol, "信号价格"])

    # 根据权重计算开仓数量
    # 先找出未开仓的币，然后把它们的权重总和作为新的权重分母，重新计算权重。
    # 如果只有它自己没开仓，那么它的权重就是100%
    # freeSymbols = symbolInfo.loc[(symbolInfo["当前持仓方向"]==0)].index
    # if len(freeSymbols)!=0:
    #     weightTotal = 0.0
    #     for s in freeSymbols:
    #         weightTotal += symbolConfig[s]["weight"]
    #     weight = weight / weightTotal

    cprint(f"{symbol}本次下单权重占比: {round(weight,3)}", "yellow")

    # 当账户目前有持仓的时候，必定是要平仓，所以直接返回持仓量即可
    hold_amount = symbolInfo.at[symbol, "持仓数量"]
    if pd.notna(hold_amount):  # 不为空
        return abs(hold_amount)

    # 当账户没有持仓时，是开仓
    balance = balance * volatility_ratio * weight
    cprint(f"{symbol}本次下单使用的余额：{round(balance,3)}", "yellow")
    size = balance * leverage / price
    size = int(size * 10 ** precision) / (10 ** precision)
    # 下单量小于最小精度的情况下，取最小精度
    size = max(size, 1/(10**precision))
    return size


def send_dingding_msg(content, robot_id='', secret=''):
    try:
        token = "mrbXSz6rSoQjtrVnDlOH9ogK8UubLdNKClUgx1kGjGoq39usdEzbHlwtFIvHHO3C"
        url = f"https://webhook.exinwork.com/api/send?access_token={token}"
        value = {
            'category':'PLAIN_TEXT', 
            'data':content,
        }
        r = requests.post(url, data=value)
        # print(r.text)
        print(content)
        print('(已发送至Mixin)')
    except Exception as e:
        traceback.print_exc()
        print("发送Mixin失败:", e)


# price 价格 money 资金量 leverage 杠杆 ratio 最小变动单位
def calculate_max_size(price, money, leverage, ratio):
    return math.floor(money * leverage / price / ratio)


def send_dingding_and_raise_error(content):
    print(content)
    # send_dingding_msg(content)
    sendMixin(content)
    raise RuntimeError(content)


def sendMixin(msg):
    token = "mrbXSz6rSoQjtrVnDlOH9ogK8UubLdNKClUgx1kGjGoq39usdEzbHlwtFIvHHO3C"
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"
    value = {
        'category':'PLAIN_TEXT', 
        'data':msg,
        }
    try:
        r = requests.post(url, data=value)
        
    except Exception as err:
        print(f"Failed to send mixin message.")
        print(err)
        traceback.print_exc()


def send_mixin_and_raise_error(content):
    print(content)
    sendMixin(content)
    raise RuntimeError(content)


def dbgShowTime(msg=""):
    print(datetime.utcnow(), msg)

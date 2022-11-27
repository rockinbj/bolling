import datetime as dt
from time import sleep

import ccxt
import pandas as pd
from termcolor import cprint

from Config import *
from Function import *

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


# 交易所参数
BINANCE_CONFIG = {
    "options": {"defaultType":"future"},
    "apiKey": "cNiKjnf1uwkmjKqsYwI2mjQ61OyAQFA2vEEMWurMXRxb0IwciDQfj7Jmaam8qeJ2",
    "secret": "mfXIJz8gDBY0EOe83ZAI9kQYH0KqSoQTTkVpz1PDad6uc13RCHdhCYmSDacau9Uo",
}
exchange = ccxt.binance(BINANCE_CONFIG)
markes = exchange.load_markets()

# 策略参数
# 定义交易币种、策略参数
symbol_config = {
    # "ETH/USDT": {
    #     "weight": 0.4,
    #     "leverage": 3,
    #     # "strategy_name": "real_signal_none",
    #     # "strategy_name": "real_signal_random",
    #     "strategy_name": "real_signal_simple_bolling",
    #     "level": "5m",
    #     "para": [825, 1.2],
    # },
    "DOGE/USDT": {
        "weight": 0.3,
        "leverage": 3,
        # "strategy_name": "real_signal_none",
        "strategy_name": "real_signal_random",
        # "strategy_name": "real_signal_simple_bolling",
        "level": "1m",
        "para": [395, 1.4],
    },
    "BTC/USDT": {
        "weight": 0.3,
        "leverage": 3,
        # "strategy_name": "real_signal_none",
        "strategy_name": "real_signal_random",
        # "strategy_name": "real_signal_simple_bolling",
        "level": "5m",
        "para": [575, 3.5],
    },
}
# 获取交易所的交易对书写格式
for symbol in symbol_config.keys():
    symbol_config[symbol]["instrument_id"] = markes[symbol]["id"]


def main():
    
    # ████████ 第一步：获取历史k线数据，数量与策略参数相关
    dbgShowTime("一、获取历史k线数据")
    # k线字典
    symbol_candle_data = dict()
    # 获取所有交易币种的历史k线
    for symbol in symbol_config.keys():
        # dbgShowTime(f"开始 获取{symbol}历史k线数据")
        # 需要获取的历史k线数，如果是布林带就获取ma均线长度+10根k线，用于计算中轨
        if symbol_config[symbol]["strategy_name"] == "real_signal_simple_bolling":
            max_len = symbol_config[symbol]["para"][0] + 10
        elif symbol_config[symbol]["strategy_name"] == "real_signal_none":
            max_len = 500
        elif symbol_config[symbol]["strategy_name"] == "real_signal_random":
            max_len = 500

        time_interval = symbol_config[symbol]["level"]
        
        symbol_candle_data[symbol] = fetch_binance_symbol_history_candle_data(exchange, symbol, time_interval, max_len=max_len)
        print(f"{symbol}获取历史k线 {time_interval} {len(symbol_candle_data[symbol])}根")
        print()
        time.sleep(medium_sleep_time)
    
    # 开始循环执行策略
    while True:
        
        # ████████ 第二步：获取持仓数据
        dbgShowTime("二、获取当前持仓数据")
        # 初始化symbol_info，在每次循环开始时都初始化
        symbol_info_columns = ['账户余额', '当前持仓方向', '持仓数量', "持仓价值",
                                '持仓收益率', '持仓收益', 
                                '持仓均价', '标记价格', "爆仓价格",
                                '杠杆倍数', "保证金模式", 
                                "价格精度", "数量精度", "信号价格", "信号时间", "目标持仓方向"]
        symbol_info = pd.DataFrame(index=symbol_config.keys(), columns=symbol_info_columns)
        
        # 更新账户信息symbol_info
        symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)
        print(f"{symbol_info}\n")

        # 获取策略执行时间，并sleep至该时间
        run_time = sleep_until_run_time(time_interval)
        print()
        

        # ████████ 第三步：获取交易币种的最新k线
        dbgShowTime("三、获取交易币种的最新k线")
        exchange.timeout = 1000  # 即将获取最新数据，临时将timeout设置为1s，加快获取数据速度
        candle_num = 5  # 只获取最近candle_num根K线数据，可以获得更快的速度
        # 获取k线数据
        recent_candle_data = single_threading_get_data(exchange, symbol_info, symbol_config, time_interval, run_time, candle_num)
        for symbol in recent_candle_data:
            print(f"{symbol}获取最新k线{len(recent_candle_data[symbol])}根")

        # 将symbol_candle_data和最新获取的recent_candle_data数据合并
        for symbol in symbol_config.keys():
            # df = symbol_candle_data[symbol].append(recent_candle_data[symbol], ignore_index=True)
            # 最新获取的k线与历史k线合并
            df = pd.concat([symbol_candle_data[symbol], recent_candle_data[symbol]], ignore_index=True)
            # 去重、擦除最早k线
            df.drop_duplicates(subset=['candle_begin_time_GMT8'], keep='last', inplace=True)
            df.sort_values(by='candle_begin_time_GMT8', inplace=True)  # 排序，理论上这步应该可以省略，加快速度
            df = df.iloc[-max_len:]  # 保持最大K线数量不会超过max_len个
            df.reset_index(drop=True, inplace=True)
            symbol_candle_data[symbol] = df
        print()

        # ████████ 第四步：计算每个币种的交易信号
        dbgShowTime("四、计算每个币种的交易信号")
        symbol_signal = calculate_signal(symbol_info, symbol_config, symbol_candle_data)
        cprint(f"本周期交易计划:{symbol_signal}", "green")
        if symbol_signal:
            print(f"\n出现交易信号，更新交易价格和时间:\n{symbol_info}")
        print()
        

        # 如果有交易信号，进行第五、六步
        if symbol_signal:
            
            # ████████ 第五步：根据交易信号下单
            dbgShowTime("五、根据交易信号下单")
            exchange.timeout = exchange_timeout  # 下单时需要增加timeout的时间，将timout恢复正常
            symbol_order = pd.DataFrame()
            symbol_order = single_threading_place_order(exchange, symbol_info, symbol_config, symbol_signal)  # 单线程下单
            print()


            # ████████ 第六步：交易结束后重新更新账户信息
            dbgShowTime("六、交易结束后重新更新账户信息")
            time.sleep(long_sleep_time)  # 休息一段时间再更新
            symbol_info = pd.DataFrame(index=symbol_config.keys(), columns=symbol_info_columns)
            symbol_info = update_symbol_info(exchange, symbol_info, symbol_config)
            print(f"执行交易后，本周期最终持仓状态：{symbol_info}")


        # 发送钉钉
        dingding_report_every_loop(symbol_info, symbol_signal, run_time)

        # 本次循环结束
        print(f"{'==='*5}本周期结束{'==='*5}\n\n")
        time.sleep(long_sleep_time)


if __name__ == '__main__':
    main()

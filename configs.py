EXCHANGE = "BINANCE"

# 定义交易币种、策略参数
SYMBOLS_CONFIG = [
    {
        "symbol": "ETH/USDT",
        "weight": 0.4,
        "leverage": 3,
        # "strategy": "real_signal_none",
        # "strategy": "real_signal_random",
        "strategy": "real_signal_simple_bolling",
        "level": "5m",
        "para": [825, 1.2],
        "slippage": 0.02,
        "volatility": 0.98,
    },
    {
        "symbol": "DOGE/USDT",
        "weight": 0.2,
        "leverage": 3,
        # "strategy": "real_signal_none",
        # "strategy": "real_signal_random",
        "strategy": "real_signal_simple_bolling",
        "level": "30m",
        "para": [395, 1.4],
        "slippage": 0.02,
        "volatility": 0.98,
    },
    {
        "symbol": "BTC/USDT",
        "weight": 0.2,
        "leverage": 3,
        # "strategy": "real_signal_none",
        # "strategy": "real_signal_random",
        "strategy": "real_signal_simple_bolling",
        "level": "30m",
        "para": [575, 3.5],
        "slippage": 0.02,
        "volatility": 0.98,
    },
    {
        "symbol": "BNB/USDT",
        "weight": 0.2,
        "leverage": 3,
        # "strategy": "real_signal_none",
        # "strategy": "real_signal_random",
        "strategy": "real_signal_simple_bolling",
        "level": "4h",
        "para": [190, 2.6],
        "slippage": 0.02,
        "volatility": 0.98,
    },
]

# 最大重试次数
MAX_TRY = 3

# 获取最新k线的数量
NEW_KLINE_NUM = 5

# 本轮开始之前的预留秒数，小于预留秒数则顺延至下轮
AHEAD_SEC = 3

# 休眠时间
SLEEP_SHORT = 0.5
SLEEP_MEDIUM = 3
SLEEP_LONG = 6

# mixin webhook token
MIXIN_TOKEN = "mrbXSz6rSoQjtrVnDlOH9ogK8UubLdNKClUgx1kGjGoq39usdEzbHlwtFIvHHO3C"

# 报告发送间隔分钟
REPORT_INTERVAL = 30


"""
更新时间：2021-10-08
《邢不行|Python数字货币量化投资课程》
无需编程基础，助教答疑服务，专属策略网站，一旦加入，永续更新。
课程详细介绍：https://quantclass.cn/crypto/class
邢不行微信: xbx9025
本程序作者: 邢不行

# 课程内容
择时策略实盘需要的配置参数
"""

# 订单对照表
okex_order_type = {
    '1': '开多',
    '2': '开空',
    '3': '平多',
    '4': '平空',
}

# 订单状态对照表
okex_order_state = {
    '-2': '失败',
    '-1': '撤单成功',
    '0': '等待成交',
    '1': '部分成交',
    '2': '完全成交',
    '3': '下单中',
    '4': '撤单中',
}

# 币种面值对照表
coin_value_table = {
    "BTC/USDT": 0.001,
    "ETH/USDT": 0.001,
}

# 下单量按现有余额的比例，防止价格瞬间波动造成余额不足开仓失败
MAX_ORDER_PERCENT = 0.93

# 限价单下单价格比真实价格的滑动比例，防止一档盘口不能成交
PRICE_SLIPPAGE = 0.02

# 用户手续费
COMMISSION = 4 / 10000


# sleep时间配置
short_sleep_time = 0.5  # 用于和交易所交互时比较紧急的时间sleep，例如获取数据、下单
medium_sleep_time = 1  # 用于和交易所交互时不是很紧急的时间sleep，例如获取持仓
long_sleep_time = 5  # 用于较长的时间sleep

# timeout时间
exchange_timeout = 3000  # 3s

# 最大重试次数
MAX_TRY = 3

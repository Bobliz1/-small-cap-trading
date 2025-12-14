# 导入函数库
from jqdata import *

'''
今天目标处理两个问题：
1. 当股票的短期动量已利用完，股票会进入小幅波动的状态，此时赖在手里收益几乎为0。（用布林带解决）
    解决1：用成交量判断第一个高度效果如何
2. 添加个股的风险控制。
    解决2：不能这么处理，因为我的是一篮子策略
3.意识到两个问题，更新最高价值，以及计算回撤，都要用收盘价，不能在盘中进行；下单与结算的异步性；
    解决3：创建一个收盘运行的函数
4.找到合适自己的策略
    解决4：不处理盘中，只关注日间策略。
5.观察到新现象：最大回撤不应该设置太大，当亏到最低点时，可能反弹了，关键在于快跌的时候卖出，跌倒最低时进入
6.其实对于小市值策略来说，增长到第一个较大值平台后就可以换下一手小市值了，其实也没必要看很长的布林带
    解决6：其实还是要看，他可能不涨
7：科创板无法下单。（已解决）
8.带宽输出，处理warning
'''
## 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 只设置order日志级别为warning
    log.set_level('order', 'warning')
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True) 
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, 
                             open_commission=0.0003, close_commission=0.0003,\
                             close_today_commission=0, min_commission=5), type='stock')
                             
    # 止损状态: "normal"=正常, "clearing"=清仓中
    g.stop_loss_status = "normal"                 
    
    g.stocknum = 10     # 持仓数量
    g.max_drawdown_threshold = 0.1 # 最大回撤阈值
    g.portfolio_high = 0     # 记录投资组合最高价值
    g.buy_date = None  # 买入日期
    g.initial_portfolio_value = 0  # 初始投资组合价值
    g.kc_buffer = 0.05     #添加科创板保护缓冲比例参数
    
    # 修改点1: 添加相对收紧阈值参数
    g.relative_squeeze_ratio = 0.7  # 相对收紧比例阈值(当前宽度<历史平均宽度的70%)
    
    # 运行函数
    run_daily(trade, 'every_bar')
    run_daily(after_market_update, 'after_close')


def check_stocks(context):
    """选择市值在5%到10%分位数之间的最小10支股票"""
    # 使用前一天作为查询日期
    query_date = context.previous_date
    
    # 查询所有A股股票的市值
    q_all = query(
        valuation.code,
        valuation.market_cap
    )
    df_all = get_fundamentals(q_all, date=query_date)  # 指定查询日期
    
    if df_all is None or len(df_all) == 0:
        return []
    
    # 去除市值NaN值（如果有）
    df_all = df_all.dropna(subset=['market_cap'])
    
    # 按市值升序排序
    df_sorted = df_all.sort_values('market_cap', ascending=True)
    
    # 计算5%和10%分位数对应的索引位置
    n = len(df_sorted)
    idx_low = int(n * 0.05)  # 5%分位数位置（向下取整）
    idx_high = int(n * 0.10)  # 10%分位数位置（向下取整）
    
    # 确保idx_high > idx_low，避免切片为空
    if idx_low >= idx_high:
        idx_high = idx_low + 1
        if idx_high > n:
            idx_high = n
    
    # 选择市值排名在5%到10%之间的股票
    selected_df = df_sorted.iloc[idx_low:idx_high]  # 切片: [idx_low, idx_high)
    
    if len(selected_df) == 0:
        return []
    
    buylist = list(selected_df['code'])
    
    # 过滤停牌股票和ST股票
    buylist = filter_paused_stock(buylist)
    
    # 返回市值最小的10支股票（如果不足10支，则返回全部）
    return buylist[:g.stocknum]
    
def filter_paused_stock(stock_list):
    """过滤停牌股票和ST股票"""
    if not stock_list:
        return []
    
    # 获取当前时间数据字典
    current_data = get_current_data()
    
    result = []
    for stock in stock_list:
        try:
            # 直接通过键访问，会按需获取该股票的数据
            stock_data = current_data[stock]
            
            # 检查是否停牌
            is_paused = stock_data.paused
            
            # 检查是否是ST或*ST股票
            is_st = stock_data.is_st
            
            if not is_paused and not is_st:
                result.append(stock)

        except Exception as e:
            # 如果获取股票数据失败，记录日志并过滤该股票
            continue
    
    return result
    
    
## 计算当前回撤
def calculate_drawdown(context):
    """计算从最高点的回撤"""
    # 获取当前投资组合总价值
    current_value = context.portfolio.total_value
    
    # 计算回撤（从最高点下跌的百分比）
    if g.portfolio_high > 0:
        drawdown = (g.portfolio_high - current_value) / g.portfolio_high
    else:
        drawdown = 0
    
    return drawdown


## 检查是否触发止损
def check_stop_loss(context):
    """检查是否达到最大回撤阈值"""      
    # 如果已经在清仓状态，不需要再次检查
    if g.stop_loss_status == "clearing":
        return False
    
    # 如果没有持仓，不需要止损
    if len(context.portfolio.positions) == 0:
        return False
    
    # 计算当前回撤
    current_drawdown = calculate_drawdown(context)
    
    # 如果回撤超过阈值，触发止损
    if current_drawdown >= g.max_drawdown_threshold:
        log.info(f"触发止损！当前回撤: {current_drawdown:.2%}, 阈值: {g.max_drawdown_threshold:.0%}")
        g.stop_loss_status = "clearing"
        return True
    
    return False

## 清仓所有股票
def clear_all_positions(context):
    """
    清仓所有持仓的股票
    返回是否清仓完成
    """
    if len(context.portfolio.positions) == 0:
        return True
    
    # 获取当前时间数据字典
    current_data = get_current_data()
    
    positions = list(context.portfolio.positions.keys())
    all_orders_placed = True  # 表示"是否已下单"
    
    for stock in positions:
        try:
            # 通过键访问获取股票数据
            stock_data = current_data[stock]
            # 检查股票是否可交易
            if not stock_data.paused and context.portfolio.positions[stock].closeable_amount > 0:
                try:
                    # 修改点2: 为科创板股票添加保护限价
                    # 检查是否是科创板（代码以688开头）
                    if stock.startswith('688'):
                        # 获取开盘价
                        open_price = get_current_data()[stock].day_open
                        if open_price > 0:
                            # 科创板卖出保护价 = 开盘价 × (1 - 安全缓冲)
                            limit_price = open_price * (1 - g.kc_buffer)
                            # 获取可卖出数量
                            amount = context.portfolio.positions[stock].closeable_amount
                            if amount > 0:
                                # 使用限价单
                                order(stock, -amount, style=MarketOrderStyle(limit_price))
                                log.info(f"科创板卖出: {stock}, 数量: {amount}, 限价: {limit_price:.2f}")
                        else:
                            # 如果开盘价为0，使用市价单
                            order_target_value(stock, 0)
                    else:
                        # 非科创板使用市价单
                        order_target_value(stock, 0)
                except Exception as e:
                    all_orders_placed = False
                    log.error(f"卖出股票失败: {stock}, 错误: {e}")
            else:
                all_orders_placed = False
        except Exception as e:
            all_orders_placed = False
            log.error(f"获取股票数据失败: {stock}, 错误: {e}")
    
    # 在聚宽回测中，我们只需下单，清仓会在下一个交易日确认
    return all_orders_placed

## 买入股票
def buy_stocks(context):
    """买入选中的股票"""
    # 选股
    g.stock_list = check_stocks(context)
    
    if not g.stock_list:
        return
    
    # 获取当前时间数据字典，用于检查是否可以买入
    current_data = get_current_data()
    
    # 计算每只股票的投资金额
    available_cash = context.portfolio.available_cash
    num_to_buy = 0
    valid_stocks = []
    
    # 检查每只股票是否可以交易
    for stock in g.stock_list:
        try:
            stock_data = current_data[stock]
            if not stock_data.paused and not stock_data.is_st:
                valid_stocks.append(stock)
                num_to_buy += 1
                if num_to_buy >= g.stocknum:
                    break
        except Exception as e:
            continue
    
    if num_to_buy == 0:
        return
    
    cash_per_stock = available_cash / num_to_buy
    
    for stock in valid_stocks:
        try:
            # 修改点3: 为科创板股票添加保护限价
            # 检查是否是科创板（代码以688开头）
            if stock.startswith('688'):
                # 获取开盘价
                open_price = get_current_data()[stock].day_open
                if open_price > 0:
                    # 科创板买入保护价 = 开盘价 × (1 + 安全缓冲)
                    limit_price = open_price * (1 + g.kc_buffer)
                    # 计算买入数量
                    amount = int(cash_per_stock / open_price / 100) * 100
                    if amount > 0:
                        # 使用限价单
                        order(stock, amount, style=MarketOrderStyle(limit_price))
                        log.info(f"科创板买入: {stock}, 数量: {amount}, 限价: {limit_price:.2f}")
                else:
                    # 如果开盘价为0，跳过
                    continue
            else:
                # 非科创板使用市价单
                order_value(stock, cash_per_stock)
        except Exception as e:
            log.error(f"买入股票失败: {stock}, 错误: {e}")
            pass
    
    # 在买入时记录买入日期和初始投资组合价值
    g.buy_date = context.current_dt
    g.initial_portfolio_value = context.portfolio.total_value
    log.info(f"【买入】日期: {g.buy_date.date()}, 买入金额: {g.initial_portfolio_value:.2f}")

def check_portfolio_sell_conditions(context):
    """
    检查投资组合的卖出条件
    条件1: 收益率 >= 15% 且 (成交量萎缩 或 布林带收紧)
    条件2: 持有天数 >= 7天, 收益率 < 15%, 布林带收紧, 且股价在20日均线±3%范围内
    """
    if len(context.portfolio.positions) == 0:
        return False
    
    # 检查是否有买入日期记录
    if g.buy_date is None or g.initial_portfolio_value <= 0:
        return False
    
    # 计算持有天数
    hold_days = (context.current_dt - g.buy_date).days
    
    # 获取持仓股票列表
    positions = list(context.portfolio.positions.keys())
    if not positions:
        return False
    
    # 计算投资组合的整体收益率
    current_portfolio_value = context.portfolio.total_value
    portfolio_return = (current_portfolio_value - g.initial_portfolio_value) / g.initial_portfolio_value
    
    # 计算投资组合的平均收益
    portfolio_avg_return = 0
    for stock in positions:
        position = context.portfolio.positions[stock]
        buy_price = position.avg_cost
        # 获取当前价格
        current_price = get_current_data()[stock].last_price
        stock_return = (current_price - buy_price) / buy_price
        portfolio_avg_return += stock_return
    portfolio_avg_return /= len(positions)
    
    is_bollinger_squeeze = False
    today_avg_price = 0
    ma20 = 0
    volume_ratio = 0
    bandwidth_ratio = 0
    price_position_ratio = 0
    
    if hold_days >= 7:  # 只有持有天数≥7天时才计算布林带
        # 修改点4: 修复原代码错误，获取更长时间的历史数据用于相对收紧比较
        # 原代码错误: 只获取20天数据，无法计算历史平均带宽(滑动窗口需要>20天)
        # 新代码: 获取40天数据，这样有足够的历史窗口计算平均带宽
        portfolio_prices = []
        
        for stock in positions:
            # 修改点5: 获取过去40天的收盘价(而不是20天)
            hist = attribute_history(stock, 40, '1d', ['close'], skip_paused=True, df=True)
            if hist is not None and len(hist) == 40:  # 确保有完整的40天数据
                portfolio_prices.append(hist['close'].values)
        
        if len(portfolio_prices) == len(positions) and portfolio_prices:
            # 计算投资组合每天的平均价格
            import numpy as np
            portfolio_prices_array = np.array(portfolio_prices)
            avg_prices = portfolio_prices_array.mean(axis=0)  # 现在avg_prices长度为40
            
            # 计算当前20天窗口的布林带宽度(最近20天)
            current_window = avg_prices[-20:]  # 索引20-39: 最后20天
            if len(current_window) >= 2:  # 至少需要2个点计算标准差
                # 计算当前20日均线
                ma20_current = current_window.mean()
                # 计算当前20日标准差
                std20_current = current_window.std()
            else:
                ma20_current = 0
                std20_current = 0
            
            # 计算当前布林带宽度
            upper_band = ma20_current + 2 * std20_current
            lower_band = ma20_current - 2 * std20_current
            current_bandwidth = upper_band - lower_band
            
            
            bandwidths = []
            # 计算历史带宽(滑动20天窗口，从第0天开始)
            # 我们有40天数据，可以计算21个20天窗口(索引0-19, 1-20, ..., 20-39)
            # 但当前窗口是第20个窗口(索引20-39)，我们用它作为当前窗口
            # 历史平均使用前20个窗口(索引0-19到19-38)
            
            for i in range(0, 20):  # i从0到19，共20个历史窗口
                window = avg_prices[i:i+20]  # 窗口i: 第i天到i+19天
                if len(window) >= 2:  # 至少需要2个点计算标准差
                    ma20_i = window.mean()
                    std20_i = window.std()
                    upper_i = ma20_i + 2 * std20_i
                    lower_i = ma20_i - 2 * std20_i
                    bandwidth_i = upper_i - lower_i
                    bandwidths.append(bandwidth_i)
            
            # 计算历史平均带宽
            avg_bandwidth = np.mean(bandwidths) if bandwidths else 0
            
            # 修改点7: 布林带相对收紧条件
            # 当前宽度 < 历史平均宽度的70%
            bandwidth_ratio = current_bandwidth / avg_bandwidth if avg_bandwidth > 0 else 1
            if avg_bandwidth > 0 and current_bandwidth < avg_bandwidth * g.relative_squeeze_ratio:
                # 获取今天的平均价格(最后一天)
                today_avg_price = avg_prices[-1]
                is_bollinger_squeeze = True
                ma20 = ma20_current
                log.debug(f"布林带收紧检测: 当前宽度={current_bandwidth:.4f}, 历史平均={avg_bandwidth:.4f}, 比例={bandwidth_ratio:.2%}")
    
    ############################################################
    # 条件1: 收益率达标且(成交量萎缩或布林带收紧)
    ############################################################
    condition1 = False
    condition1_type = ""
    if portfolio_avg_return >= 0.15:  # 收益率 >= 15%
        # 计算投资组合的平均成交量
        total_volume_today = 0
        total_volume_5day_avg = 0
        
        for stock in positions:
            # 获取过去5天的成交量数据
            hist = attribute_history(stock, 5, '1d', ['volume'], skip_paused=True, df=True)
            if hist is not None and len(hist) == 5:
                today_volume = hist['volume'].iloc[-1]
                past_4_avg_volume = hist['volume'].iloc[-5:-1].mean()  # 索引-5到-2，共4天
                total_volume_today += today_volume
                total_volume_5day_avg += past_4_avg_volume
        
        volume_condition = False
        if total_volume_5day_avg > 0:
            avg_volume_today = total_volume_today / len(positions)
            avg_volume_5day = total_volume_5day_avg / len(positions)
            volume_ratio = avg_volume_today / avg_volume_5day
            
            if volume_ratio < 0.8:  # 今天平均成交量 < 过去5天平均成交量的80%
                volume_condition = True
                condition1_type = "成交量萎缩"
        
        # 收益率达标时，如果成交量萎缩或布林带收紧，就卖出
        condition1 = volume_condition or is_bollinger_squeeze
        
        if condition1 and is_bollinger_squeeze and condition1_type == "":
            condition1_type = "布林带收紧(相对)"
        elif condition1 and volume_condition and is_bollinger_squeeze:
            condition1_type = "成交量萎缩+布林带收紧(相对)"
    
    ############################################################
    # 条件2: 持有天数≥7天, 收益率未达标, 布林带收紧, 且股价在20日均线±5%范围内
    ############################################################
    condition2 = False
    if hold_days >= 7 and portfolio_avg_return < 0.15:  # 持有天数>=7天且收益率未达标
        if is_bollinger_squeeze and today_avg_price > 0:
            # 条件2D: 股价位置在20日均线±5%范围内
            price_position_ratio = (today_avg_price - ma20) / ma20 if ma20 > 0 else 0
            if (today_avg_price >= ma20 * 0.95) and (today_avg_price <= ma20 * 1.05):
                condition2 = True
    
    # 如果满足任一条件，触发清仓
    if condition1 or condition2:
        # 记录详细的卖出原因
        if condition1:
            log.info(f"【卖出-条件1】日期: {context.current_dt.date()}, "
                     f"持有天数: {hold_days}, 平均收益率: {portfolio_avg_return:.2%}, "
                     f"原因: {condition1_type}, 成交量比例: {volume_ratio:.2%}, "
                     f"带宽比例: {bandwidth_ratio:.2%}, 阈值: {g.relative_squeeze_ratio:.0%}")
        elif condition2:
            log.info(f"【卖出-条件2】日期: {context.current_dt.date()}, "
                     f"持有天数: {hold_days}, 平均收益率: {portfolio_avg_return:.2%}, "
                     f"原因: 布林带收紧(相对)+股价在20日均线附近, "
                     f"带宽比例: {bandwidth_ratio:.2%}, 阈值: {g.relative_squeeze_ratio:.0%}, 价格位置: {price_position_ratio:.2%}")
        return True
    
    return False

## 交易函数
def trade(context):
    """主交易逻辑"""
    
    # 状态1: 清仓中
    if g.stop_loss_status == "clearing":
        # 尝试清仓
        orders_placed = clear_all_positions(context)
        
        if orders_placed:
            pass
        else:
            pass
        
        return
    
    # 状态2: 正常状态
    elif g.stop_loss_status == "normal":
        # 如果没有持仓，买入股票
        if len(context.portfolio.positions) == 0:
            buy_stocks(context)
            return

def after_market_update(context):
    """收盘后运行的函数"""
    # 获取当前投资组合总价值（当天收盘后的价值）
    current_value = context.portfolio.total_value
    
    # 更新投资组合最高价值
    if current_value > g.portfolio_high:
        g.portfolio_high = current_value
    
    # 状态1: 清仓中
    if g.stop_loss_status == "clearing":
        # 检查是否真的清仓完成
        if len(context.portfolio.positions) == 0:
            g.stop_loss_status = "normal"
            g.portfolio_high = 0  # 重置最高价值
            g.buy_date = None
            g.initial_portfolio_value = 0
        return
    
    # 状态2: 正常状态
    elif g.stop_loss_status == "normal":
        # 如果没有持仓，不需要检查止损
        if len(context.portfolio.positions) == 0:
            return
        
        # 计算当前回撤
        current_drawdown = calculate_drawdown(context)
        
        # 检查是否触发止损
        if current_drawdown >= g.max_drawdown_threshold:
            log.info(f"【止损】日期: {context.current_dt.date()}, "
                     f"回撤: {current_drawdown:.2%}, 阈值: {g.max_drawdown_threshold:.0%}")
            g.stop_loss_status = "clearing"
            return
        
        # 检查卖出条件
        should_sell = check_portfolio_sell_conditions(context)
        
        if should_sell:
            g.stop_loss_status = "clearing"
            return

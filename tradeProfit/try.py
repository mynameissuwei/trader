# 眼厉的群友发现在低溢价里面，碰到强赎最后一天也会买入（这个不会进行转股，只会会按照最后一天的价格卖出，等于单天买入就卖出），所以为了让日志更符合现实，强赎最后一天就排除。 



# 同时如果轮动期间持有的转债卖出了，也是按最后一天价格卖出，而日志里面还会残留，也把日志输出部分做的更加更加符合实际。（强调：这个对实际收益率最大回撤指标不会有任何影响，纯粹为了“好看”）



# 下面代码加了规模过滤因子，把不满足规模要求的转债排除；

# 根据转债高点回落的值，也把不满足的转债排除。【比如：前100天的最高点回落20元，抄抄小底，不满足的移除】

# 最后根据上面剩下的标的，进行双低，低价，低溢价排序，进行排名轮动。这里会有个5名的轮动阈值，只有持仓里面的标的跌出 原有名次+阈值 才会 移除。 阈值设为0， 就当作无阈值处理。
import pandas as pd
import datetime
from loguru import logger as log

pd.set_option('display.max_rows', None)

CSV_PATH = '../data/merge_bond_info-2023-04-03.csv'
HOLD_NUM = 10  # 持有个数

START_DATE = '2018-01-01'
END_DATE = '2022-09-03'

FREQ = 1  # 5天轮动一次 轮动频率

condition = 'premratio'
# condition = 'doublelow'
# condition = 'lowprice'

threshold = 0  # 排名阈值

DROP_DOWN_ENABLE = True
DROP_DOWN_DAY = 100
DROP_DOWN_PRICE = 20
DROP_DOWN_ENABLE_LOG = True

REMAIN_SIZE = 3
REMAIN_SIZE_LOG_ENABLE = False

# 获取某个日期的前 n 天的日期
def last_n_day(date, n, origin_fmt, target_fmt):
    return (datetime.datetime.strptime(date, origin_fmt) - datetime.timedelta(days=n)).strftime(target_fmt)


class DataFeed():
    '''
    数据驱动
    '''

    def __init__(self):
        self.csv_path = CSV_PATH
        self.date_list, self.source = self.feed()

    # 使用 Pandas 库中的 read_csv() 方法读取指定路径下的 CSV 文件，同时指定编码为 UTF-8，并且将 tickerEqu、tickerBond 和 secID_x 这些列的数据类型设置为字符串。
    # 如果 df（即 DataFrame）中包含 Unnamed: 0 这一列，则使用 del 关键字将其从 DataFrame 中删除。
    # 将 DataFrame 中的 'tradeDate' 这一列的数据类型转换成 datetime 格式，并将其作为索引，以便后面按照日期进行筛选。
    # 获取所有交易日（tradeDate）的集合，并将其转换成字符串格式的列表。
    # 将上一步得到的日期列表进行排序。
    # 返回日期列表和 DataFrame。
    def feed(self):
        df = pd.read_csv(self.csv_path,
                         encoding='utf8',
                         dtype={'tickerEqu': str, 'tickerBond': str, 'secID_x': str},
                         )
        if 'Unnamed: 0' in df.columns:
            del df['Unnamed: 0']
        df['tradeDate'] = pd.to_datetime(df['tradeDate'], format='%Y-%m-%d')
        df = df.set_index('tradeDate')
        date_set = set(df.index.tolist())
        date_list = list(map(lambda x: x.strftime('%Y-%m-%d'), date_set))
        date_list.sort()
        return date_list, df

    def get_date(self):
        return self.date_list

    def get_source_data(self):
        return self.source


data_feed = DataFeed()



class RemainSizeFilter():

    def apply(self, df):
        target = df[df['remainSize'] <= REMAIN_SIZE]
        if REMAIN_SIZE_LOG_ENABLE:
            log.info(target[['tickerBond', 'secShortNameBond', 'remainSize']])
        return target

class DropDownFilter():
    '''
    高位回落过滤因子
    '''

    def __init__(self):
        self.enable = DROP_DOWN_ENABLE
        self.data = data_feed.get_source_data()
        self.date_list = data_feed.get_date()

        self.fmt = '%Y-%m-%d'

    def apply(self, *args, **kwargs):
        date = kwargs.pop('date')
        return self.last_n_day_drop(date)

    def find_nearest(self, date):
        start = -1
        while start < len(self.date_list):
            start += 1

            if date > self.date_list[start]:
                continue
            else:
                break

        return self.date_list[start]

    def last_n_day_drop(self, date):
        start_date = last_n_day(date, DROP_DOWN_DAY, self.fmt, self.fmt)
        start_date = self.find_nearest(start_date)
        last_day_df = self.data.loc[start_date].copy()
        last_day_df = last_day_df.set_index('tickerBond')
        current_df = self.data.loc[date].copy()
        current_df = current_df.set_index('tickerBond')

        result_df = last_day_df['closePriceBond'] - current_df['closePriceBond']
        result_list = result_df[result_df > DROP_DOWN_PRICE].index.tolist()
        if DROP_DOWN_ENABLE_LOG:
            for code in result_list:
                log.info('日期{}, {}, 价差{} , 天数{}'.format(date, code, round(result_df.loc[code], 2), DROP_DOWN_DAY))
        return result_list


price_drop_filter = DropDownFilter()
remain_size_filter = RemainSizeFilter()


class BackTrade:
    def __init__(self):
        self.position = {}
        self.HighValue = 0
        self.Start_Cash = 1000000  # 初始资金
        self.MyCash = self.Start_Cash
        self.Withdraw = 0
        self.daily_netvalue = []
        self.current_day = 0
        self.PosValue = 0
        self.threshold = 0  # 阈值
        self.HighValue = self.Start_Cash
        self.date_list, self.source = data_feed.get_date(), data_feed.get_source_data()
        self.day_count = 0
        self.last_day_dict = self.last_day_exclude_v1()
        func_dict = {'premratio': self.condition_low_premratio, 'doublelow': self.condition_doublelow,
                     'lowprice': self.condition_low_price}
        self.func = func_dict.get(condition)

    def unpossibile(self, df, date):
        # 剔除当日涨停的转债，买不入
        raise_limited_dict = {
            '2022-04-08': ['127057', ],
            '2022-07-27': ['127065', ],
            '2022-07-28': ['127065', ],
        }
        target_list = raise_limited_dict.get(date, None)
        if target_list is None:
            return df

        return df.drop(index=target_list, axis=1)

    def last_day_exclude_v1(self):
        last_day_calendar_dict={}
        for code,sub_df in self.source.groupby('tickerBond'):
            date = sub_df.index.tolist()[-1]
            date = date.strftime('%Y-%m-%d')
            if date!=END_DATE:
                last_day_calendar_dict.setdefault(date,[])
                last_day_calendar_dict[date].append(code)


        return last_day_calendar_dict


    def filters(self, df, today):
        # 过滤条件，可添加多个条件

        df = self.unpossibile(df, today)
        df = self.exclude_last(df, today)
        df = remain_size_filter.apply(df)
        price_drop_code = price_drop_filter.apply(date=today)

        origin_index = df.index.tolist()
        same_code = list(set(origin_index) & set(price_drop_code))
        df = df.loc[same_code]
        return df

    def exclude_last(self, df, today):

        if today in self.last_day_dict:
            for item in self.last_day_dict[today]:
                df = df.drop(index=item, axis=1)

        return df

    def logprint(self, current):
        log.info('当前日期{}'.format(current))

    def run(self):
        for current in self.date_list:

            if current < START_DATE or current > END_DATE:
                continue

            if self.day_count % FREQ != 0:
                self.get_daily_netvalue(current)
            else:
                self.handle_data(current)

            self.day_count += 1

        self.after_trade()

    def get_daily_netvalue(self, today):
        ticker_list = list(self.position.keys())
        if len(ticker_list) == 0:
            log.info('{}没有持仓'.format(today))
            return

        current_day_df = self.source.loc[today]
        current_day_df = current_day_df.set_index('tickerBond', drop=False)

        last_post = self.PosValue
        self.PosValue = self.MyCash

        for ticker in ticker_list:
            current_price, forced = self.get_current_price(current_day_df, ticker, today)
            bond_pos = self.position[ticker] * current_price * 10
            self.PosValue += bond_pos
            if forced:
                log.info('强赎卖出{} {}'.format(ticker, today))
                self.MyCash += bond_pos
                del self.position[ticker]

        self.update_max_withdraw()  # 更新最大回撤

        today_pct = (self.PosValue - last_post) / last_post * 100
        ratio = round((self.PosValue - self.Start_Cash) / self.Start_Cash * 100, 2)

        log.info('{} 累计收益率 {}%, {}, 日收益率 {} 最大回撤{}\n'.format(today, ratio, round(self.PosValue, 2),
                                                                          round(today_pct, 2),
                                                                          round(self.Withdraw * 100, 2)))
        self.daily_netvalue.append({'日期': today, '当前市值': self.PosValue, '收益率': ratio,
                                    '最大回撤': round(self.Withdraw * 100, 2), 'daily_profit': today_pct})

    def handle_data(self, today):
        self.logprint(today)
        today_df = self.source.loc[today]
        self.strategy(today_df, today)

    def after_trade(self):
        today = datetime.date.today()
        df = pd.DataFrame(self.daily_netvalue)
        df.to_excel('./net-profit-{}-{}-{}.xlsx'.format(condition, str(today), FREQ), encoding='utf8')

    def get_last_price(self, code, date):
        previous = last_n_day(date, 30, '%Y-%m-%d', '%Y-%m-%d')
        series = self.source.loc[self.source['tickerBond'] == code].loc[previous:].iloc[-1]
        return series['closePriceBond']

    def update_max_withdraw(self):
        if self.PosValue > self.HighValue: self.HighValue = self.PosValue
        if (self.HighValue - self.PosValue) / self.HighValue > self.Withdraw:
            self.Withdraw = (self.HighValue - self.PosValue) / self.HighValue

    def get_current_price(self, df, code, date):
        forced = False
        try:
            current_price = df.loc[code]['closePriceBond']
        except Exception as e:
            log.info('{}强赎卖出{}'.format(date, code))
            current_price = self.get_last_price(code, date)
            forced = True
        return current_price, forced

    def condition_low_premratio(self, df):
        # 条件 低溢价
        target = df.sort_values('bondPremRatio', ascending=True)
        return target

    def condition_doublelow(self, df):
        # 条件 低溢价
        # 自定义 权重，默认 1:1
        w1 = 1
        w2 = 1
        df['doublelow'] = df['bondPremRatio'] * w1 + df['closePriceBond'] * w2
        target = df.sort_values('doublelow', ascending=True)
        return target

    def condition_low_price(self, df):
        # 条件 低价格
        target = df.sort_values('closePriceBond', ascending=True)
        return target

    def strategy(self, current_day_df, date):
        current_day_df = current_day_df.set_index('tickerBond', drop=False)

        target_day_df = self.filters(current_day_df, date)

        target = self.func(target_day_df)

        target = target[:HOLD_NUM+threshold]

        target_name_dict = dict(zip(target['tickerBond'].tolist(), target['secShortNameBond'].tolist()))
        target_list = list(target_name_dict.keys())

        self.PosValue = self.MyCash
        for hold_code in self.position.copy().keys():
            current_price, forced = self.get_current_price(current_day_df, hold_code, date)
            self.PosValue += self.position[hold_code] * current_price * 10
            if hold_code not in target_list:
                self.MyCash += self.position[hold_code] * current_price * 10
                try:
                    secShortNameBond = current_day_df.loc[hold_code]['secShortNameBond']
                except:
                    secShortNameBond = ''

                del self.position[hold_code]
                log.info('{} 卖出{} {} 价格{} '.format(date, hold_code, secShortNameBond, current_price))

        self.update_max_withdraw()  # 更新最大回撤
        # 卖出
        for i in range(min(HOLD_NUM+threshold, len(target_list))):
            if len(self.position) == HOLD_NUM: break

            code = target_list[i]
            if code not in self.position:
                buy_price = target_day_df.loc[code]['closePriceBond']
                bondPremRatio = target_day_df.loc[code]['bondPremRatio']
                secShortNameBond = target_day_df.loc[code]['secShortNameBond']
                amount = int(self.MyCash / (HOLD_NUM - len(self.position)) / buy_price / 100) * 10  # TODO 取整数
                self.position[code] = amount
                self.MyCash -= amount * buy_price * 10

                log.info('{} 买入{} {}, 价格  {}, 数量{} ,溢价率 {}'.format(date, code, secShortNameBond, buy_price,
                                                                            amount * 10, bondPremRatio))
        ratio = round((self.PosValue - self.Start_Cash) / self.Start_Cash * 100, 2)

        if len(self.daily_netvalue) > 0:
            last_trading_date_value = self.daily_netvalue[-1]
            daily_profit = round(
                (self.PosValue - last_trading_date_value['当前市值']) / last_trading_date_value['当前市值'] * 100, 2)
        else:
            daily_profit = ratio

        log.info('{} 累计收益率 {}%, {}, 日收益率 {}% ,最大回撤{}\n'.format(date, ratio, round(self.PosValue, 2),
                                                                            round(daily_profit, 2),
                                                                            round(self.Withdraw * 100, 2)))



        self.daily_netvalue.append(
            {'日期': date, '当前市值': self.PosValue, '收益率': ratio, '最大回撤': round(self.Withdraw * 100, 2),
             'daily_profit': daily_profit})

        self.display_position(target_name_dict)

    def display_position(self,name_dict):
        log.info('持仓信息')
        log.info('='*20)
        for k,v in self.position.items():
            log.info('代码 {} {}，数量{}'.format(k,name_dict.get(k,""),v))
        log.info('='*20)



if __name__ == '__main__':
    backtrade = BackTrade()
    backtrade.run()

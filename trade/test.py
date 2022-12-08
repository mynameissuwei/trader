import pandas as pd
import datetime
from loguru import logger as log

CSV_PATH = 'merge_bond_daily.csv'
HOLD_NUM = 10 # 持有个数

START_DATE = '2022-01-01'
END_DATE = '2022-08-25'

FREQ = 5  # 5天轮动一次 轮动频率


def last_n_day(date, n, origin_fmt, target_fmt):
    return (datetime.datetime.strptime(date, origin_fmt) - datetime.timedelta(days=n)).strftime(target_fmt)

class BackTest:
    def __init__(self):
        self.csv_path = CSV_PATH
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
        self.date_list, self.source = self.feed()
        self.day_count = 0

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

    def feed(self):
        df = pd.read_csv(self.csv_path,
                         encoding='utf8',
                         dtype={'tickerEqu': str, 'tickerBond': str, 'secID_x': str},
                         )
        del df['Unnamed: 0']
        df['tradeDate'] = pd.to_datetime(df['tradeDate'], format='%Y-%m-%d')
        df = df.set_index('tradeDate')
        date_set = set(df.index.tolist())
        date_list = list(map(lambda x: x.strftime('%Y-%m-%d'), date_set))
        date_list.sort()
        return date_list, df

    def filters(self, df, today):
        # 过滤条件，可添加多个条件
        df = self.unpossibile(df, today)
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
            current_price = self.get_current_price(current_day_df,ticker,today)
            bond_pos = self.position[ticker] * current_price * 10
            self.PosValue += bond_pos

        self.update_max_withdraw()  # 更新最大回撤

        today_pct = (self.PosValue - last_post) / last_post * 100
        ratio = round((self.PosValue - self.Start_Cash) / self.Start_Cash * 100, 2)

        log.info('{} 累计收益率 {}%, {}, 日收益率 {} 最大回撤{}\n'.format(today, ratio, self.PosValue, today_pct,
                                                                          self.Withdraw * 100))
        self.daily_netvalue.append({'日期': today, '当前市值': self.PosValue, '收益率': ratio,
                                    '最大回撤': round(self.Withdraw * 100, 2), 'daily_profit': today_pct})

    def handle_data(self, today):
        self.logprint(today)
        today_df = self.source.loc[today]
        self.strategy(today_df, today)

    def after_trade(self):
        df = pd.DataFrame(self.daily_netvalue)
        df.to_excel('daily.xlsx', encoding='utf8')

    def get_last_price(self, code, date):
        previous = last_n_day(date, 30, '%Y-%m-%d', '%Y-%m-%d')
        series = self.source.loc[self.source['tickerBond'] == code].loc[previous:].iloc[-1]
        return series['closePriceBond']

    def update_max_withdraw(self):
        if self.PosValue > self.HighValue: self.HighValue = self.PosValue
        if (self.HighValue - self.PosValue) / self.HighValue > self.Withdraw:
            self.Withdraw = (self.HighValue - self.PosValue) / self.HighValue

    def get_current_price(self,df,code,date):
        try:
            current_price = df.loc[code]['closePriceBond']
        except Exception as e:
            current_price = self.get_last_price(code, date)
        return current_price


    def condition_low_premratio(self,df):
        # 条件 低溢价
        target = df.sort_values('bondPremRatio', ascending=True)[:HOLD_NUM]
        target_name_dict = dict(zip(target['tickerBond'].tolist(), target['secShortNameBond'].tolist()))
        return target_name_dict

    def condition_doublelow(self,df):
        # 条件 低溢价
        # 自定义 权重，集思录 默认 1:1
        w1=1
        w2=1
        df['doublelow']=df['bondPremRatio']*w1+df['closePriceBond']*w2

        target = df.sort_values('doublelow', ascending=True)[:HOLD_NUM]
        target_name_dict = dict(zip(target['tickerBond'].tolist(), target['secShortNameBond'].tolist()))
        return target_name_dict


    def strategy(self, current_day_df, date):
        current_day_df = current_day_df.set_index('tickerBond', drop=False)

        target_day_df = self.filters(current_day_df, date)
        # target_name_dict = self.condition_doublelow(target_day_df)
        target_name_dict = self.condition_low_premratio(target_day_df)

        target_list=list(target_name_dict.keys())
        log.info('当前持仓{}'.format(target_name_dict))

        self.PosValue = self.MyCash
        for hold_code in self.position.copy().keys():
            current_price = self.get_current_price(current_day_df,hold_code,date)
            self.PosValue += self.position[hold_code] * current_price * 10
            if hold_code not in target_list:
                self.MyCash += self.position[hold_code] * current_price * 10
                del self.position[hold_code]
                log.info('{} 卖出{} 价格{}'.format(date, hold_code, current_price))

        self.update_max_withdraw()  # 更新最大回撤
        # 卖出
        for i in range(min(HOLD_NUM, len(target_list))):
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

        log.info('{} 累计收益率 {}%, {}, 日收益率 {} 最大回撤{}\n'.format(date, ratio, self.PosValue, daily_profit,
                                                                          self.Withdraw * 100))
        self.daily_netvalue.append(
            {'日期': date, '当前市值': self.PosValue, '收益率': ratio, '最大回撤': round(self.Withdraw * 100, 2),
             'daily_profit': daily_profit})

if __name__ == '__main__':
    backtrade = BackTest()
    backtrade.run()

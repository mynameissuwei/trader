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

date_list = data_feed.get_date()

print(date_list,'date_list')

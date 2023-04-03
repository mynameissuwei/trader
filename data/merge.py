# 优矿历史数据文件，csv格式
# 集思录每日盘后数据，（csv和excel数据格式均可），下文以excel格式为主演示
# tushare的token，没有的话去注册一个就可以，免费。
import datetime
import pandas as pd
import tushare as ts
import akshare as ak




# start_date = '2022-08-26' # 优矿停止当天
start_date = '2023-04-02' # 优矿停止当天
ts_token_pro = 'a792a69fe8769272620c5b2e6f23d741af040e0c366931934ae1c6f4' # tushare token ，
uqer_file = '2017-2022-08-25日线数据.csv' 

JSL_FILE_NAME = 'jsl_{}.xlsx' # 平时保存的集思录excel文件，文件名 需要有日期，如 集思录_2022-08-08.xlsx，集思录_2022-08-09.xlsx
fmt = '%Y-%m-%d' # 文件的日期格式

today = datetime.datetime.now().strftime(fmt)
uqer_df = pd.read_csv(uqer_file,encoding='utf8',dtype={'tickerBond':str,'tickerEqu':str,'tradeDate':str})

ts.set_token(ts_token_pro)
pro = ts.pro_api()

# 使用股市日历，避免平时节假日也把集思录的数据也抓下来，合并进去
def calendar():
    df = ak.tool_trade_date_hist_sina()
    cal = df['trade_date'].tolist()
    cal = list(map(fmt_date, cal))
    return cal



def run():
    global start_date
    cal = calendar()
    result_df = []
    result_df.append(uqer_df)

    while start_date <= today:

        if start_date in cal:
            print('更新数据{}'.format(start_date))
            filename = JSL_FILE_NAME.format(start_date)
            print(filename,'filename')

            try:
                df = pd.read_excel(filename)
            except Exception as e:
                print(e)
            else:
                df['tradeDate']=start_date
                df=df.rename(columns={'bond_id':'tickerBond','bond_nm':'secShortNameBond','stock_id':'tickerEqu',
                                      'price':'closePriceBond','curr_iss_amt':'remainSize','convert_price':'convPrice',
                                      'orig_iss_amt':'totalSize','increase_rt':'chgPct','premium_rt':'bondPremRatio',
                                      'sprice':'closePriceEqu','turnover_rt':'turnover_rt','volume':'turnoverVol','stock_nm':'secShortNameEqu'
                                      })

                result_df.append(df)




        start_date = (datetime.datetime.strptime(start_date, fmt) + datetime.timedelta(days=1)).strftime(fmt)
        merge_df = pd.concat(result_df)
        merge_df = merge_df.reset_index()
        merge_df.to_csv('merge_bond_info-{}.csv'.format(today),encoding='utf8')

def fmt_date(x):
    return x.strftime('%Y-%m-%d')

def main():
    run()

if __name__ == '__main__':
    main()
import easytrader
from scrapy import getData
from send import send_message
import pandas as pd
import datetime


def ranking(df,condition1='premium_rt'):
    NUM = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    return df

def fa(x):
  if x.startswith('110') or x.startswith('113'):
    return 'SH' + x
  if x.startswith('123') or x.startswith('127') or x.startswith('128'):
    return 'SZ' + x

def getName(data):
  result = map(fa,data)
  result = list(result)
  return result

# 指定雪球
user = easytrader.use('xq')
# 初始化信息
user.prepare(
  cookies='Hm_lvt_1db88642e346389874251b5a1eded6e3=1670309701; device_id=5f22847ae9031689d81505e66b19a7b6; acw_tc=276077a816704692117161232e654217f5394ee7e89e764b36547eb3953413; s=cl1z8sd4r2; xq_a_token=9add626bc70c0775e19e573b1d90de6ca6d137bb; xqat=9add626bc70c0775e19e573b1d90de6ca6d137bb; xq_r_token=e8cf2c2e8dfaa0b6532b0db80de837953114efed; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg2NzQ2MDE2MDYsImlzcyI6InVjIiwiZXhwIjoxNjczMDYxMjI3LCJjdG0iOjE2NzA0NjkyMjc0NTcsImNpZCI6ImQ5ZDBuNEFadXAifQ.LJ5B-IUmgLvd-mNH2kB36CqtY246wRDqmR2-ycnBY6ixoZFl0N0d1tB3f3T6QVBt97aLkic92Nelc9xxtpXpaGXLVmebIpF-LIiXQgykdfqBb_-sEtxAmq0m_4N7NOOFdM3bP_dgSgDfvcd1ZoPszQ4j8F7E5eO6bp_LfU2GiZ1_bXN0Mh4-y0yB2ia3VfqN360aXhEWpbAiwlaCdpmi1XMhTMuhpfQlgASMtb9kLHXiwTlCjQDzIGFT8fFjNccz4lsLCFzccepb7vLj0PWbIRVf6-4DuxNCSX6phmI_uK14onocnmO5nWB54o7axz42FzVfpKCDIZxyvZdrgwI9fw; xq_is_login=1; u=8674601606; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1670469370',
  portfolio_code='ZH2476354',  
  portfolio_market='cn'
)
# 打印账户
# print(user.position,'position')
print(user.balance)
currentData = list(map(lambda x:x['stock_code'],user.position))
# 打印持仓
df = getData()
df = ranking(df)
today = datetime.datetime.now().strftime('%Y%m%d')
writer = pd.ExcelWriter('jsl_{}.xlsx'.format(today))
df.to_excel(writer,'低溢价')

dfName = list(df['bond_nm'])
df = getName(list(df['bond_id']))

removeData = list(set(currentData).difference(set(df)))
buyData =  list(set(df).difference(set(currentData)))

print(currentData,'currentData')
print(df,'dffff')
for x in currentData:
  try:
    user.adjust_weight(x, 0)
  except Exception:
    print('asdfasdfasfd')
    user.adjust_weight('SH512690', 10)
# user.adjust_weight('SH512690', 10)
for x in df:
  print(x,'xxxxx')
  try:
    user.adjust_weight(x, 10)
  except Exception:
    print('asdfasdfasfd')
    user.adjust_weight('SH512690', 10)


# userId = ['SuWei','DanErShenYang','BaoChiBengGan','MaoXiaoMao','life']
userId = ['SuWei']


# text1 = " 组合收益率: %.2f%% \n 组合雪球链接 %s \n 今日卖出: %s \n 今日买进: %s \n 当前持仓: %s" % (user.balance[0]['asset_balance'] / 10000,'https://xueqiu.com/P/ZH2476354',','.join(removeData),','.join(buyData),','.join(dfName))

text1 = "组合收益率: %.2f%% \n 组合雪球链接 %s "  % (user.balance[0]['asset_balance'] / 10000,'https://xueqiu.com/P/ZH2476354')

# try:
#     writer.save()
# except Exception as e:
#     print(e)
# else:
#     print('导出excel成功')

for item in userId:
  send_message(text1,item)



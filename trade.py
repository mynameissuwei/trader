import easytrader
from scrapy import getData
from send import send_message

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
  cookies='device_id=bcffa4f5bde5109e6bd563dbee92085b; s=dg15btnbrl; xq_a_token=b3b57cd5d04fc38f27643d826d635b8883e70838; xqat=b3b57cd5d04fc38f27643d826d635b8883e70838; xq_r_token=a4f252fa5d01c9e356a2e4aade41c0d0f6444bfa; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg2NzQ2MDE2MDYsImlzcyI6InVjIiwiZXhwIjoxNjYyMTg0MTk5LCJjdG0iOjE2NTk1OTIxOTkzOTksImNpZCI6ImQ5ZDBuNEFadXAifQ.p_M5v47Uf7aEbHBTEMxMQizYelklWspwU31qRfX5DImP1yftQTOQeCGJBRg4mN72FjJn5RbjGwn42zYY6hOaz4N2f8kcWkkkKhijdkQ57VWemdiwYoCo-nzUNkPJgkkTfjwV6SkQzjN-daaQX8J217d0qPGSbRoxtH2jZGPT8rUFa5ziiTZKgxiHekCNKfOmyCQyf0STCzI36GXy6AvTTNf0bN5HNpk-8ldWloD4BbQLYu4rH1s9rryLZ-QjWiGTgIS0GgAb_-RT763JLBWTcm10S87JlobHqS5H5oN-0DWGyyL_H8gB40Sv75VG0JOB2C3U3jnHJ91hUgyh4eHbBg; xq_is_login=1; u=8674601606; bid=2a430e784436d90c894fa77e3fed0d71_l6emd53p; Hm_lvt_1db88642e346389874251b5a1eded6e3=1659592051,1660274508; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1660274508',
  portfolio_code='ZH2476354',  
  portfolio_market='cn'
)
# 打印账户
print(user.balance)
# print(user.position,'position')
currentData = list(map(lambda x:x['stock_code'],user.position))
# 打印持仓
# print(user.position)
df = getData()
df = ranking(df)
dfName = list(df['bond_nm'])
df = getName(list(df['bond_id']))

removeData = list(set(currentData).difference(set(df)))
buyData =  list(set(df).difference(set(currentData)))

for x in removeData:
  user.adjust_weight(x, 0)
for x in df:
  user.adjust_weight(x, 10)

userId = ['SuWei','DanErShenYang','BaoChiBengGan','MaoXiaoMao','life']

text1 = " 组合收益率: %.2f%% \n 组合雪球链接 %s \n 今日卖出: %s \n 今日买进: %s \n 当前持仓: %s" % (user.balance[0]['asset_balance'] / 10000,'https://xueqiu.com/P/ZH2476354',','.join(removeData),','.join(buyData),','.join(dfName))
# for item in userId:
#   send_message(text1,item)



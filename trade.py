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
  cookies='device_id=5f22847ae9031689d81505e66b19a7b6; s=cl1z8sd4r2; xq_is_login=1; u=8674601606; bid=2a430e784436d90c894fa77e3fed0d71_les65f1u; Hm_lvt_fe218c11eab60b6ab1b6f84fb38bcc4a=1677835239; xq_a_token=21a26baf2a683d05f3a1f26dc0dc16224fbd4445; xqat=21a26baf2a683d05f3a1f26dc0dc16224fbd4445; xq_r_token=3848168deaeb635170dfe498e5765c81cfd47ce3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjg2NzQ2MDE2MDYsImlzcyI6InVjIiwiZXhwIjoxNjgyNzM1MDczLCJjdG0iOjE2ODAxNDMwNzMwODksImNpZCI6ImQ5ZDBuNEFadXAifQ.S7IFwNFr51hs88Cy9sscMZkZSQiwnjic30THQxLaTNNLjrW5kxz1lHekUAqVihp8I0TPIUu0Dz9W1q00n49y_jpj2qeO7O-6WkxP3WTXbYmf44F2uN4TPdVmja_wGU8VF67ime5h2SurTXXQ6tHMhQTmpWMRk6HCfpMsu3vw-7QGH-w6aQAuJkQtqXnhvIBVSNBd2MYSu217w2VLWMgZTx5cXjirm9twa_gMAq7LAYnr6z7JYwa9hzKA9BB_Ai1vAzl3_FgBYyfnSR1uI6O6gYEkMNlMC62PkRpvM_TCulwrgp9Tn8rGKE_2Nn_Gm98ovrSKYASyv0ofqGVM-Sjazw; Hm_lvt_1db88642e346389874251b5a1eded6e3=1677825441,1680143074; __utma=1.907161972.1680143074.1680143074.1680143074.1; __utmc=1; __utmz=1.1680143074.1.1.utmcsr=ww2.ezhai.net.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; acw_tc=2760827c16801649470976684e3d38f7beb2820d88609a6030f209dc2bd33c; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1680164990; is_overseas=0',
  portfolio_code='ZH2476354',  
  portfolio_market='cn'
)

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



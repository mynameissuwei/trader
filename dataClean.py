from scrapy import getData
import pandas as pd
import datetime

# 低余额40 低溢价10 
def ranking_low_small(df,condition1='curr_iss_amt',condition2='premium_rt'):
    NUM = 40
    HoldNum = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    return df

# 低余额<3 低溢价10 
def ranking_low3_small(df,condition1='curr_iss_amt',condition2='premium_rt'):
    HoldNum = 10
    df = df[df[condition1] < 3]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    return df

# 低余额40 低溢价20 双低10 
def ranking_low_small_dblow(df,condition1='curr_iss_amt',condition2='premium_rt',condition3='dblow'):
    NUM = 40
    HoldNum = 20
    BuyNum = 10

    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    df = df.sort_values(by=condition3,ascending=True)[:BuyNum]
    return df

# 低溢价
def ranking_low(df,condition1='premium_rt'):
    NUM = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    return df

# 低余额40 双低10
def ranking_dblow_small(df,condition1='curr_iss_amt',condition2='dblow'):
    NUM = 40
    HoldNum = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    return df

# 低双低40 余额10
def ranking_xxx(df,condition1='dblow',condition2='curr_iss_amt'):
    NUM = 40
    HoldNum = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    return df

# 低双低40 余额10
def ranking_fff(df,condition1='curr_iss_amt',condition2='doublelow'):
    NUM = 40
    HoldNum = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=True)[:HoldNum]
    return df

# 妖债 余额前40 换手率前10
def ranking_remain_turnover(df,condition1='curr_iss_amt',condition2='turnover_rt'):
    NUM = 40
    HoldNum = 10
    df = df.sort_values(by=condition1,ascending=True)[:NUM]
    df = df.sort_values(by=condition2,ascending=False)[:HoldNum]
    return df

# 平价底价溢价率 高价 低溢价 
def ranking_high(df,condition1='price'):
    NUM = 10
    df = df.sort_values(by=condition1,ascending=False)[:NUM]
    return df


def main(): # 主函数
    today = datetime.datetime.now().strftime('%Y%m%d')
    df = getData()
    df = df[['bond_id','bond_nm','premium_rt','price','dblow','curr_iss_amt','increase_rt','turnover_rt']]
    df["price"] = pd.to_numeric(df["price"],errors='coerce')
    df["turnover_rt"] = pd.to_numeric(df["turnover_rt"],errors='coerce')
    df["curr_iss_amt"] = pd.to_numeric(df["curr_iss_amt"],errors='coerce')
    # 老双低
    df["dblow"] = pd.to_numeric(df["dblow"],errors='coerce')
    # df['premium_rt'] = df['premium_rt'].str.replace(r'%', '', regex=True)
    
    df["premium_rt"] = pd.to_numeric(df["premium_rt"],errors='coerce')
    # df['increase_rt'] = df['increase_rt'].str.replace(r'%', '', regex=True)
    df["increase_rt"] = pd.to_numeric(df["increase_rt"],errors='coerce')
    # 新双低
    df['doublelow'] = df['price'] * 0.8  + df['premium_rt'] * 1.2

    writer = pd.ExcelWriter('jsl_{}.xlsx'.format(today))
    
    # 低溢价进行对比
    filter_low_data = ranking_low(df.copy())
    # lastday_low_data = pd.read_excel('jsl_20220302.xlsx')
    
    # current_position = set(lastday_low_data.bond_nm).difference(filter_low_data.bond_nm)
    # target_position = set(filter_low_data.bond_nm).difference(lastday_low_data.bond_nm)

    # print(current_position,target_position,'differnectData')
    # 
    filter_low_small_data = ranking_low_small(df.copy())
    filter_low3_small_data = ranking_low3_small(df.copy())
    filter_dblow_remain_data = ranking_dblow_small(df.copy())
    filter_xxx = ranking_xxx(df.copy())
    filter_fff = ranking_fff(df.copy())
    filter_remain_turnover_data = ranking_remain_turnover(df.copy())
    filter_high_data = ranking_high(df.copy())
    filter_dblow_remain_low_data = ranking_low_small_dblow(df.copy())

    filter_low3_small_data.to_excel(writer,'低余额<3 低溢价10')
    filter_low_data.to_excel(writer,'低溢价')
    filter_low_small_data.to_excel(writer,'低余额40 低溢价10')
    filter_dblow_remain_data.to_excel(writer,'低余额40 双低10')
    filter_fff.to_excel(writer,'低余额40 新双低10')
    filter_xxx.to_excel(writer,'老双低40 低余额10')
    filter_dblow_remain_low_data.to_excel(writer,'低余额40 低溢价20 双低10')
    filter_remain_turnover_data.to_excel(writer,'低余额前40 换手率前10')
    filter_high_data.to_excel(writer,'平价底价溢价率 高价10 低溢价')




    # resultString = ' '.join(list(filter_data['bond_nm']))
    # wx_send.wx_send(title='双低加规模', content=resultString)
    try:
        writer.save()
    except Exception as e:
        print(e)
    else:
        print('导出excel成功')


if __name__ == '__main__':
    main()
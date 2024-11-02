import requests
import json
import baostock as bs
import pandas as pd
from datetime import datetime

# 登录baostock
lg = bs.login()
print('login respond error_code:' + lg.error_code)
print('login respond error_msg:' + lg.error_msg)

# 获取所有股票的基本信息
rs = bs.query_stock_basic()
print('query_stock_basic error_code:' + rs.error_code)
print('query_stock_basic error_msg:' + rs.error_msg)

# 打印结果集
stock_list = []
while (rs.error_code == '0') & rs.next():
    stock_list.append(rs.get_row_data())

result = pd.DataFrame(stock_list, columns=rs.fields)
print(result)

# 保存股票代码列表
stock_codes = [(code.split('.')[-1], name) for code, name in zip(result['code'].tolist(), result['code_name'].tolist())]

# 登出系统
bs.logout()

def fetch_stock_data(symbol):
    url = f"http://127.0.0.1:8080/api/public/stock_zh_a_hist?symbol={symbol}"
    response = requests.get(url)
    return json.loads(response.text)

def calculate_kdj(data, n=9, m1=3, m2=3):
    kdj_values = []
    k, d = 50, 50  # 初始化K和D的值
    
    for i in range(len(data)):
        if i < n - 1:
            kdj_values.append({'date': data[i]['日期'], 'K': None, 'D': None, 'J': None})
            continue
        
        # 计算最近n天的最高价和最低价
        high_prices = [float(d['最高']) for d in data[i-n+1:i+1]]
        low_prices = [float(d['最低']) for d in data[i-n+1:i+1]]
        close_price = float(data[i]['收盘'])
        
        highest_high = max(high_prices)
        lowest_low = min(low_prices)
        
        if highest_high == lowest_low:
            rsv = 100  # 防止除零错误
        else:
            rsv = (close_price - lowest_low) / (highest_high - lowest_low) * 100
        
        # 计算K值
        k = ((m1 - 1) * k + rsv) / m1
        # 计算D值
        d = ((m2 - 1) * d + k) / m2
        # 计算J值
        j = 3 * k - 2 * d
        
        kdj_values.append({'date': data[i]['日期'], 'K': round(k, 2), 'D': round(d, 2), 'J': round(j, 2)})
    
    return kdj_values

# 定义计算周线和月线KDJ的函数
def calculate_weekly_monthly_kdj(data, period='W'):
    df = pd.DataFrame(data)
    df['日期'] = pd.to_datetime(df['日期'])
    
    # 按周或月聚合数据
    if period == 'W':
        df = df.resample('W', on='日期').agg({
            '开盘': 'first',
            '收盘': 'last',
            '最高': 'max',
            '最低': 'min',
            '成交量': 'sum',
            '成交额': 'sum'
        })
    elif period == 'M':
        df = df.resample('ME', on='日期').agg({
            '开盘': 'first',
            '收盘': 'last',
            '最高': 'max',
            '最低': 'min',
            '成交量': 'sum',
            '成交额': 'sum'
        })
    
    df = df.dropna().reset_index()
    data_list = df.to_dict(orient='records')
    return calculate_kdj(data_list)

if __name__ == "__main__":
    # symbol = "002594"  # 股票代码
    all_kdj_results = []
    negative_j_stocks = []
    for symbol, name in stock_codes:
        try:
            stock_data = fetch_stock_data(str(symbol))

            # kdj_results = calculate_kdj(stock_data)
            # if kdj_results:
            #     latest_kdj = kdj_results[-1]
            #     latest_kdj['symbol'] = symbol
            #     all_kdj_results.append(latest_kdj)
            #     # print(kdj_results[-1])
            #     # for result in kdj_results:
            #     #     print(result)

            # 计算每日KDJ
            daily_kdj_results = calculate_kdj(stock_data)
            if daily_kdj_results:
                latest_daily_kdj = daily_kdj_results[-1]
                latest_daily_kdj['symbol'] = symbol
                latest_daily_kdj['name'] = name
                latest_daily_kdj['period'] = 'daily'
                all_kdj_results.append(latest_daily_kdj)
                if latest_daily_kdj['J'] < 0:
                    negative_j_stocks.append(latest_daily_kdj)
            
            # 计算周线KDJ
            weekly_kdj_results = calculate_weekly_monthly_kdj(stock_data, period='W')
            if weekly_kdj_results:
                latest_weekly_kdj = weekly_kdj_results[-1]
                latest_weekly_kdj['symbol'] = symbol
                latest_weekly_kdj['name'] = name
                latest_weekly_kdj['period'] = 'weekly'
                all_kdj_results.append(latest_weekly_kdj)
                if latest_weekly_kdj['J'] < 0:
                    negative_j_stocks.append(latest_weekly_kdj)
            
            # 计算月线KDJ
            monthly_kdj_results = calculate_weekly_monthly_kdj(stock_data, period='M')
            if monthly_kdj_results:
                latest_monthly_kdj = monthly_kdj_results[-1]
                latest_monthly_kdj['symbol'] = symbol
                latest_monthly_kdj['name'] = name
                latest_monthly_kdj['period'] = 'monthly'
                all_kdj_results.append(latest_monthly_kdj)
                if latest_monthly_kdj['J'] < 0:
                    negative_j_stocks.append(latest_monthly_kdj)

        except Exception as e:
            print(f"Error processing stock {symbol}: {e}")

    
    # 将结果保存为CSV文件
    df = pd.DataFrame(all_kdj_results)
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f'kdj_{date_str}.csv'
    df.to_csv(filename, index=False)
    print(f"KDJ results saved to {filename}")

    # 将J值为负数的股票保存为CSV文件
    df_negative = pd.DataFrame(negative_j_stocks)
    filename_negative = f'negative_j_stocks_{date_str}.csv'
    df_negative.to_csv(filename_negative, index=False)
    print(f"Negative J stocks saved to {filename_negative}")

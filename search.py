import baostock as bs
import pandas as pd
import datetime
import numpy as np
from chinese_calendar import is_workday
import argparse

parser = argparse.ArgumentParser(description='searchstock')
parser.add_argument('--mode',default='var',type=str,help="select mode:var,kdj,default=var")
parser.add_argument('--threshold',default=0.1,type=float,help='code var threshold, default=0.04')
parser.add_argument('--days',default=30,type=int,help='search continue smooth days, default=30')
parser.add_argument('--kdjdays',default=365,type=int,help='compute kdj days of work, default=220')
parser.add_argument('--market',default="sz,sh",type=str,help='search market, default=sz,sh')
parser.add_argument('--sort',default=True,type=str,help='is sort, default=True')
args = parser.parse_args()

def get_allcode():
    rs = bs.query_all_stock(day="2022-01-24")
    print("error code:"+rs.error_code)
    print("error msg:"+rs.error_msg)

    datalist = []
    while rs.error_code=='0' and rs.next():
        datalist.append(rs.get_row_data()[0])
    return datalist

def get_price(start_date,end_date,code):
    rs = bs.query_history_k_data_plus(code,
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date=start_date,end_date=end_date,frequency="d",adjustflag="2")
    pricelist = []
    while rs.error_code=='0' and rs.next():
        pricelist.append(rs.get_row_data()[5])
    return pricelist

def get_codeinfo(start_date,end_date,code):
    rs = bs.query_history_k_data_plus(code,
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date=start_date,end_date=end_date,frequency="d",adjustflag="2")
    codeinfolist = []
    while rs.error_code=='0' and rs.next():
        codeinfolist.append(rs.get_row_data())
    return codeinfolist

def get_date(datecount):
    today = datetime.datetime.now().date()
    enddate = today+datetime.timedelta(days=-1)
    while not is_workday(enddate):
        enddate =  enddate+datetime.timedelta(days=-1)

    startdate = enddate
    while datecount>-1:
        startdate = startdate+datetime.timedelta(days=-1)
        if is_workday(startdate):
            datecount -= 1
    return startdate.strftime('%Y-%m-%d'),enddate.strftime('%Y-%m-%d')

def get_var(pricelist):
    np_price = np.array(pricelist).astype(np.float)
    _range = np.max(np_price) - np.min(np_price)
    if _range == 0.0:
        return 0.0
    normalization = (np_price-np.min(np_price))/_range
    var = np.var(normalization)
    return var

def get_trend(pricelist):
    var_days = 5
    np_price = np.array(pricelist).astype(np.float)
    N = len(pricelist)
    count = N//var_days
    cp_price = np_price
    if N%var_days!=0:
        cp_price = np_price[:(var_days*count)]
    cp_price = cp_price.reshape(count,var_days)
    cp_price = np.mean(cp_price,axis=1)
    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    trueadd = is_sorted(cp_price)
    return trueadd

def mode_var(allcode):
    vardict = {}
    start_date,end_date = get_date(args.days)
    print(start_date,end_date)
    for code in allcode:
        pricelist = get_price(start_date,end_date,code)
        if len(pricelist)>1:
            basic = bs.query_stock_basic(code=code)
            basicinfo =  basic.get_row_data()
            if len(basicinfo)==0:
                continue
            codetype,status =basicinfo[4],basicinfo[5]
            if codetype == '2' or status=="0":
                continue 
            _,code_var = get_trend(pricelist),get_var(pricelist)
            market = code.split(".")[0]
            if code_var < args.threshold and market in args.market and _:
                if code_var < 0.5*args.threshold and code_var>0:
                    print('\033[1;35;46m',"code:",code,",var:",code_var,'\033[0m')
                elif code_var < 0.8*args.threshold and code_var>0:
                    print('\033[0;33m',"code:",code,",var:",code_var,'\033[0m')
                elif code_var > 0:
                    print("code:",code,",var:",code_var)
                else:
                    continue
            vardict[code] = get_var(pricelist)
    if args.sort:
        varsort = sorted(vardict.items(),key=lambda x:x[1],reverse=False)
        print(varsort[0:20])

def compute_kdj(L9,H9,Ct,K_pri,D_pri):
    if H9==L9:
        K,D,J=0,0,0
    else:
        RSV=(Ct-L9)/(H9-L9)*100 #Ct:now;L9:low of 9 days;H9:high of 9 days
        K=RSV/3+2*K_pri/3
        D=K/3+2*D_pri/3
        J=3*D-2*K
    return K,D,J

def get_kdj(codeinfolist):
    kdjlist = []
    if len(codeinfolist)>9:
        
        K_pri,D_pri = 50,50
        for i,data in enumerate(codeinfolist):
            low,high,now = float(data[4]),float(data[3]),float(data[5])
            K,D,J = compute_kdj(low,high,now,K_pri,D_pri)
            kdjlist.append([K,D,J])
            K_pri,D_pri = K,D
    return kdjlist
                
def select_kdj(kdjlist):
    K_last1,D_last1 = kdjlist[-1][0],kdjlist[-1][1]
    K_last2,D_last2 = kdjlist[-2][0],kdjlist[-2][1]
    K_last3,D_last3 = kdjlist[-3][0],kdjlist[-3][1]
    if D_last1-K_last1<1.0 and K_last1>K_last2 and D_last1>K_last1 and K_last2>K_last3 and D_last2>D_last1:
        return True
    else:
        return False

def mode_kdj(allcode):
    start_date,end_date = get_date(args.kdjdays)
    for code in allcode:
        codeinfolist = get_codeinfo(start_date,end_date,code)
        # print(codeinfolist)
        if len(codeinfolist)!=0:
            basic = bs.query_stock_basic(code=code)
            basicinfo =  basic.get_row_data()
            if len(basicinfo)==0:
                continue
            codetype,status =basicinfo[4],basicinfo[5]
            if codetype == '2' or status=="0":
                continue
            kdjlist = get_kdj(codeinfolist)
            if len(kdjlist)<3:
                continue
            if select_kdj(kdjlist):
                print(code)
            

if  __name__ == "__main__":
    lg = bs.login()

    allcode = get_allcode()
    if args.mode == "var":
        mode_var(allcode)
    elif args.mode == "kdj":
        mode_kdj(allcode)

    bs.logout()

import pandas as pd
from datetime import datetime
import numpy as np
import os
from glob import glob


# 월별 수익률을 계산하기 위해 데이터의 정제하는 함수
def create_1m_rtn(
    _df,
    _ticker,
    _start = "2010-01-01",
    _col = "Adj Close"
):
    # 데이터프레임 카피
    result = _df.copy()
    
    # 컬럼에 "Date"가 존재하면 인덱스로 만든다
    if "Date" in result.columns:
        result.set_index("Date", inplace = True)
        
    # "Date"를 datetime 타입으로 만든다
    result.index = pd.to_datetime(result.index)
    
    # 수익률을 계산할 기준일로 인덱스를 자른다
    result = result.loc[_start : , [_col]]
    
    # 년-월 형식의 컬럼을 만든다
    result['STD-YM'] = result.index.strftime("%Y-%m")

    # 월별 수익률을 담을 컬럼을 0으로 초기화한다
    result['1m_rtn'] = 0
    
    # 각 csv 파일은 ticker 별로 되어 있고 ticker를 담는다
    result['CODE'] = _ticker
    
    # 기준 년월 컬럼의 데이터에서 유일한 값들만 리스트로 만든다
    ym_list = result['STD-YM'].unique()
    
    # 만든 데이터프레임과 기준 년월 리스트를 반환한다
    return result, ym_list


# 데이터를 로드하고 월별 수익을 계산하여 데이터프레임을 결합하는 함수
def dfs_load(
    _path = "./data/csv/",
    _end = "csv",
    _start = "2010-01-01",
    _col = "Adj Close"
):
    # 한 폴더의 파일 주소 가져오기
    files = glob(f"{_path}*.{_end}")
    
    # files의 데이터로 기준 년월 컬럼을 추가한 데이터프레임을 합칠 데이터프레임
    stock_df = pd.DataFrame()
    
    # 월말의 데이터만 합칠 데이터프레임
    month_last_df = pd.DataFrame()
    

    for file in files:
        # 파일의 경로와 파일의 이름
        file_path, file_name = os.path.split(file)

        # 파일의 이름에서 ticker만 가져오기
        head, tail = os.path.splitext(file_name)
        
        # 현재 파일
        read_df = pd.read_csv(file)

        # 기준 년월 컬럼을 추가한 데이터프레임, 기준 년월 리스트
        price_df, ym_list = create_1m_rtn(read_df, head, _start, _col)
        
        # 합치기
        stock_df = pd.concat([stock_df, price_df])
        
        
        for ym in ym_list:
            
            # 현재 선택한(돌고있는) 기준 년월
            flag = price_df['STD-YM'] == ym
            
            # 구매가 (월초 수정종가)
            buy = price_df.loc[flag].iloc[0, 0]
            
            # 판매가 (월말 수정종가)
            sell = price_df.loc[flag].iloc[-1, 0]
            
            # 한 달 수익률
            m_rtn = buy / sell
            
            # 월별 수익률
            price_df.loc[flag, '1m_rtn'] = m_rtn
            
            # 월말의 데이터만 가져오기
            last_data = price_df.loc[flag, ['CODE', '1m_rtn']].tail(1)
            
            # 월말의 데이터를 합치기
            month_last_df = pd.concat([month_last_df, last_data])
            
    # 월초+월말 데이터프레임, 월말 데이터프레임
    return stock_df, month_last_df
    
    
# 월별 수익률을 기준으로 랭크를 설정하는 함수
def create_position(
    _df,
    _pct = 0.15
):
    # 월말 데이터프레임을 인덱스를 초기화하여 카피한다
    result = _df.reset_index()
    
    # _pct를 백분율로 만든다
    if _pct >= 1:
        _pct = _pct / 100
    
    # 재구조화
    # "Date" "CODE1" "CODE2" ...
    # 100129  1.114   0.988
    result = result.pivot_table(
        index = "Date",
        columns = "CODE",
        values = "1m_rtn"
    )
    
    # rank를 구하여 백분율로 표시
    result = result.rank(
        axis = 1,
        ascending = False,
        method = 'max',
        pct = True
    )
    
    # 랭크가 기준값보다 작으면 0으로 바꾼다
    result = result.where(
        result < _pct, 0
    )
    
    # 0이 아닌 것들은 모두 1로 만든다
    result[result != 0] = 1
    
    # 날짜별로 매수 신호가 발생한 종목 CODE를 담을 딕셔너리
    sig_dict = dict()
    
    
    for date in result.index:
        # 특정 날짜(date)에 대해 랭킹이 1인 종목들만 남는다
        # index를 가져오는 이유는 result[date]를 하면 결과가 Series이다
        # CODE~가 인덱스가 된다
        # CODE1    1
        # CODE2    0
        # CODE3    1
        # CODE4    0
        ticker_list = list(
            result.loc[date, result.loc[date] == 1].index
        )
        sig_dict[date] = ticker_list
    
    stock_code = list(result.columns)
    
    # 재구조화한 데이터프레임을 반환한다
    return sig_dict, stock_code


# 거래 내역컬럼을 생성하는 데이터프레임 생성하는 함수
def create_trade_book(
    _stock_df,
    _codes,
    _sig_dict
):
    # 재구조화
    # 수정종가만 가져와서 날짜/코드들 별 수정종가 테이블을 만든다
    book = _stock_df.reset_index().pivot_table(
        index = "Date",
        columns = "CODE",
        values = _stock_df.columns[0] # Adj Close
    )
    
    # 기준 년월 컬럼을 추가한다
    book['STD-YM'] = book.index.strftime("%Y-%m")

    # Position 컬럼과 Return(수익률) 컬럼을 추가한다
    for c in _codes:
        book[f"p_{c}"] = "" 
        book[f"r_{c}"] = ""
        
    # p_{ticker}라는 컬럼 생성하여 월말 데이터에 read_{ticker} 저장한다
    # 해당 날짜에 특정 종목을 매수 준비 상태로 표시
    for date, values in _sig_dict.items():
        for stock in values:
            book.loc[date, f"p_{stock}"] = f"ready_{stock}"

    return book


# 거래 내역 추가 
def create_trading(
    _book,
    _codes
):
    # 카피
    book = _book.copy() 
    
    # 매수 시작을 추적하는 변수       
    std_ym = "" # 매수가 시작된 년-월
    buy_phase = False # 매수 상태(신호)
    
    # 종목별
    for code in _codes:
        for idx in book.index:
            col = f"p_{code}"
            
            # 매수 시작 조건
            # 현재 날짜가 ""이거나 ready_{code}이면 월말 날짜가 되고
            # 현재 날짜가 ""이면 월초 또는 월말 날짜가 된다
            # 월초 날짜의 데이터의 포지션이 null이고 월말 날짜의 데이터의 포지션이 ready(매수 대기 상태)라면
            # "ready_{code}" 신호가 발생한 **다음 날** 매수 신호를 활성화
            # 매수 신호 True로 변경
            if(book.loc[idx, col] == "") & (book.shift().loc[idx, col] == f"ready_{code}"):
                std_ym = book.loc[idx, "STD-YM"]
                buy_phase = True
            
            # 매수 유지 조건
            # 매수 신호라면 (위 if문을 한 상태라면)
            if(book.loc[idx, col] == "") & (book.loc[idx, "STD-YM"] == std_ym) & (buy_phase):
                book.loc[idx, col] = f"buy_{code}"
                
            # 매수 종료 조건(월이 지나면 리셋)
            # 한번의 for문이 지나면 변수 초기화(월초-월말-월초-월말-... 순서로 idx가 들어와서)
            if book.loc[idx, col] == "":
                std_ym = ""
                buy_phase = False
                
    return book
    

# 수익률 계산 함수
def multi_returns(
    _book,
    _codes
):
    # 카피
    book = _book.copy()
    
    rtn = 1
    buy_dict = dict()
    sell_dict = dict()
    
    for idx in book.index:
        for code in _codes:
            col = f"p_{code}"
            
            # 매수 조건(1)인가 매도 조건(2)인가
            
            # 📌 매수 조건 체크 (매수 시점 찾기)
            # - 2일 전에는 "" (아무 포지션 없음)
            # - 1일 전에는 "ready_{code}" (매수 준비 상태)
            # - 현재는 "buy_{code}" (실제 매수)
            if(book.shift(2).loc[idx, col] == "") & (book.shift(1).loc[idx, col] == f"ready_{code}") & (book.loc[idx, col] == f"buy_{code}"):
                buy_dict[code] = book.loc[idx, code] # 매수가 저장
                print(f"매수일 : {idx}, 종목코드 : {code}, 매수가 : {buy_dict[code]}")
                
            # 📌 매도 조건 체크 (매도 시점 찾기)
            # - 1일 전에는 "buy_{code}" (매수 상태)
            # - 현재는 "" (매도 상태, 포지션 종료)
            elif(book.shift(1).loc[idx, col] == f"buy_{code}") & (book.loc[idx, col] == ""):
                sell_dict[code] = book.loc[idx, code] # 매도가 저장
                
                # 수익률 계산
                rtn = sell_dict[code] / buy_dict[code]
                book.loc[idx, col] = rtn
                
                # print
                print(f"매도일 : {idx}, 종목코드 : {code}, 매도가 : {sell_dict[code]}, 수익률 : {rtn}")
                
            # 📌 매수, 매도 가격 초기화 (거래가 끝났거나 신호가 없을 때)
            # (1) 조건에 맞았으면 현재 상태가 buy_{code}이고,
            # (2) 조건에 맞았음면 현재 상태가 ""이어서
            # 매도조건(2)에 맞았을 때나 null일 때 이후에 초기화를 하게 된다
            if book.loc[idx, col] == "":
                buy_dict[code] = 0
                sell_dict[code] = 0
                
    return book


# 누적 수익률 함수
def multi_acc_returns(
    _book,
    _codes
):
    book = _book.copy()
    
    # 누적 수익률 변수
    acc_rtn = 1
    
    for idx in book.index:
        count = 0
        rtn = 0
        
        for code in _codes:
            if book.loc[idx, f"r_{code}"]:
                count += 1
                rtn += book.loc[idx, f"r_{code}"]
                
        if(rtn != 0) & (count != 0):
            acc_rtn *= (rtn / count)
            print(f"누적 매도일 : {idx}, 매도 종목수 : {count}, 수익률 : {round(rtn / count, 2)}")
            
        book.loc[idx, 'acc_rtn'] = acc_rtn
        
    return book, acc_rtn
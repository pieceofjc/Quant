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
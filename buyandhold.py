from datetime import datetime
import pandas as pd

# 누적 수익률 계산기
def BuyAndHold(
    _df,
    _start = "2010-01-01",
    _end = datetime.now(),
    _col = "Adj Close"
):
    result = _df.copy()
    
    # 인덱스를 Date 컬럼으로 바꾼다
    if "Date" in result.columns:
        result.set_index("Date", inplace=True)
    
    # Date의 타입을 datetime으로
    result.index = pd.to_datetime(result.index)
    
    # 포맷에 맞지 않은 날짜가 들어오면 에러
    try:
        start = datetime.strptime(_start, "%Y-%m-%d")

        if type(_end) == str:
            end = datetime.strptime(_end, "%Y-%m-%d")
        else:
            end = _end
    except:
        print("시작 시간과 종료 시간의 포맷은 YYYY-mm-dd 입니다")
        return ""
    
    # 시작시간과 종료시간 기준으로 인덱스 필터링
    result = result.loc[start:end, [_col]]
    
    # 일별 수익률
    result['Daily Rtn'] = (result[_col].pct_change() + 1).fillna(1)
    
    # 누적 수익률
    result['acc_rtn'] = result['Daily Rtn'].cumprod()
    
    # return 데이터에 데이터프레임, 총 수익률
    return result, result.iloc[-1,2]
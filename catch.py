# -*- coding: euc-kr -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


import pandas as pd

plan_df = pd.read_csv("plan.csv", encoding='utf-8')
tags_df = pd.read_csv("tag.csv", encoding='utf-8')

plan_df['날짜'] = pd.to_datetime(plan_df['일자']).dt.date
tags_df['날짜'] = pd.to_datetime(tags_df['일자']).dt.date

def merged_df(plan, tag):
    merged_df = pd.merge(plan, tag, how='inner', 
                    left_on=['날짜', '수급자명', '요양보호사명'], 
                    right_on=['날짜', '수급자명', '요양보호사명']) 
    return merged_df  
    

def transmission_error(plan, tag):
    merged_df = pd.merge(plan, tag, how='outer', 
                    left_on=['날짜', '수급자명'], 
                    right_on=['날짜', '수급자명'],
                    indicator=True) 
    # 일치하지 않는 데이터(양쪽에만 존재하는 데이터)를 필터링하여 '전송에러'로 저장
    transmission_error_df = merged_df[merged_df['_merge'] != 'both']
    transmission_error_df = transmission_error_df[['수급자명', '날짜', '시작시간', '종료시간']]
    # 필요한 열만 선택하여 저장
    transmission_error_df.to_csv('전송에러.csv', index=False, encoding='utf-8-sig')
    
    # 일치하는 데이터만 필터링하여 반환
    merged_df = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    
    
    

def wrong_tag():
    plan_df['시작시간_datetime'] = pd.to_datetime(plan_df['시작시간'], format='%H:%M')
    tags_df['시작태그_datetime'] = pd.to_datetime(tags_df['시작태그'], format='%H:%M:%S', errors='coerce')

    merged = merged_df(plan_df, tags_df)
    
    merged['시간차이'] = (merged['시작시간_datetime'] - merged['시작태그_datetime']).abs().dt.total_seconds() / 60
    
    filtered_df = merged[merged['시간차이'] > 30]

    wrongtag_df = filtered_df[['수급자명', '요양보호사명', '날짜', '시작시간', '종료시간', '시작태그', '종료태그']]
    wrongtag_df.to_csv('일정상이.csv', index=False, encoding='utf-8-sig')
    print("일정상이 저장 완료")
    
def overtime():
    plan_df['시작시간_datetime'] = pd.to_datetime(plan_df['시작시간'], format='%H:%M')
    plan_df['종료시간_datetime'] = pd.to_datetime(plan_df['종료시간'], format='%H:%M')
    
    plan_df['총시간_계획'] = (plan_df['종료시간_datetime'] - plan_df['시작시간_datetime']).dt.total_seconds() / 60
    
    tags_df['총시간_태그'] = pd.to_numeric(tags_df['총시간'])
    
    merged = merged_df(plan_df, tags_df)
    merged['시간차이(분)'] = (merged['총시간_태그'] - merged['총시간_계획'])
    
    filtered_df = merged[merged['시간차이(분)'] > 30]
    
    overtime_df = filtered_df[['수급자명', '요양보호사명', '날짜', '시작시간', '종료시간', '총시간_계획', '시작태그', '종료태그','총시간_태그']]
    overtime_df.to_csv('시간초과.csv', index=False, encoding='utf-8-sig')
    
    

transmission_error(plan_df, tags_df)
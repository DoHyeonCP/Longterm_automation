# -*- coding: euc-kr -*-
import sys
import io
import pandas as pd
from holidayskr import is_holiday

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


class Catch:
    def __init__(self, plan_path, tag_path):
        self.plan_df = pd.read_csv(plan_path, encoding='utf-8')
        self.tags_df = pd.read_csv(tag_path, encoding='utf-8')

        self.plan_df['날짜'] = pd.to_datetime(self.plan_df['일자']).dt.date
        self.tags_df['날짜'] = pd.to_datetime(self.tags_df['일자']).dt.date
        
        self.identify_multiple_services()
        
    def identify_multiple_services(self):
        # 날짜, 수급자명, 수급자 인정번호를 기준으로 그룹화하여 서비스 횟수 계산
        group_fields = ['날짜', '수급자명', '수급자 인정번호']
        self.plan_df['시작시간_datetime'] = pd.to_datetime(self.plan_df['시작시간'], format='%H:%M')
        self.plan_df['종료시간_datetime'] = pd.to_datetime(self.plan_df['종료시간'], format='%H:%M')
        # 다중 서비스 식별
        self.plan_df['2회서비스'] = False
        for _, group in self.plan_df.groupby(group_fields):
            if len(group) > 1:  # 동일 날짜에 두 개 이상의 서비스가 있는 경우
                # 시작시간과 종료시간이 다른 데이터만 고려
                unique_times = group.drop_duplicates(subset=['시작시간_datetime', '종료시간_datetime'])
                if len(unique_times) > 1:
                    self.plan_df.loc[unique_times.index, '2회서비스'] = True

        # 다중 서비스 데이터 별도 저장
        second_service_df = self.plan_df[self.plan_df['2회서비스']]
        second_service_df.to_csv('error/2회서비스.csv', index=False, encoding='utf-8-sig')
        print("2회서비스 데이터 저장 완료")

        # 다중 서비스 데이터 제외
        self.plan_df = self.plan_df[~self.plan_df['2회서비스']]
        
        
    def merged_df(self):
        merged_df = pd.merge(self.plan_df, self.tags_df, how='inner', 
                        left_on=['날짜', '수급자명', '요양보호사명'], 
                        right_on=['날짜', '수급자명', '요양보호사명']) 
        return merged_df  
        

    def transmission_error(self):
        merged_df = pd.merge(self.plan_df, self.tags_df, how='outer', 
                        left_on=['날짜', '수급자명'], 
                        right_on=['날짜', '수급자명'],
                        indicator=True) 
        # 일치하지 않는 데이터(양쪽에만 존재하는 데이터)를 필터링하여 '전송에러'로 저장
        transmission_error_df = merged_df[merged_df['_merge'] != 'both']
        transmission_error_df = transmission_error_df[['수급자명', '날짜', '시작시간', '종료시간']]
        # 필요한 열만 선택하여 저장
        transmission_error_df.to_csv('error/전송에러.csv', index=False, encoding='utf-8-sig')
        
        print("전송에러 완료")
        
        
        

    def wrong_tag(self):
        self.plan_df['시작시간_datetime'] = pd.to_datetime(self.plan_df['시작시간'], format='%H:%M')
        self.tags_df['시작태그_datetime'] = pd.to_datetime(self.tags_df['시작태그'], format='%H:%M:%S', errors='coerce')

        merged = self.merged_df()
        
        merged['시간차이'] = (merged['시작시간_datetime'] - merged['시작태그_datetime']).abs().dt.total_seconds() / 60
        
        filtered_df = merged[merged['시간차이'] > 30]

        wrongtag_df = filtered_df[['수급자명', '요양보호사명', '날짜', '시작시간', '종료시간', '시작태그', '종료태그']]
        wrongtag_df.to_csv('error/일정상이.csv', index=False, encoding='utf-8-sig')
        print("일정상이 저장 완료")
        
    def overtime(self):
        self.plan_df['시작시간_datetime'] = pd.to_datetime(self.plan_df['시작시간'], format='%H:%M')
        self.plan_df['종료시간_datetime'] = pd.to_datetime(self.plan_df['종료시간'], format='%H:%M')
        
        self.plan_df['총시간_계획'] = (self.plan_df['종료시간_datetime'] - self.plan_df['시작시간_datetime']).dt.total_seconds() / 60
        
        self.tags_df['총시간_태그'] = pd.to_numeric(self.tags_df['총시간'])
        
        merged = self.merged_df()
        merged['시간차이(분)'] = (merged['총시간_태그'] - merged['총시간_계획'])
        
        filtered_df = merged[merged['시간차이(분)'] > 30]
        
        overtime_df = filtered_df[['수급자명', '요양보호사명', '날짜', '시작시간', '종료시간', '총시간_계획', '시작태그', '종료태그','총시간_태그']]
        overtime_df.to_csv('error/시간초과.csv', index=False, encoding='utf-8-sig')
        
        print("시간초과 완료")
        
    def holiday(self):
        self.plan_df['공휴일'] = self.plan_df['날짜'].apply(lambda x: is_holiday(x.strftime('%Y-%m-%d')) or x.weekday() == 6)
        holiday_df = self.plan_df[(self.plan_df['공휴일']) & (self.plan_df['가족여부'] == 'N')]
        holiday_df = holiday_df[['수급자명', '요양보호사명', '날짜', '시작시간', '종료시간', ]]
        holiday_df.to_csv('error/공휴일.csv', index=False, encoding='utf-8-sig')
        print("공휴일 데이터 추출 완료")
        
    def one_tag(self):
        filtered_tags = self.tags_df[self.tags_df['종료태그'] == '-']

        # 필터링된 태그 데이터와 계획 데이터를 수급자 인정번호와 날짜, 요양보호사명을 기준으로 합치기
        merged_df = pd.merge(filtered_tags, self.plan_df, on=['수급자 인정번호', '수급자명', '일자', '요양보호사명'], how='left')

        # 결과 데이터프레임 정리
        result_df = merged_df[['수급자 인정번호', '수급자명', '일자', '시작시간', '종료시간', '시작태그', '종료태그']]
        
        # CSV 파일로 저장
        result_df.to_csv('error/시작,종료태그오류.csv', index=False, encoding='utf-8-sig')
        print("시작,종료태그")
        
        
        
        

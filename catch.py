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

        self.plan_df['��¥'] = pd.to_datetime(self.plan_df['����']).dt.date
        self.tags_df['��¥'] = pd.to_datetime(self.tags_df['����']).dt.date
        
        self.identify_multiple_services()
        
    def identify_multiple_services(self):
        # ��¥, �����ڸ�, ������ ������ȣ�� �������� �׷�ȭ�Ͽ� ���� Ƚ�� ���
        group_fields = ['��¥', '�����ڸ�', '������ ������ȣ']
        self.plan_df['���۽ð�_datetime'] = pd.to_datetime(self.plan_df['���۽ð�'], format='%H:%M')
        self.plan_df['����ð�_datetime'] = pd.to_datetime(self.plan_df['����ð�'], format='%H:%M')
        # ���� ���� �ĺ�
        self.plan_df['2ȸ����'] = False
        for _, group in self.plan_df.groupby(group_fields):
            if len(group) > 1:  # ���� ��¥�� �� �� �̻��� ���񽺰� �ִ� ���
                # ���۽ð��� ����ð��� �ٸ� �����͸� ���
                unique_times = group.drop_duplicates(subset=['���۽ð�_datetime', '����ð�_datetime'])
                if len(unique_times) > 1:
                    self.plan_df.loc[unique_times.index, '2ȸ����'] = True

        # ���� ���� ������ ���� ����
        second_service_df = self.plan_df[self.plan_df['2ȸ����']]
        second_service_df.to_csv('error/2ȸ����.csv', index=False, encoding='utf-8-sig')
        print("2ȸ���� ������ ���� �Ϸ�")

        # ���� ���� ������ ����
        self.plan_df = self.plan_df[~self.plan_df['2ȸ����']]
        
        
    def merged_df(self):
        merged_df = pd.merge(self.plan_df, self.tags_df, how='inner', 
                        left_on=['��¥', '�����ڸ�', '��纸ȣ���'], 
                        right_on=['��¥', '�����ڸ�', '��纸ȣ���']) 
        return merged_df  
        

    def transmission_error(self):
        merged_df = pd.merge(self.plan_df, self.tags_df, how='outer', 
                        left_on=['��¥', '�����ڸ�'], 
                        right_on=['��¥', '�����ڸ�'],
                        indicator=True) 
        # ��ġ���� �ʴ� ������(���ʿ��� �����ϴ� ������)�� ���͸��Ͽ� '���ۿ���'�� ����
        transmission_error_df = merged_df[merged_df['_merge'] != 'both']
        transmission_error_df = transmission_error_df[['�����ڸ�', '��¥', '���۽ð�', '����ð�']]
        # �ʿ��� ���� �����Ͽ� ����
        transmission_error_df.to_csv('error/���ۿ���.csv', index=False, encoding='utf-8-sig')
        
        print("���ۿ��� �Ϸ�")
        
        
        

    def wrong_tag(self):
        self.plan_df['���۽ð�_datetime'] = pd.to_datetime(self.plan_df['���۽ð�'], format='%H:%M')
        self.tags_df['�����±�_datetime'] = pd.to_datetime(self.tags_df['�����±�'], format='%H:%M:%S', errors='coerce')

        merged = self.merged_df()
        
        merged['�ð�����'] = (merged['���۽ð�_datetime'] - merged['�����±�_datetime']).abs().dt.total_seconds() / 60
        
        filtered_df = merged[merged['�ð�����'] > 30]

        wrongtag_df = filtered_df[['�����ڸ�', '��纸ȣ���', '��¥', '���۽ð�', '����ð�', '�����±�', '�����±�']]
        wrongtag_df.to_csv('error/��������.csv', index=False, encoding='utf-8-sig')
        print("�������� ���� �Ϸ�")
        
    def overtime(self):
        self.plan_df['���۽ð�_datetime'] = pd.to_datetime(self.plan_df['���۽ð�'], format='%H:%M')
        self.plan_df['����ð�_datetime'] = pd.to_datetime(self.plan_df['����ð�'], format='%H:%M')
        
        self.plan_df['�ѽð�_��ȹ'] = (self.plan_df['����ð�_datetime'] - self.plan_df['���۽ð�_datetime']).dt.total_seconds() / 60
        
        self.tags_df['�ѽð�_�±�'] = pd.to_numeric(self.tags_df['�ѽð�'])
        
        merged = self.merged_df()
        merged['�ð�����(��)'] = (merged['�ѽð�_�±�'] - merged['�ѽð�_��ȹ'])
        
        filtered_df = merged[merged['�ð�����(��)'] > 30]
        
        overtime_df = filtered_df[['�����ڸ�', '��纸ȣ���', '��¥', '���۽ð�', '����ð�', '�ѽð�_��ȹ', '�����±�', '�����±�','�ѽð�_�±�']]
        overtime_df.to_csv('error/�ð��ʰ�.csv', index=False, encoding='utf-8-sig')
        
        print("�ð��ʰ� �Ϸ�")
        
    def holiday(self):
        self.plan_df['������'] = self.plan_df['��¥'].apply(lambda x: is_holiday(x.strftime('%Y-%m-%d')) or x.weekday() == 6)
        holiday_df = self.plan_df[(self.plan_df['������']) & (self.plan_df['��������'] == 'N')]
        holiday_df = holiday_df[['�����ڸ�', '��纸ȣ���', '��¥', '���۽ð�', '����ð�', ]]
        holiday_df.to_csv('error/������.csv', index=False, encoding='utf-8-sig')
        print("������ ������ ���� �Ϸ�")
        
    def one_tag(self):
        filtered_tags = self.tags_df[self.tags_df['�����±�'] == '-']

        # ���͸��� �±� �����Ϳ� ��ȹ �����͸� ������ ������ȣ�� ��¥, ��纸ȣ����� �������� ��ġ��
        merged_df = pd.merge(filtered_tags, self.plan_df, on=['������ ������ȣ', '�����ڸ�', '����', '��纸ȣ���'], how='left')

        # ��� ������������ ����
        result_df = merged_df[['������ ������ȣ', '�����ڸ�', '����', '���۽ð�', '����ð�', '�����±�', '�����±�']]
        
        # CSV ���Ϸ� ����
        result_df.to_csv('error/����,�����±׿���.csv', index=False, encoding='utf-8-sig')
        print("����,�����±�")
        
        
        
        

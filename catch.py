# -*- coding: euc-kr -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')


import pandas as pd

plan_df = pd.read_csv("plan.csv", encoding='utf-8')
tags_df = pd.read_csv("tag.csv", encoding='utf-8')

plan_df['��¥'] = pd.to_datetime(plan_df['����']).dt.date
tags_df['��¥'] = pd.to_datetime(tags_df['����']).dt.date

def merged_df(plan, tag):
    merged_df = pd.merge(plan, tag, how='inner', 
                    left_on=['��¥', '�����ڸ�', '��纸ȣ���'], 
                    right_on=['��¥', '�����ڸ�', '��纸ȣ���']) 
    return merged_df  
    

def transmission_error(plan, tag):
    merged_df = pd.merge(plan, tag, how='outer', 
                    left_on=['��¥', '�����ڸ�'], 
                    right_on=['��¥', '�����ڸ�'],
                    indicator=True) 
    # ��ġ���� �ʴ� ������(���ʿ��� �����ϴ� ������)�� ���͸��Ͽ� '���ۿ���'�� ����
    transmission_error_df = merged_df[merged_df['_merge'] != 'both']
    transmission_error_df = transmission_error_df[['�����ڸ�', '��¥', '���۽ð�', '����ð�']]
    # �ʿ��� ���� �����Ͽ� ����
    transmission_error_df.to_csv('���ۿ���.csv', index=False, encoding='utf-8-sig')
    
    # ��ġ�ϴ� �����͸� ���͸��Ͽ� ��ȯ
    merged_df = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge'])
    
    
    

def wrong_tag():
    plan_df['���۽ð�_datetime'] = pd.to_datetime(plan_df['���۽ð�'], format='%H:%M')
    tags_df['�����±�_datetime'] = pd.to_datetime(tags_df['�����±�'], format='%H:%M:%S', errors='coerce')

    merged = merged_df(plan_df, tags_df)
    
    merged['�ð�����'] = (merged['���۽ð�_datetime'] - merged['�����±�_datetime']).abs().dt.total_seconds() / 60
    
    filtered_df = merged[merged['�ð�����'] > 30]

    wrongtag_df = filtered_df[['�����ڸ�', '��纸ȣ���', '��¥', '���۽ð�', '����ð�', '�����±�', '�����±�']]
    wrongtag_df.to_csv('��������.csv', index=False, encoding='utf-8-sig')
    print("�������� ���� �Ϸ�")
    
def overtime():
    plan_df['���۽ð�_datetime'] = pd.to_datetime(plan_df['���۽ð�'], format='%H:%M')
    plan_df['����ð�_datetime'] = pd.to_datetime(plan_df['����ð�'], format='%H:%M')
    
    plan_df['�ѽð�_��ȹ'] = (plan_df['����ð�_datetime'] - plan_df['���۽ð�_datetime']).dt.total_seconds() / 60
    
    tags_df['�ѽð�_�±�'] = pd.to_numeric(tags_df['�ѽð�'])
    
    merged = merged_df(plan_df, tags_df)
    merged['�ð�����(��)'] = (merged['�ѽð�_�±�'] - merged['�ѽð�_��ȹ'])
    
    filtered_df = merged[merged['�ð�����(��)'] > 30]
    
    overtime_df = filtered_df[['�����ڸ�', '��纸ȣ���', '��¥', '���۽ð�', '����ð�', '�ѽð�_��ȹ', '�����±�', '�����±�','�ѽð�_�±�']]
    overtime_df.to_csv('�ð��ʰ�.csv', index=False, encoding='utf-8-sig')
    
    

transmission_error(plan_df, tags_df)
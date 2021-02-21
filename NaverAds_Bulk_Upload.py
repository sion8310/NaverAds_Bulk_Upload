#!/usr/bin/env python
# coding: utf-8

import time
import random
import requests
import urllib.parse
import json
import base64
import hmac
import hashlib
import jsonpickle
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 500)

print("네이버 키워드 대량 등록 프로그램")

# Naver Signature
class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)

        hash.hexdigest()
        return base64.b64encode(hash.digest())


BASE_URL = 'https://api.naver.com'
API_KEY = '<API_KEY>'
SECRET_KEY = '<SECRET_KEY>'
CUSTOMER_ID = '<CUSTOMER_ID>'


def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(round(time.time() * 1000))
    signature = Signature.generate(timestamp, method, uri, secret_key)
    return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp, 'X-API-KEY': api_key,
            'X-Customer': str(CUSTOMER_ID), 'X-Signature': signature}


print("비즈채널 정보를 수집하겠습니다.")
# Business Channel Id
method = 'GET'
uri = '/ncc/channels'
r = requests.get(BASE_URL + uri,
                 headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
data = r.json()
Channel_Data = pd.read_json(json.dumps(data))
#데이터 자르고
Channel_Data2 = Channel_Data[['nccBusinessChannelId', 'name', 'channelKey']]
print("비즈채널 정보를 수집 완료했습니다.")

#####################################################################################################
# 캠페인 정보 빼내고
print("캠페인 정보를 수집하고 있습니다.")

uri = '/ncc/campaigns'
method = 'GET'
r = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
data = r.json()

campaign_result = pd.read_json(json.dumps(data))
try:
    campaign_result = campaign_result[['nccCampaignId', 'name']]
    campaign_result.columns = ['nccCampaignId', 'Campaign_Name']
except:
    campaign_result = pd.DataFrame(columns = ['nccCampaignId', 'Campaign_Name'])

#####################################################################################################
# 광고그룹 정보 빼내고
print("광고그룹 정보를 수집하고 있습니다.")

uri = '/ncc/adgroups'
method = 'GET'
r = requests.get(BASE_URL + uri, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
data = r.json()

adgroup_result = pd.read_json(json.dumps(data))

try:
    adgroup_result = adgroup_result[['nccAdgroupId', 'name', 'nccCampaignId']]
    adgroup_result.columns = ['nccAdgroupId', 'AdGroup_Name', 'nccCampaignId']
except:
    adgroup_result = pd.DataFrame(columns=['nccAdgroupId', 'AdGroup_Name', 'nccCampaignId'])


file_name = str(input("확장자명을 제외한 엑셀 파일 이름을 쳐주세요.\n"))
file_name = file_name + ".xlsx"

flag = True
while(flag):
    try: 
        exl_data = pd.read_excel(file_name)
        print("데이터를 성공적으로 불러왔습니다.")
        flag = False
    except:
        print("파일 이름이 잘못됬습니다. 다시 한번 입력해주세요.")
        file_name = str(input("확장자명을 제외한 엑셀 파일 이름을 쳐주세요.\n"))
        file_name = file_name + ".xlsx"

    


exl_data = pd.read_excel(file_name)
print("데이터를 성공적으로 불러왔습니다.")



print("업로드 하기전에 확인 작업을 거치겠습니다.")

print("캠페인 / 광고그룹 / 키워드 길이 조건 확인중..")
print("\n\n")

campaign_check = exl_data.loc[(exl_data['캠페인'].str.len() > 30) | (exl_data['캠페인'].str.len() < 1),]
print("아래의 캠페인이 조건이 맞지 않아 삭제하겠습니다.")
print(campaign_check['캠페인'].unique())


adgroup_check = exl_data.loc[(exl_data['광고그룹'].str.len() > 30) | (exl_data['광고그룹'].str.len() < 1),]
print("아래의 광고그룹 조건이 맞지 않아 삭제하겠습니다.")
print(campaign_check['광고그룹'].unique())

keyword_check = exl_data.loc[(exl_data['키워드'].str.len() > 25) | (exl_data['키워드'].str.len() < 1),]
print("아래의 키워드 조건이 맞지 않아 삭제하겠습니다.")
print(campaign_check['키워드'].unique())




exl_data = exl_data.loc[(exl_data['캠페인'].str.len() < 30) & (exl_data['캠페인'].str.len() > 1),]
exl_data = exl_data.loc[(exl_data['광고그룹'].str.len() < 30) & (exl_data['광고그룹'].str.len() > 1),]
exl_data = exl_data.loc[(exl_data['키워드'].str.len() < 25) & (exl_data['키워드'].str.len() > 1),]

print("\n\n\n")


print("\n최종 캠페인 리스트:")
campaign_name = exl_data['캠페인'].unique()
print(campaign_name)

print("\n최종 광고그룹 리스트:")
print(exl_data['광고그룹'].unique())

print("\n최종 비즈채널 리스트:")
print(exl_data['비즈채널'].unique())

continue_decision = str(input("\n\n 위의 내용이 맞습니까? 맞다면 (1)번을 틀리면 (2)번을 눌러주세요.\n"))

if continue_decision == '2':
    exit(1)

# 캠페인 생성하기.
method0 = 'POST'
uri0 = '/ncc/campaigns'
# 광고그룹 생성하기.
uri = '/ncc/adgroups'
method = 'POST'
# 키워드 생성하기.
uri2 = '/ncc/keywords'
method2 = 'POST'

print("지금부터 대량 업로드를 시작하겠습니다.")
for campaign in campaign_name:
    if campaign in campaign_result["Campaign_Name"].values.tolist():
        print("{}은 이미 존재하는 Campaign입니다.".format(campaign))
        # 캠페인 ID를 가져와야해
        created_campaign = campaign_result.loc[campaign_result['Campaign_Name'] == campaign, 'nccCampaignId'].unique()
        subset_campaign = exl_data.loc[exl_data['캠페인'] == campaign,]
        ad_group_name = subset_campaign['광고그룹'].unique()
        for adgroup_name in ad_group_name:
            keyword_list_in_json = []
            # 잘라낸 데이터의 광고 그룹에 해당되는 키워드들을 뽑아낸다.
            keyword_df = subset_campaign.loc[subset_campaign['광고그룹'] == adgroup_name, '키워드']
            keyword_list = keyword_df.values.tolist()
            # 키워드 리스트화 완성.
            for keyword in keyword_list:
                keyword_list_in_json.append({'keyword': keyword, 'bidAmt': 100, 'useGroupBidAmt': False})

            business_Channel = subset_campaign.loc[subset_campaign['광고그룹'] == adgroup_name, '비즈채널']
            business_Channel_name = business_Channel.unique()
            # ID랑 매칭
            for channel_name in business_Channel_name:
                business_Channel_ID_df = Channel_Data2.loc[
                    Channel_Data2['name'] == channel_name, 'nccBusinessChannelId']
                business_Channel_ID = business_Channel_ID_df.unique()
                created_group = adgroup_result.loc[(adgroup_result['nccCampaignId'] == created_campaign[0]) & (
                            adgroup_result['AdGroup_Name'] == adgroup_name), 'nccAdgroupId'].unique()

                if created_group != "":
                    print("{}은 이미 존재하는 AdGroup입니다.".format(adgroup_name))
                    r = requests.post(BASE_URL + uri2, params={'nccAdgroupId': created_group[0]},
                                      json=keyword_list_in_json,
                                      headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))
                else:
                    print("{}은 새로운 AdGroup입니다.".format(adgroup_name))
                    r = requests.post(BASE_URL + uri, json={'name': adgroup_name, 'nccCampaignId': created_campaign[0],
                                                            'pcChannelId': business_Channel_ID[0],
                                                            'mobileChannelId': business_Channel_ID[0]},
                                      headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
                    data = r.json()
                    created_group = data["nccAdgroupId"]
                    r = requests.post(BASE_URL + uri2, params={'nccAdgroupId': created_group},
                                      json=keyword_list_in_json,
                                      headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))

    else:
        print("{}은 새로운 Campaign입니다.".format(campaign))
        r = requests.post(BASE_URL + uri0, json={'campaignTp': 'WEB_SITE', 'name': campaign,
                                                 'customerId': CUSTOMER_ID},
                          headers=get_header(method0, uri0, API_KEY, SECRET_KEY, CUSTOMER_ID))
        data = r.json()
        created_campaign = data["nccCampaignId"]

        # 데아터를 나눠버린다.
        subset_campaign = exl_data.loc[exl_data['캠페인'] == campaign,]
        # 해당 되는 subsetted 데이터의 광고그룹을 가져온다.
        ad_group_name = subset_campaign['광고그룹'].unique()

        for adgroup_name in ad_group_name:
            keyword_list_in_json = []
            # 잘라낸 데이터의 광고 그룹에 해당되는 키워드들을 뽑아낸다.
            keyword_df = subset_campaign.loc[subset_campaign['광고그룹'] == adgroup_name, '키워드']
            keyword_list = keyword_df.values.tolist()
            # 키워드 리스트화 완성.
            for keyword in keyword_list:
                keyword_list_in_json.append({'keyword': keyword, 'bidAmt': 100, 'useGroupBidAmt': False})

            business_Channel = subset_campaign.loc[subset_campaign['광고그룹'] == adgroup_name, '비즈채널']
            business_Channel_name = business_Channel.unique()

            # ID랑 매칭
            for channel_name in business_Channel_name:
                business_Channel_ID_df = Channel_Data2.loc[
                    Channel_Data2['name'] == channel_name, 'nccBusinessChannelId']
                business_Channel_ID = business_Channel_ID_df.unique()

                r = requests.post(BASE_URL + uri, json={'name': adgroup_name, 'nccCampaignId': created_campaign,
                                                        'pcChannelId': business_Channel_ID[0],
                                                        'mobileChannelId': business_Channel_ID[0]},
                                  headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

                data = r.json()
                created_group = data["nccAdgroupId"]

                r = requests.post(BASE_URL + uri2, params={'nccAdgroupId': created_group},
                                  json=keyword_list_in_json,
                                  headers=get_header(method2, uri2, API_KEY, SECRET_KEY, CUSTOMER_ID))


print('대량 업로드 성공적으로 마무리 했습니다.')
time.sleep(1)
print('3초 후에 프로그램이 종료됩니다.')
time.sleep(3)

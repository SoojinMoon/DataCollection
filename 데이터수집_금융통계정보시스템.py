# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 10:33:10 2022

@author: 20W00212
"""

import pandas as pd
import os 
import requests  
import time 

# 회사 코드 확인 
url = 'http://fisis.fss.or.kr/openapi/companySearch.json' 
result_params = {
                    'auth' :'c3ad6babee2fb101f8c080cb611df690', 
                    'lang' : 'kr' ,
                    'partDiv' : 'T'} # 할부금융 T, 리스 K,  신기술 N
req = requests.get(url, params=result_params).json()
company_df = pd.DataFrame(req['result']['list'])

# company dictionary 
company_dict = {
        'K' : ['0012452', '0010250', '0010235', '0010238', '0011375', '0012303'], # BNK, KB, 산은, 신한, 엠(효성), DGB
        'T' : ['0010267', '0011797', '0010259', '0013202', '0010255', '0010268', '0010271'], # JB, NH, 롯데, 메리츠, 우리금융, 하나, 현대
        'N' : ['0010741'] # IBK
}


# 수집 계정(통계 API) 확인 
url = 'http://fisis.fss.or.kr/openapi/statisticsListSearch.json' 
def get_list_f(lrgDiv) : 
    result_params = {
                        'auth' :'c3ad6babee2fb101f8c080cb611df690', 
                        'lang' : 'kr' ,
                        'lrgDiv' : lrgDiv
                     }
    req = requests.get(url, params=result_params).json()
    res_df = pd.DataFrame(req['result']['list'])
    return res_df
feature_df = pd.concat([get_list_f('K'), get_list_f('T'), get_list_f('N')], axis = 0)


# 계정 항목 ID 확인 
url = 'http://fisis.fss.or.kr/openapi/accountListSearch.json' 
account_df = pd.DataFrame()

for feature_code in feature_df['list_no'] :
        result_params = {
                            'auth' :'c3ad6babee2fb101f8c080cb611df690', 
                            'lang' : 'kr' ,
                            'listNo' : feature_code, 
                            'lrgDiv' : 'K',                     
        }
        req = requests.get(url, params=result_params).json()
        account_df = pd.concat([account_df, pd.DataFrame(req['result']['list'])], axis = 0)
        

feature_dvcd = ['S{}007', 'S{}008', 'S{}009', 'S{}010', 'S{}117', 'S{}103', 'S{}104', 'S{}118']
res_df = pd.DataFrame()         # 주요 경영 지표 dataframe (feature_df 참고)
res_df_v1 = pd.DataFrame()      # 금액 전용 dataframe (feature_df 참고)
res_df_v2 = pd.DataFrame()      # 누계 전용 dataframe (feature_df 참고)

strt_base_ym = '201801'
end_base_ym = '202109'


for bz_key in company_dict.keys() : # 업종(리스, 할부금융, 신기술)별
    
    for company_code in company_dict[bz_key] : # 회사별  
    
        for feature_code in feature_dvcd : # 계정별
            print(bz_key, company_code, feature_code)
            # company_code = '0012452'; feature_code = 'ST103'
            # base_ym = '202106'; company_code = '0010741'; feature_code = 'S{}007'
            feature_info = feature_df[feature_df['list_no'] == feature_code.format(bz_key)].values
            if feature_code in ['S{}103', 'S{}104', 'S{}118'] : # 요약재무상태표(자산), 요약재무상태표(부채 및 자본), 요약손익계산서
                result_params = {
                                'lang' : 'kr' ,
                                'auth' :'c3ad6babee2fb101f8c080cb611df690', 
                                'financeCd' : company_code, 
                                'listNo' : feature_code.format(bz_key),   
                                'term' : 'Q',
                                'startBaseMm' : strt_base_ym, 
                                'endBaseMm' : end_base_ym
                                # 'accountCd' : 'B3',                                      
                }
                time.sleep(0.1) 
                req = requests.get('http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json', params=result_params).json()
                tmp_df = pd.DataFrame(req['result']['list'])
                        
                # 금액 dataframe
                tmp_df_v1 =  pd.DataFrame(columns = ['base_month', 'finance_cd', 'finance_nm', 'account_cd', 'account_nm', 'val'
                                                      , 'lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm', 'total_yn'])   
                tmp_df_v1[['base_month', 'finance_cd', 'finance_nm','account_cd', 'val']] = tmp_df[['base_month', 'finance_cd', 'finance_nm', 'account_cd', 'a']].copy()
                tmp_df_v1['account_nm'] = tmp_df['account_nm'] + ':금액'
                tmp_df_v1[['lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm']] = feature_info
                tmp_df_v1['total_yn'] = 'N'
                res_df_v1 = pd.concat([res_df_v1, tmp_df_v1], axis = 0)
                 
            
                # 누계 dataframe
                tmp_df_v2 =  pd.DataFrame(columns = ['base_month', 'finance_cd', 'finance_nm', 'account_cd', 'account_nm', 'val'
                                                      , 'lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm', 'total_yn'])   
                tmp_df_v2[['base_month', 'finance_cd', 'finance_nm','account_cd', 'val']] = tmp_df[['base_month', 'finance_cd', 'finance_nm', 'account_cd', 'b']].copy()
                tmp_df_v2['account_nm'] = tmp_df['account_nm'] + ':누계'
                tmp_df_v2[['lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm']] = feature_info
                tmp_df_v2['total_yn'] = 'Y'
                res_df_v2 = pd.concat([res_df_v2, tmp_df_v2], axis = 0)

            if feature_code in ['S{}007', 'S{}008', 'S{}009', 'S{}010', 'S{}117'] :# 주요경영지표 * 부문별 영업실적, 보도자료통계 (ST119, ST124, ST125, ST126, ST127)는 당사에서 집계하지 않는 값이므로 제외
                result_params = {
                                   'lang' : 'kr' ,
                                   'auth' :'c3ad6babee2fb101f8c080cb611df690', 
                                   'financeCd' : company_code, 
                                   'listNo' :  feature_code.format(bz_key),   
                                   'term' : 'Q',
                                   'startBaseMm' : strt_base_ym, 
                                    'endBaseMm' : end_base_ym
                }
                time.sleep(0.1)
                req = requests.get('http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json', params=result_params).json()
                tmp_df = pd.DataFrame(req['result']['list'])
                tmp_df = tmp_df.reindex(columns = tmp_df.columns.tolist() + ['lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm'])
                tmp_df[['lrg_div_nm', 'sml_div_nm', 'list_no', 'list_nm']] = feature_info.tolist()[0]
                tmp_df['total_yn'] = 'N'
                res_df = pd.concat([res_df, tmp_df], axis = 0) 

res_df.rename(columns = {'a':'val'}, inplace =True)
res_df_tot = pd.concat([res_df, res_df_v1, res_df_v2], axis = 0)
final_df = res_df_tot[['base_month', 'lrg_div_nm', 'sml_div_nm', 'list_nm', 'account_cd', 'account_nm', 'total_yn', 'finance_cd', 'finance_nm', 'val']]

import sqlite3
con = sqlite3.connect('D:/금감원/db.db')
final_df.to_sql('finaldat', con, if_exists = 'append', index = False)
final_df.to_excel('D:/금감원/finaldata.xlsx', index = False, header = True)




            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
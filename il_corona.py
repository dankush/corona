# -*- coding: utf-8 -*-
'''
Python 3.8
Author : Dan Kushner <dan.kushner@sisense.com>
IL COVID-19
'''
import time
import requests
import pandas as pd


BASE_URL='https://data.gov.il/api/3/action/datastore_search'

def get_all_data(resource_name, resource_id):
    print('Getting {0}'.format(resource_name))
    headers = {}
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    offset=0
    while True:
        # df = None
        url = r'{0}?resource_id={1}&offset={2}&limit=100'.format(BASE_URL, resource_id, str(offset))
        print(url)
        try:
            response = requests.get(url, headers=headers)
            lab_data = response.json()['result']['records']
        except Exception as ex:
            print(response)
            print('Error {0}'.format(ex))
            continue
        data_limit = len(lab_data)
        print('offset-> {0} data -> {1}'.format(offset, data_limit))
        if data_limit==0:
            break
        df = pd.DataFrame(lab_data)
        # print(df.head()) #for debug
        if offset==0:
            main_df = df
        else:
            main_df = main_df.append(df, ignore_index=True)
        offset = offset + data_limit
    count_row = main_df.shape[0]  # gives number of row count
    count_col = main_df.shape[1]  # gives number of col count
    print('num rows: {0} col: {1}'.format(count_row, count_col))
    print(main_df.head())
    main_df.to_csv(r'/Users/dankushner/Documents/data/COVID-19/{0}.csv'.format(resource_name), index=False, header=True, encoding = 'utf-8-sig')
    return main_df



if __name__ == '__main__':
    START = time.time()
    get_all_data('נתוני קורונה מאושפזים', 'e4bf0ab8-ec88-4f9b-8669-f2cc78273edd') # working
    get_all_data('נתוני קורונה קבוצות מין וגיל', '89f61e3a-4866-4bbf-bcc1-9734e5fee58e') # working
    get_all_data('נתוני קורונה נפטרים', 'a2b2fceb-3334-44eb-b7b5-9327a573ea2c') # working
    get_all_data('טבלת האוכלוסייה הצעירה', '767ffb4e-a473-490d-be80-faac0d83cae7') # working
    get_all_data('מאפייני נבדקים - טבלת עזר', '09e66a69-ad5b-4c46-a5d9-1d1479b1f338') # Check how many more than 60 age and above
    get_all_data('נתוני קורונה איזורים סטטיסטיים', 'd07c0771-01a8-43b2-96cc-c6154e7fa9bd')# working
    # get_all_data('נתוני קורונה מאפייני נבדקים - אוגוסט-ספטמבר 2020', '74216e15-f740-4709-adb7-a6fb0955a048') # 1M

    print('\n' + 'The script took: {} seconds.'.format(str(time.time() - START)))
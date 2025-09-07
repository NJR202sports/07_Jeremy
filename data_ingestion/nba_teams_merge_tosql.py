import pandas as pd
import numpy as np
import os
from data_ingestion.mysql import  upload_data_to_mysql_upsert, nba_teams_merge_table

def nba_teams_merge():
    # 1. 設定檔案路徑和年份範圍
    data_folder = 'nba_teams_merge'  # 將 'your_data_folder' 替換為你的資料夾路徑
    
    # 2. 建立一個空的列表來儲存每年的合併結果
    merged_dfs = []

    # 3. 逐年處理並合併資料
    for year in years:
        try:
            # 根據年份建立檔案路徑
            salary_file = os.path.join(data_folder, f'nba_teams_salary_{year}.csv')
            state_file = os.path.join(data_folder, f'nba_teams_state_{year}.csv')
            advanced_state_file = os.path.join(data_folder, f'nba_teams_advancedstate_{year}.csv')

            # 讀取當年份的檔案
            salary_df = pd.read_csv(salary_file)
            state_df = pd.read_csv(state_file)
            advanced_state_df = pd.read_csv(advanced_state_file)
            
            # 新增 'year' 欄位作為複合主鍵的一部分
            salary_df['year'] = year
            state_df['year'] = year
            advanced_state_df['year'] = year

            # 合併三個 DataFrame，使用複合主鍵 ['year', 'team']
            # 先合併薪資和基本數據
            merged_yearly_df = pd.merge(salary_df, state_df, on=['year', 'team'], how='inner')
            
            # 再將結果與進階數據合併
            merged_yearly_df = pd.merge(merged_yearly_df, advanced_state_df, on=['year', 'team'], how='inner')
            
            # 將合併後的 DataFrame 加入列表
            merged_dfs.append(merged_yearly_df)
            print(f"成功合併 {year} 年的資料。")

        except FileNotFoundError:
            print(f"錯誤：找不到 {year} 年的檔案，跳過。")
        except Exception as e:
            print(f"處理 {year} 年的檔案時發生錯誤：{e}")

    # 4. 將所有年份的合併結果整合成一個最終的 DataFrame
    if merged_dfs:
        final_combined_df = pd.concat(merged_dfs, ignore_index=True)
        final_combined_df = final_combined_df.replace({pd.NaT: None, np.nan: None}) #把NaN轉換回None

        # 5. 顯示和儲存最終的 DataFrame
        print("\n--- 所有檔案已成功合併！---")
        # print("最終 DataFrame 的前五行：")
        # print(final_combined_df.head())

        # 將最終的合併檔案儲存為新的 CSV
        # output_path = 'nba_teams_merge.csv'
        # fn = os.path.join(data_folder, f"{output_path}")
        # final_combined_df.to_csv(fn, index=False)
        # print(f"\n最終的合併檔案已儲存至：{fn}")

        # 上傳SQL
        data = final_combined_df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
        upload_data_to_mysql_upsert(table_obj=nba_teams_merge_table, data=data)
        print(f"nba_teams_merge_table has been uploaded to mysql.")

    else:
        print("\n沒有任何檔案被成功合併。請檢查檔案路徑和名稱是否正確。")

    
    

if __name__ == '__main__':

    years = list(range(2015,2026))

    nba_teams_merge()
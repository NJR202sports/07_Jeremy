import pandas as pd
import numpy as np
import os
from data_ingestion.mysql import  upload_data_to_mysql_upsert, nba_players_merge_table


def nba_players_merge():
    # 1. 設定檔案路徑和年份範圍
    data_folder = 'nba_players_merge'  # 將 'your_data_folder' 替換為你的資料夾路徑
    
    # 2. 建立一個空的列表來儲存每年的合併結果
    merged_dfs = []

    # 3. 逐年處理並合併資料
    for year in years:
        try:
            # 建立檔案路徑
            salary_file = os.path.join(data_folder, f'nba_players_salary_{year}.csv')
            state_file = os.path.join(data_folder, f'nba_players_state_{year}.csv')

            # 讀取當年份的 salary 和 state 檔案
            salary_df = pd.read_csv(salary_file)
            state_df = pd.read_csv(state_file)

            # 確保 'year' 欄位存在且數值正確
            salary_df['year'] = year
            state_df['year'] = year

            # 使用 pd.merge() 根據複合主鍵 ['player', 'year'] 進行合併
            # 這裡使用 'outer' 連結，只保留在兩個檔案中所有出現的 player-year 組合
            merged_yearly_df = pd.merge(salary_df, state_df, on=['year', 'player', 'team' ], how='outer')

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
        final_combined_df = final_combined_df.replace({np.nan: None}) #把NaN轉換回None
        
        # 5. 顯示合併後的 DataFrame 資訊
        print("\n所有檔案合併完成！")
        # print("最終 DataFrame 的前五行：")
        # print(final_combined_df.head())
        
        # (可選) 儲存最終的 DataFrame 到新的 CSV 檔案
        # output_path = 'nba_players_merge.csv'
        # fn = os.path.join(data_folder, f"{output_path}")
        # final_combined_df.to_csv(fn, index=False)
        # print(f"\n最終的合併檔案已儲存至：{fn}")
        # print(f"總共處理 {len(final_combined_df)} 筆記錄。")
        
        # 上傳SQL
        data = final_combined_df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
        upload_data_to_mysql_upsert(table_obj=nba_players_merge_table, data=data)
        print(f"nba_players_merge_table has been uploaded to mysql.")

    else:
        print("\n沒有任何檔案被成功合併。請檢查檔案路徑和名稱是否正確。")

if __name__ == '__main__':

    years = list(range(2015,2026))

    nba_players_merge()
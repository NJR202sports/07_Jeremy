import pandas as pd
import numpy as np
from glob import glob
import os

# with open('data_ingestion/nba_players_state_{year}.csv', newline='', encoding='utf-8-sig') as f:
#     reader = csv.reader(f)
#     columns = next(reader)  # 讀取第一行作為欄位名稱
# dirname = "nba_players_state"
# if not os.path.exists(dirname):
#         os.mkdir(dirname)

# fn = os.path.join(dirname, f"nba_players_state_{year}.csv")
# df.read_csv(fn, encoding="utf-8-sig", index=False)

 
# files = glob(r'./nba_players_table/*.csv')  #取得所有CSV檔案路徑
# df_list = [pd.read_csv(file) for file in files]  #串列中包含兩個Pandas DataFrame
# print(df_list[0].columns)
# print(df_list[1].columns) 
#result = pd.merge(df_list[0], df_list[1], on='year'and'name')  #合併兩個DataFrame，並指定合併的欄位


# all_data = []  #用來存放所有檔案的資料  
# for file in files:
#     df = pd.read_csv(file, encoding='utf-8-sig')  #讀取每個CSV檔案
#     all_data.append(df)  #將資料加入列表

# players = pd.concat(all_data, ignore_index=False)  #合併所有資料，並重設索引
# players.to_csv('data_ingestion/nba_players_table/nba_players_table_2015_2025.csv', encoding='utf-8-sig', index=False)  #將合

# path_domestic = os.path.abspath(os.getcwd()) + '/non_domestic'
# non_domestic = glob.glob(os.path.join(path_domestic, '*.csv'))
# df_domestic = pd.concat((pd.read_csv(f) for f in non_domestic))

import pandas as pd
import os

# 1. 設定檔案路徑和年份範圍
data_folder = 'nba_players_table'  # 將 'your_data_folder' 替換為你的資料夾路徑
start_year = 2015
end_year = 2025

# 2. 建立一個空的列表來儲存每年的合併結果
merged_dfs = []

# 3. 逐年處理並合併資料
for year in range(start_year, end_year + 1):
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

        # 使用 pd.merge() 根據複合主鍵 ['name', 'year'] 進行合併
        # 這裡使用 'inner' 連結，只保留在兩個檔案中都存在的球員
        merged_yearly_df = pd.merge(salary_df, state_df, on=['year', 'player', 'team' ], how='inner')

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
    
    # 5. 顯示合併後的 DataFrame 資訊
    print("\n所有檔案合併完成！")
    print("最終 DataFrame 的前五行：")
    print(final_combined_df.head())
    
    # (可選) 儲存最終的 DataFrame 到新的 CSV 檔案
    output_path = 'nba_players_table_2015_2025.csv'
    fn = os.path.join(data_folder, f"{output_path}")
    final_combined_df.to_csv(fn, index=False)
    print(f"\n最終的合併檔案已儲存至：{fn}")

else:
    print("\n沒有任何檔案被成功合併。請檢查檔案路徑和名稱是否正確。")
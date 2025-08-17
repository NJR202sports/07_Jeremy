#儲存2015-2025球隊薪資表格

import requests
import pandas as pd
import os

# dirname = "nba_teams_salary"
# if not os.path.exists(dirname):
#     os.mkdir(dirname)

for year in range(2015, 2026):
    url = f"https://www.hoopshype.com/salaries/teams/?season={year}"
    response = requests.get(url)


    try:

        tables = pd.read_html(response.text)
        df = tables[0]
        #fn = os.path.join(dirname)
        df.to_csv(f"salary_{year}.csv", index=False)

    except Exception as e:
        print(f"{year} 資料讀取失敗: {e}")

#合併2015-2025球隊薪資表格成一份

years = range(2015, 2026)
dfs = []

for year in years:
    df = pd.read_csv(f"salary_{year}.csv")
    if "Team" in df.columns:
        df["Team"] = df["Team"].str.strip().str.lower()
    dfs.append(df)

# 合併所有年度資料，欄位不同 pandas 會自動補缺值NaN
all_years = pd.concat(dfs, axis=0, sort=True)

if 'Unnamed: 0' in all_years.columns:
    all_years = all_years.drop(columns=['Unnamed: 0'])

agg_dict = {}


for col in all_years.columns:
    if col == "Team":
        agg_dict[col] = "first"
    elif col == "year":
        agg_dict[col] = lambda x: ", ".join(map(str, sorted(x.unique())))
    else:
        agg_dict[col] = lambda x: ", ".join(x.dropna().astype(str).unique())

dirname = "nba_teams_salary"
if not os.path.exists(dirname):
        os.mkdir(dirname)

# 使用 groupby 聚合
grouped = all_years.groupby("Team").agg(agg_dict).reset_index(drop=True)

grouped.index = grouped.index + 1

# team 欄移至第一欄
team_col = grouped.pop("Team")
grouped.insert(0, "Team", team_col)
fn = os.path.join(dirname, 'nba_teams_salary.csv')
grouped.to_csv(fn, index=True)
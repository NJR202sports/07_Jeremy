#儲存2015-2025球隊薪資表格

import requests
import pandas as pd
import os
import io
from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert, nba_teams_salary_table

def nba_teams_salary(year: int):
    # dirname = "nba_teams_merge"
    # if not os.path.exists(dirname):
    #     os.mkdir(dirname)

    url = f'https://www.hoopshype.com/salaries/teams/?season={year}'
    response = requests.get(url)
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]
    df.rename(columns={df.columns[0]: 'year'}, inplace=True)
    df.rename(columns={df.columns[1]: 'team'}, inplace=True)
    df.rename(columns={df.columns[2]: 'total_salary'}, inplace=True)
    df['year'] = year 
    df['total_salary'] = df['total_salary'].str.replace('$', '', regex=False).str.replace(',', '', regex=False)
    if year == 2025:
        df.drop(df.columns[3:6], axis=1, inplace=True)
    df['team_cut'] = df["team"].str.split(' ')
    df['team'] = df['team_cut'].str[-1]
    df.drop(columns=['team_cut'], inplace=True)
    # fn = os.path.join(dirname, f"team_salary_{year}.csv")
    # df.to_csv(fn, index=False)

    data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    upload_data_to_mysql_upsert(table_obj=nba_teams_salary_table, data=data)
    print(f"nba_teams_salary_{year} has been uploaded to mysql.")

    return df

if __name__ == '__main__':

    years = list(range(2015,2026))

    for year in years:

        nba_teams_salary(year)

# print(nba_teams_salary(2001))
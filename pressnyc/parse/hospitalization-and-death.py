#/usr/bin/python3

import re
import datetime
import io
import pandas as pd
from git import Repo



repo = Repo('.')


path = "totals/by-group.csv"

output_array = []


dateMatcher = re.compile('[0-9]+/[0-9]+')


for commit in list(repo.iter_commits('master', paths=path)):

  dateMatch = dateMatcher.search(commit.message)
  
  date = ''
  if dateMatch:
    date = dateMatch[0] + '/' + str(datetime.datetime.fromtimestamp(commit.committed_date).year)

  page_content = (commit.tree / path).data_stream.read()
  
  df = pd.read_csv(io.StringIO(page_content.decode('utf-8')))


  death = df.query('`subgroup` == "0-17"')
  h_0_4 = df.query('`subgroup` == "0-4"')
  h_5_12 = df.query('`subgroup` == "5-12"')
  h_13_17 = df.query('`subgroup` == "13-17"')


  output_array.append({
    'Date': date,
    'Hospitalization, 0-4': int( h_0_4['HOSPITALIZED_COUNT'].values[0] ),
    'Hospitalization, 5-12': int( h_5_12['HOSPITALIZED_COUNT'].values[0] ),
    'Hospitalization, 13-17': int( h_13_17['HOSPITALIZED_COUNT'].values[0] ),
    'Hospitalization, 0-17': 
      int( h_0_4['HOSPITALIZED_COUNT'].values[0] ) +
      int( h_5_12['HOSPITALIZED_COUNT'].values[0] ) +
      int( h_13_17['HOSPITALIZED_COUNT'].values[0] ),
    'Death Count, 0-17': int( death['DEATH_COUNT'].values[0] ) 
    })

pd.DataFrame(output_array).to_csv('pressnyc/csv/hospitalization-and-death.csv', index=False)

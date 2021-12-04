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


previous_death = 0
previous_h_0_4 = 0
previous_h_5_12 = 0
previous_h_13_17 = 0
previous_h_0_17 = 0
previous_date = 0



for commit in list(repo.iter_commits('master', paths=path)):
  
  date = str(datetime.datetime.fromtimestamp(commit.committed_date).month)\
     + '/' + str(datetime.datetime.fromtimestamp(commit.committed_date).day)\
     + '/' + str(datetime.datetime.fromtimestamp(commit.committed_date).year)
  
  if date == '12/2/2021': continue
  
  page_content = (commit.tree / path).data_stream.read()
  
  df = pd.read_csv(io.StringIO(page_content.decode('utf-8')))


  death   = df.query('`subgroup` == "0-17"')
  h_0_4   = df.query('`subgroup` == "0-4"')
  h_5_12  = df.query('`subgroup` == "5-12"')
  h_13_17 = df.query('`subgroup` == "13-17"')

  
  this_death   = int( death['DEATH_COUNT'].values[0] )
  this_h_0_4   = int( h_0_4['HOSPITALIZED_COUNT'].values[0] )
  this_h_5_12  = int( h_5_12['HOSPITALIZED_COUNT'].values[0] )
  this_h_13_17 = int( h_13_17['HOSPITALIZED_COUNT'].values[0] )
  this_h_0_17  = this_h_0_4 + this_h_5_12 + this_h_13_17


  diff_h_0_4   = previous_h_0_4 - this_h_0_4
  diff_h_5_12  = previous_h_5_12 - this_h_5_12
  diff_h_13_17 = previous_h_13_17 - this_h_13_17
  diff_h_0_17  = previous_h_0_17 - this_h_0_17
  diff_death   = previous_death - this_death

  if diff_h_0_4 < 0:   diff_h_0_4 = 0
  if diff_h_5_12 < 0:  diff_h_5_12 = 0
  if diff_h_13_17 < 0: diff_h_13_17 = 0
  if diff_h_0_17 < 0:  diff_h_0_17 = 0
  if diff_death < 0:   diff_death = 0

  if previous_date != 0:
    output_array.append({
      'Date': previous_date,
      'Hospitalization, 0-4': diff_h_0_4,
      'Hospitalization, 5-12': diff_h_5_12,
      'Hospitalization, 13-17': diff_h_13_17,
      'Hospitalization, 0-17': diff_h_0_17,
      'Death Count, 0-17': diff_death,
      })

  previous_date = date
  previous_death = this_death
  previous_h_0_4 = this_h_0_4
  previous_h_5_12 = this_h_5_12
  previous_h_13_17 = this_h_13_17
  previous_h_0_17 = this_h_0_17

pd.DataFrame(output_array).to_csv('pressnyc/csv/hospitalization-and-death-daily.csv', index=False)

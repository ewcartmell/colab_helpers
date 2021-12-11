import pandas as pd

def numerify(x):
  if(type(x) == str):
    x = x.replace(',','')
    x = x.replace('$','')
    x = x.replace('%','')
    x = x.replace(' ','')
    x = x.strip()
    x = pd.to_numeric(x, errors = 'coerce')
    x = x.round(2)
  return x


def calc_monthly_productivity(yearly_prod):
  yearly_prod = numerify(yearly_prod)
  return (yearly_prod ** (1/11))


def friendly_columns(columns):
  return columns.str.replace(' ','_').str.lower()


def gsheet_to_df(df, transpose = False):
  if(transpose):
    df = df.transpose()
  df.columns = df.iloc[0,:]
  df.columns = friendly_columns(df.columns)
  df = df.iloc[1:,:]
  df.reset_index(inplace=True)
  return df


def blended_rate(output_df, rates_df, site_keyword = 'site', rate_keyword = 'monthly_rate'):
  i = 0
  sites = list(rates_df[site_keyword])
  for index, row in output_df.iterrows():
    blended_rate = 0
    for i in range(len(sites)):
      blended_rate += rates_df.loc[rates_df[site_keyword] == sites[i], rate_keyword].item() * (row[sites[i]] / 100)
      i += 1
    output_df.loc[index,'blended_rate'] = blended_rate
  return output_df


def update_transactions_per_hc(df, grouping_column = 'team', date_column = 'month', cutoff_date = '2022-01-01'):
  temp_df = df
  temp_df.sort_values([grouping_column, date_column])

  cpi = 'cumulative_productivity_improvement'
  mpi = 'monthly_productivity_improvement'
  utph = 'updated_transactions_per_hc'
  tph = 'transactions_per_hc'
  tps = 'transactions_per_shipment'
  t = 'transactions'
  s = 'shipments'

  after_cutoff = temp_df.month >= cutoff_date
  before_cutoff = temp_df.month < cutoff_date

  temp_df['one'] = 1
  temp_df['run_tot'] = temp_df.loc[after_cutoff,:].groupby(grouping_column)['one'].cumsum()
  temp_df.loc[after_cutoff, cpi] = temp_df.loc[after_cutoff, mpi] ** (temp_df.loc[after_cutoff,'run_tot']-1)
  temp_df.loc[after_cutoff, utph] = temp_df.loc[after_cutoff, tph] * temp_df.loc[after_cutoff, cpi]
  temp_df.loc[before_cutoff, utph] = temp_df.loc[before_cutoff, tph] 
  
  temp_df[t] = temp_df[s] * temp_df[tps]
  temp_df['updated_hc'] = temp_df[t] / temp_df[utph]
  temp_df['original_hc'] = temp_df[t] / temp_df[tph]
  return temp_df

def import_gsheets(gsheets, names):
  d = {}
  counter = 0

  for i in gsheets:
    worksheet = gc.open(i).sheet1
    rows = worksheet.get_all_values()
    d[names[counter]] = pd.DataFrame.from_records(rows)
    d[names[counter]].columns = d[names[counter]].iloc[0,:]
    d[names[counter]] = d[names[counter]].iloc[1:,:]
    counter = counter + 1

  return d
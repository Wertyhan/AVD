import sys, pandas as pd, numpy as np, os
raw, out = sys.argv[1], sys.argv[2]
R = os.path.getsize(raw)/1e6
df = pd.read_csv(raw)
F = {}
F['raw_file']='btcusd_1-min_data.csv'
F['raw_csv_size_MB']=round(R,1)
F['archive_size_MB']=round(os.path.getsize(os.path.dirname(raw)+'/archive.zip')/1e6,1)
F['raw_rows']=len(df)
F['raw_columns_dtypes']='; '.join(f'{c}:{t}' for c,t in df.dtypes.astype(str).items())
F['raw_nan_cells']=int(df.isna().sum().sum())
df['dt']=pd.to_datetime(df.Timestamp,unit='s',utc=True)
F['raw_sorted']=bool(df.Timestamp.is_monotonic_increasing)
df=df.sort_values('Timestamp',kind='stable')
dup=int(df.Timestamp.duplicated().sum()); F['duplicate_timestamps']=dup
df=df.drop_duplicates('Timestamp',keep='first')
df=df.dropna(subset=['Open','High','Low','Close','Volume']) if False else df
nan_rows=int(df[['Open','High','Low','Close','Volume']].isna().any(axis=1).sum()); F['rows_with_nan']=nan_rows
F['off_minute_timestamps']=int((df.Timestamp%60!=0).sum())
F['first_timestamp_utc']=str(df.dt.iloc[0]); F['last_timestamp_utc']=str(df.dt.iloc[-1])
span=int((df.Timestamp.iloc[-1]-df.Timestamp.iloc[0])//60)+1
F['grid_minutes_expected']=span; F['missing_minutes']=span-len(df)
d=df.Timestamp.diff().dropna(); g=d[d>60].sort_values(ascending=False)
F['gap_count']=len(g)
for i,(ix,v) in enumerate(g.head(10).items(),1):
    s=df.dt.loc[ix]-pd.Timedelta(seconds=int(v)-60); e=df.dt.loc[ix]-pd.Timedelta(minutes=1)
    F[f'gap_{i}']=f'{int(v//60)-1} missing min, {s} to {e}'
z=int((df.Volume==0).sum()); F['zero_volume_candles']=z; F['zero_volume_share_pct']=round(100*z/len(df),3)
o,h,l,c,v=[df[x] for x in ['Open','High','Low','Close','Volume']]
F['invalid_high_lt_max_oc']=int((h<np.maximum(o,c)).sum())
F['invalid_low_gt_min_oc']=int((l>np.minimum(o,c)).sum())
F['invalid_high_lt_low']=int((h<l).sum())
F['negative_values']=int(((df[['Open','High','Low','Close','Volume']]<0).any(axis=1)).sum())
F['nonpositive_price_rows']=int((df[['Open','High','Low','Close']]<=0).any(axis=1).sum())
bad=(h<np.maximum(o,c))|(l>np.minimum(o,c))|(h<l)|(df[['Open','High','Low','Close','Volume']]<0).any(axis=1)|df[['Open','High','Low','Close','Volume']].isna().any(axis=1)
F['invalid_candles_total']=int(bad.sum())
df=df[~bad]  # removed (0 expected)
F['final_rows_1min']=len(df)
cols=['Timestamp','Open','High','Low','Close','Volume']
m=df[['dt']+cols[1:]].copy(); m.insert(0,'Timestamp',df.Timestamp.values)
m1=m.rename(columns={'dt':'datetime_utc'})
m1.to_parquet(out+'/btc_1min.parquet',index=False)
df[cols].to_csv(out+'/btc_1min.csv',index=False)
s=df[(df.dt>='2021-05-01')&(df.dt<'2021-06-01')]; s[cols].to_csv(out+'/btc_sample_1min.csv',index=False); F['sample_rows']=len(s)
def agg(rule,name):
    x=df.set_index('dt')
    r=x.resample(rule).agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    r['n_minutes']=x.Open.resample(rule).count()
    r=r[r.n_minutes>0]; r.index.name='datetime_utc'; r=r.reset_index()
    r.insert(0,'Timestamp',(r.datetime_utc.astype('int64')//10**9 if r.datetime_utc.dtype!='O' else 0))
    r['Timestamp']=((r.datetime_utc-pd.Timestamp('1970-01-01',tz='UTC'))//pd.Timedelta(seconds=1)).astype('int64')
    r.to_parquet(f'{out}/btc_{name}.parquet',index=False)
    r.drop(columns='datetime_utc').to_csv(f'{out}/btc_{name}.csv',index=False)
    return r
h1=agg('1h','1h'); d1=agg('1D','1d')
F['final_rows_1h']=len(h1); F['final_rows_1d']=len(d1)
F['incomplete_1h_bins(n<60)']=int((h1.n_minutes<60).sum()); F['incomplete_1d_bins(n<1440)']=int((d1.n_minutes<1440).sum())
i=df.Close.idxmax(); F['max_close']=f'{df.Close.max()} at {df.dt.loc[i]}'
i=df.Close.idxmin(); F['min_close']=f'{df.Close.min()} at {df.dt.loc[i]}'
lr=np.log(d1.Close).diff().abs().dropna(); 
t=d1.assign(lr=np.log(d1.Close).diff(), a=np.log(d1.Close).diff().abs()).nlargest(5,'a')
for k,(_,r) in enumerate(t.iterrows(),1):
    F[f'top{k}_daily_abs_logret']=f'{r.datetime_utc.date()} logret={r.lr:+.4f} close={r.Close} n_minutes={r.n_minutes}'
F['daily_logret_note']='log(Close_d/Close_{d-1}) between consecutive available daily bins; includes gap days'
pd.DataFrame(list(F.items()),columns=['metric','value']).to_csv(out+'/facts.csv',index=False)
for k,v in F.items(): print(k,'=',v)

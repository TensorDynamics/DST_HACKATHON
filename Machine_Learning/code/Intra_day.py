from funcs import *
import pytz
# print(datetime.now().time())


ts = roundTime(datetime.now(pytz.timezone('Asia/Kolkata')),roundTo=15*60)
#ts = datetime(2023,3,24,7,15)
print(f"intra day script current time is {str(ts)} ")

prev_forecast_start = time(0,0)
prev_forecast_end = (ts).time()

sat_forecast_start = (ts+timedelta(hours =0.25)).time()
sat_forecast_end = (ts+timedelta(hours=3)).time()

day_ahead_forecast_start = (ts+timedelta(hours=3.25)).time()
day_ahead_forecast_end = time(23,45)
db_connection = get_sql_connection()
sample_df_columns = read_sample_table_from_DB(schema=db_config.forecast_schema,
                                              table=db_config.forecast_table,
                                             con=db_connection)
ct_ci_df = pd.read_csv(os.path.join(resource_path,'trained_ci.csv'))
ct_flag_dict={}
ci_dict = {}
for _, r in ct_ci_df.iterrows():
    ci_dict[r['CT_Index']]= r['ci_wrf']
    ct_flag_dict[r['CT_Index']] =  r['CT_Flag']

for site in tqdm(sites):
    print(f"Nowcasting is in progress for {site} site")
    db_connection = get_sql_connection()
    #intra_day_path = os.path.join(home,'OUTPUT',site)
    day_ahead_df = day_ahead(db_connection=db_connection,
                              schema=db_config.wrf_schema,
                              table = db_config.wrf_view,
                              site=site,
                              date = ts.date())
    clearsky_df = clearsky(db_connection=db_connection,table=db_config.site_clearsky_table, schema=db_config.site_actual_schema,
                          date= ts.date(),site=site)
    cs_rolled = clearsky_roll_and_shift(clearsky_df=clearsky_df)

    satellite_df = satellite_ct(db_connection=db_connection,
                               table = db_config.satellite_ct_exim_ip_view,
                               schema=db_config.satellite_schema,
                               timestamp=ts,
                               date=ts.date(),
                               site=site)
    satellite_df['ci'] = satellite_df['ct'].map(ci_dict)
#     satellite_df['ct_flag'] = satellite_df['ct'].map(ct_flag_dict)
    satellite_ct_ci_df = ct_postprocessing(satellite_df=satellite_df)
    nowcasting_df = nowcasting(satellite_df=satellite_ct_ci_df,
                              clearsky_df=cs_rolled,
                              site=site,
                              date=ts.date())

    try:
        prev_forecast_file = max(glob.glob(os.path.join(intra_day_path,str(ts.date()))+'/*'), key = os.path.getmtime)
        prev_forecast_df = pd.read_csv(prev_forecast_file,
                                       index_col='timestamp',
                                       parse_dates=True)
        prev_forecast_df['forecast_method'] = len(prev_forecast_df.index)*['prev_forecast_file']
        prev_forecast_df['time'] = prev_forecast_df.index.time
    except:
        prev_forecast_df = day_ahead_df


    intra_day_first_df = slice_forecast(df = prev_forecast_df,
                                        start = prev_forecast_start,
                                        end = prev_forecast_end) 

    intra_day_second_df = slice_forecast(df = nowcasting_df,
                                        start = sat_forecast_start,
                                        end = sat_forecast_end)
    intra_day_third_df = slice_forecast(df = day_ahead_df,
                                        start = day_ahead_forecast_start,
                                        end = day_ahead_forecast_end) 

    intra_day_df = pd.concat([intra_day_first_df,
                              intra_day_second_df,
                              intra_day_third_df], axis=0)


    intra_day_df['ghi_predicted(w/m2)'] = intra_day_df['ghi_predicted(w/m2)'].clip(lower=0)
    intra_day_df = fill_values_to_zero_in_night(df=intra_day_df,
                                                target_col= 'ghi_predicted(w/m2)')
    intra_day_df = fill_missing_with_next_and_prev_mean(df=intra_day_df)

    #filling missing values to clearsky
    for i in tqdm(intra_day_df.index):
        if pd.isnull(intra_day_df.loc[i,'ghi_predicted(w/m2)']):
            intra_day_df.loc[i,'ghi_predicted(w/m2)'] = cs_rolled.loc[i,'cs_rolled']
            intra_day_df.loc[i,'forecast_method'] = 'clearsky_push'
    #filling missing values to day ahead
    for i in tqdm(intra_day_df.index):
        if np.isnan(intra_day_df.loc[i,'ghi_predicted(w/m2)']):
            intra_day_df.loc[i,'ghi_predicted(w/m2)'] = day_ahead_df.loc[i,'ghi_predicted(w/m2)']
            intra_day_df.loc[i,'forecast_method'] = 'day_ahead_push'
            
            
    #week ahead_data
    db_connection = get_sql_connection()
    week_ahead_data = week_ahead_df(con=db_connection,
                                   site=site,
                                   date=ts.date())
    week_ahead_data.rename(columns={'swdown':'ghi(w/m2)','temp':'T2'},inplace=True)
    week_ahead_data = clearsky_roll_and_shift(clearsky_df=week_ahead_data,
                                               target_col='ghi(w/m2)',
                                               target_col_new_name='ghi_predicted(w/m2)')
    for ts in tqdm(intra_day_df.index):
        try:
            week_ahead_data.loc[ts,'ghi_predicted(w/m2)'] = intra_day_df.loc[ts,'ghi_predicted(w/m2)']
        except:
            print(f"error in replacing week ahead data to nowcasting data")
    
#     week_ahead_data.to_csv(os.path.join(intra_day_path,'week_ahead.csv'), index_label='timestamp')
    
    final_intra_day_df = intra_day_df[['ghi_predicted(w/m2)','site_name']]
    try:
        last_hour = int(os.path.basename(prev_forecast_file).split('_')[-1].replace('.csv',''))
        current_forecast_hour = last_hour+1
    except:
        current_forecast_hour = 1
    print(f"saving intra day predictions for {site} site")

    #os.makedirs(os.path.join(intra_day_path,str(ts.date())), exist_ok = True)
    #final_intra_day_df.to_csv(os.path.join(intra_day_path,
    #                                     str(ts.date()),
    #                                     str(ts.date())+'_revision_'+str(current_forecast_hour)+'.csv'),
    #                                    index_label='timestamp')

    extra_cols = [col for col in sample_df_columns if col not in intra_day_df.columns]
    for col in extra_cols:
        intra_day_df[col] = len(intra_day_df.index)*[np.nan]

    intra_day_df1 = intra_day_df[sample_df_columns]

    
    #delete nowcasting data from database
    delete_db_query = f"DELETE from {db_config.forecast_schema}.{db_config.forecast_table} " \
                      f" WHERE timestamp::date = '{str(ts.date())}' and site_name = '{site}' "
    db_connection.execute(delete_db_query)
    print(f"DB intra_day data deleted for {str(ts.date())} date and {site} site ")
    #onboarding nowcasting data to database
    intra_day_df1.to_sql(con=db_connection,
                        schema=db_config.forecast_schema,
                        name=db_config.forecast_table, 
                        if_exists='append',
                        index_label='timestamp')
    print(f"Intra day data uploaded to DB for date {str(ts.date())} and {site} site ")
    
    #delete week ahead api data from database
    for dt in tqdm(set(week_ahead_data.index.date)):
        delete_db_query = f"DELETE from {db_config.forecast_schema}.{db_config.api_table} " \
                          f" WHERE timestamp::date = '{str(dt)}' and site_name = '{site}' "
        db_connection.execute(delete_db_query)
        print(f"DB week_ahead_api data deleted for {str(dt)} date and {site} site ")
    #onboarding week ahead api data to database
    week_ahead_data.to_sql(con=db_connection,
                            schema=db_config.forecast_schema,
                            name=db_config.api_table, 
                            if_exists='append',
                            index_label='timestamp')
    print(f"week_ahead data uploaded to DB for date {str(ts.date())} and {site} site ")

# print(datetime.now().time())

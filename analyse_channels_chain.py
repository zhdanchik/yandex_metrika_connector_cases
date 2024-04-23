# -*- coding: utf-8 -*-
import tqdm
from functools import reduce
import pandas as pd

metr_raw_drop_query = '''
drop table if exists {database}.{metr_raw_table_name}
'''

metr_raw_create_query = '''
CREATE TABLE {database}.{metr_raw_table_name} (
    `CounterID` UInt32, `UserID` UInt64, `VisitID` UInt64,
    `StartDate` Date, `UTCStartTime` DateTime, 
    `StartTime` DateTime, `Duration` UInt32,
    `TraficSourceID` Int8, `SearchEngineID` UInt16, `AdvEngineID` UInt8, `SocialSourceNetworkID` UInt8, 
    `ClickBannerID` UInt64, `ClickTargetType` UInt16, 
    `RecommendationSystemID` UInt8, `MessengerID` UInt8, 
    Conversions UInt32, Revenue Double
) 
ENGINE = MergeTree 
ORDER BY intHash32(UserID) 
SAMPLE BY intHash32(UserID)'''

metr_raw_insert_query = '''
insert into {database}.{metr_raw_table_name}
with TrafficSource.ClickBannerID[indexOf(TrafficSource.Model, 1)] as ClickBannerID_pre, 
    TrafficSource.ClickTargetType[indexOf(TrafficSource.Model, 1)] as ClickTargetType_pre,
    TrafficSource.StartTime[indexOf(TrafficSource.Model, 1)] as StartTime_pre,
    TrafficSource.ID[indexOf(TrafficSource.Model, 1)] as TraficSourceID_pre,
    TrafficSource.SearchEngineID[indexOf(TrafficSource.Model, 1)] as SearchEngineID_pre,
    TrafficSource.AdvEngineID[indexOf(TrafficSource.Model, 1)] as AdvEngineID_pre,
    TrafficSource.SocialSourceNetworkID[indexOf(TrafficSource.Model, 1)] as SocialSourceNetworkID_pre,
    TrafficSource.RecommendationSystemID[indexOf(TrafficSource.Model, 1)] as RecommendationSystemID_pre,
    TrafficSource.MessengerID[indexOf(TrafficSource.Model, 1)] as MessengerID_pre
SELECT
    CounterID, UserIDHash as UserID, VisitID, StartDate,     
    argMax(UTCStartTime, VisitVersion) as UTCStartTime, 
    argMax(StartTime_pre, VisitVersion) as StartTime, 
    argMax(Duration, VisitVersion) as Duration, 
    argMax(TraficSourceID_pre, VisitVersion) as TraficSourceID, 
    argMax(SearchEngineID_pre, VisitVersion) as SearchEngineID, 
    argMax(AdvEngineID_pre, VisitVersion) as AdvEngineID, 
    argMax(SocialSourceNetworkID_pre, VisitVersion) as SocialSourceNetworkID, 
    argMax(RecommendationSystemID_pre, VisitVersion) as RecommendationSystemID, 
    argMax(ClickTargetType_pre, VisitVersion) as ClickTargetType, 
    argMax(RecommendationSystemID_pre, VisitVersion) as RecommendationSystemID, 
    argMax(MessengerID_pre, VisitVersion) as MessengerID, 
    argMax({conversion_statement}, VisitVersion) as Conversions, 
    argMax({revenue_statement},  VisitVersion) as Revenue
FROM {database}.{raw_visits_table}
WHERE (StartDate >= '{portion_start_date}') AND (StartDate <= '{portion_end_date}')
group by CounterID, UserID, VisitID, StartDate
having sum(Sign) > 0
'''

metr_combine_drop_query = '''
drop table if exists {database}.{metr_combined_table_name}
'''

metr_combine_create_query = '''
CREATE TABLE {database}.{metr_combined_table_name}
( 
    StartDate Date,
    UserID UInt64,  
    CounterID UInt32,
    `history.VisitID` Array(UInt64),  
    `history.SourceCode` Array(String),
    `history.StartTime` Array(DateTime),
    `history.UTCStartTime` Array(DateTime),
    `history.EventType` Array(String),    
    `history.Duration` Array(Int32),
    `history.Conversions` Array(Float64),
    VisitsSum UInt32,
    VisitsSumAfterDuration UInt32,
    Conversions UInt32,
    Revenue Double
)
ENGINE = MergeTree
ORDER BY intHash32(UserID)
SAMPLE BY intHash32(UserID)
'''

metr_combine_insert_final_query = '''
INSERT into {database}.{metr_combined_table_name}
select 
    toDate(`history.StartTime`[-1]) as StartDate,
    UserID, 
    CounterID, 
    `history.VisitID`,
    `history.SourceCode`, 
    `history.StartTime`,
    `history.UTCStartTime`,
    `history.EventType`,
    `history.Duration`,
    `history.Conversions`,
    VisitsSum,
    VisitsSumAfterDuration,
    `history.Conversions`[-1] as Conversions,
    `history.Revenue`[-1] as Revenue
from
(
    select CounterID, UserID
        , arrayFilter(cnt_i, y -> y == 'null' and cnt_i <= i - 1, cnt, `history.SourceCode_pre`)[-1] as indx
        , arraySlice(`history.VisitID_pre`, indx + 1, i - indx) as `history.VisitID`
        , arraySlice(`history.SourceCode_pre`, indx + 1, i - indx) as `history.SourceCode`
        , arraySlice(`history.EventType_pre`, indx + 1, i - indx) as `history.EventType`
        , arraySlice(`history.UTCStartTime_pre`, indx + 1, i - indx) as `history.UTCStartTime`
        , arraySlice(`history.StartTime_pre`, indx + 1, i - indx) as `history.StartTime`
        , arraySlice(`history.Duration_pre`, indx + 1, i - indx) as `history.Duration`
        , arraySlice(`history.Conversions_pre`, indx + 1, i - indx) as `history.Conversions`
        , arraySlice(`history.Revenue_pre`, indx + 1, i - indx) as `history.Revenue`
        , arrayCount(time_i, event_type -> time_i > utc_time_start and time_i <= utc_time_start + toInt64({visit_max_timediff} )
            and event_type = '2_VISIT', `history.UTCStartTime_pre`, `history.EventType_pre`) as VisitsSum
        , arrayCount(time_i, event_type -> time_i >= utc_time_start + if(`history.Duration_pre`[i]=0,1,`history.Duration_pre`[i]) 
            and time_i <= utc_time_start + toInt64({visit_max_timediff} )
            and event_type = '2_VISIT' , `history.UTCStartTime_pre`, `history.EventType_pre`) as VisitsSumAfterDuration
    from
    (
        select CounterID, UserID,
            groupArray(Conversions) as `history.Conversions_pre`,
            groupArray(Revenue) as `history.Revenue_pre`,
            groupArray(Duration)  as `history.Duration_pre`,
            groupArray(StartTime)  as `history.StartTime_pre`,
            groupArray(UTCStartTime)  as `history.UTCStartTime_pre`,
            groupArray(VisitID) as `history.VisitID_pre`,
            groupArray(SourceCode) as `history.SourceCode_pre`,
            groupArray(EventType) as `history.EventType_pre`,
            arrayEnumerate(`history.SourceCode_pre`) as cnt
        from 
        (
            select * from 
            (
                (
                    Select '2_VISIT' as EventType, CounterID, StartTime, UTCStartTime, UserID, VisitID, Duration,
                        toString(TraficSourceID) ||  multiIf(
                            TraficSourceID == 2, '_' || toString(SearchEngineID),
                            TraficSourceID == 3 and AdvEngineID == 1 and ClickBannerID != 0, '_' || toString(AdvEngineID) || '_' || toString(ClickTargetType),
                            TraficSourceID == 3,  '_' || toString(AdvEngineID),
                            TraficSourceID == 8,  '_' || toString(SocialSourceNetworkID),
                            TraficSourceID == 9,  '_' || toString(RecommendationSystemID),
                            TraficSourceID == 10, '_' || toString(MessengerID),
                        '') as SourceCode,
                        Conversions, 
                        Revenue
                    from {database}.{metr_raw_table_name} sample 1/{iter_sample} offset {i}/{iter_sample}
                )
                union all 
                (
                    select '0_NULL' as EventType, CounterID, 
                        time + toUInt64({visit_max_timediff}) as StartTime,
                        utc_time + toUInt64({visit_max_timediff}) as UTCStartTime, 
                        UserID, 0 as VisitID, 0 as Duration,
                        'null' as SourceCode, 0 as Conversions, 0.0 as Revenue
                    from
                    (
                        SELECT
                            CounterID, UserID, 
                            arraySort(groupArray(UTCStartTime)) AS utc_times,
                            arraySort(groupArray(StartTime)) AS times,
                            arrayEnumerate(utc_times) as indexes,
                            arraySlice(arrayMap(y -> if(y = 1, 0, utc_times[y] - utc_times[(y - 1)]), indexes), 2) AS diffs,
                            arraySlice(arrayMap(y -> utc_times[(y - 1)], indexes), 2) AS cut_utc_times,
                            arraySlice(arrayMap(y -> times[(y - 1)], indexes), 2) AS cut_times
                        FROM {database}.{metr_raw_table_name} sample 1/{iter_sample} offset {i}/{iter_sample}
                        GROUP BY CounterID, UserID
                        HAVING length(times) > 1
                    )
                    array join diffs as diff, cut_times as time, cut_utc_times as utc_time
                    where diff > {visit_max_timediff}
                )
                union all
                (
                    Select EventType, CounterID,                        
                        StartTime_pre as StartTime,
                        UTCStartTime_pre as UTCStartTime,
                        UserID, 
                        VisitID, Duration,
                        SourceCode, Conversions, Revenue
                    from
                    (
                        select '0_NULL' as EventType, CounterID, 
                            max(StartTime)+ toUInt64({visit_max_timediff}) as StartTime_pre, 
                            max(UTCStartTime)+ toUInt64({visit_max_timediff}) as UTCStartTime_pre, 
                            UserID, 0 as VisitID, 0 as Duration, 
                            'null' as SourceCode, 0 as Conversions, 0.0 as Revenue
                        FROM {database}.{metr_raw_table_name} sample 1/{iter_sample} offset {i}/{iter_sample}
                        group by CounterID, UserID
                        having toDate(StartTime) <= toDate('{end_date}')
                    )
                )
            )
            order by UTCStartTime, EventType
        )
        group by CounterID, UserID
    )
    array join cnt as i, 
        `history.UTCStartTime_pre` as utc_time_start,
        `history.StartTime_pre` as time_start
)
'''

visit_diff_percentile_q = '''
SELECT quantile({percentile})(diff)
from
(
    select UserID, diffs,diff
    from
    (
        SELECT
            UserID,
            arraySort(groupArray(UTCStartTime)) AS times,
            arrayEnumerate(times) as indexes,
            arraySlice(arrayMap((x, y) -> if(y = 1, 0, times[y] - times[(y - 1)]), times, indexes), 2) AS diffs
        FROM {database}.{metr_raw_table_name} sample 1/{visits_diff_sample}
        GROUP BY UserID
        HAVING length(times) > 1
    )
    array join diffs as diff
)
format TSV
'''

# ---------Simple Attributions

last_click_attr_q = '''
WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
SELECT
    only_visits_sources[-1] as SourceCode,
    count() AS LastClickVisits,
    sum(Conversions) as LastClickConvs,
    sum(Revenue) as LastClickRevenue
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null')
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

first_click_attr_q = '''
WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
SELECT
    only_visits_sources[1] as SourceCode,
    count() AS FirstClickVisits,
    sum(Conversions) as FirstClickConvs,
    sum(Revenue) as FirstClickRevenue
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null')
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

last_sign_click_attr_q = '''
WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
SELECT
    if((arrayFilter(x -> (x NOT IN ('0', '-1', '4', '5', '6')), only_visits_sources)[-1] AS farr) != '', 
        farr, 
        only_visits_sources[-1]) AS SourceCode,
    count() AS LastSignClickVisits,
    sum(Conversions) as LastSignClickConvs,
    sum(Revenue) as LastSignClickRevenue
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null')
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

last_sign_direct_attr_q = '''
WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
SELECT
    multiIf(
        (arrayFilter(x -> (x like '3_1_%'), only_visits_sources)[-1] AS f_direct_click_arr) != '', f_direct_click_arr,
        (arrayFilter(x -> (x == '3_1'), only_visits_sources)[-1] AS f_direct_arr) != '', f_direct_arr,
        (arrayFilter(x -> (x NOT IN ('0', '-1', '4', '5', '6')), only_visits_sources)[-1] AS f_signif_arr) != '', f_signif_arr, 
            only_visits_sources[-1]
    ) AS SourceCode,
    count() AS LastSignDirectVisits,
    sum(Conversions) as LastSignDirectConvs,
    sum(Revenue) as LastSignDirectRevenue
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null')
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

linear_attr_q = '''
SELECT
    SourceCode,
    sum(1 / arr_len) AS LinearAttrVisits,
    sum(Conversions / arr_len) AS LinearAttrConvs,
    sum(Revenue / arr_len) as LinearAttrRevenue
FROM
(
    WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
    SELECT
        SourceCode,
        length(only_visits_sources) AS arr_len,
        Conversions,
        Revenue
    FROM {database}.{metr_combined_table_name}
    ARRAY JOIN arrayFilter(x -> 1, only_visits_sources) AS SourceCode
    WHERE (`history.SourceCode`[-1] != 'null')
)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

associated_convs_q = '''
SELECT
    SourceCode,
    count() AS AssociatedVisits,
    sum(Conversions) AS AssociatedConvs,
    sum(Revenue) as AssociatedRevenue
FROM
(
    WITH arrayFilter(x,y -> y ='2_VISIT',  `history.SourceCode`, `history.EventType`) as only_visits_sources
    SELECT
        SourceCode,
        length(only_visits_sources) AS arr_len,
        Conversions,
        Revenue
    FROM {database}.{metr_combined_table_name}
    ARRAY JOIN arrayReduce('groupUniqArray', only_visits_sources) AS SourceCode
    WHERE (`history.SourceCode`[-1] != 'null')
)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames
'''

# -------------Участие источников в цепочках
# 1
sources_in_chains_q1 = '''SELECT
    SourceCode,
    count() /
    (
        SELECT sum(length(`history.SourceCode`))
        FROM {database}.{metr_combined_table_name}
        WHERE `history.SourceCode`[-1] != 'null'
    ) AS in_chain_perc_total,
    countIf(Conversions > 0) / 
    (
        SELECT sumIf(length(`history.SourceCode`), Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE `history.SourceCode`[-1] != 'null'
    ) as in_conv_chains_perc_total
FROM
(
    SELECT SourceCode, Conversions
    FROM {database}.{metr_combined_table_name}
    ARRAY JOIN `history.SourceCode` AS SourceCode
    WHERE `history.SourceCode`[-1] != 'null'
)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 2
sources_in_chains_q2 = '''SELECT
    SourceCode,
    uniq(chain_str) /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE `history.SourceCode`[-1] != 'null'
    ) AS unique_chains_perc,
    uniqIf(chain_str, Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE `history.SourceCode`[-1] != 'null'
    ) AS unique_conv_chains_perc    
FROM
(
    SELECT
        SourceCode, Conversions,
        cityHash64(CAST(`history.VisitID`, 'String')) AS chain_str
    FROM {database}.{metr_combined_table_name}
    ARRAY JOIN `history.SourceCode` AS SourceCode
    WHERE `history.SourceCode`[-1] != 'null'
)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 3 First 2+
sources_in_chains_q3 = '''SELECT
    history.SourceCode[1] AS SourceCode,
    count() /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
    ) AS first_touch_2plus,
    countIf(Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
    ) AS first_touch_conv_2plus
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 4 First All
sources_in_chains_q4 = '''SELECT
    history.SourceCode[1] AS SourceCode,
    count() /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') 
    ) AS first_touch_total,
    countIf(Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') 
    ) AS first_touch_conv_total
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null') 
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 5 MIddle

sources_in_chains_q5 = '''SELECT
    SourceCode,
    uniq(chain_str) /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 3)
    ) AS mid_touch_3plus,
    uniqIf(chain_str, Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 3)
    ) AS mid_touch_conv_3plus
FROM
(
    SELECT
        SourceCode, Conversions,
        cityHash64(CAST(`history.VisitID`, 'String')) AS chain_str
    FROM {database}.{metr_combined_table_name}
    ARRAY JOIN arraySlice(`history.SourceCode`, 2, -1) AS SourceCode
    WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 3)
)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 6 Last 2+
sources_in_chains_q6 = '''SELECT
    history.SourceCode[-1] AS SourceCode,
    count() /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
    ) AS last_touch_2plus,
    countIf(Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
    ) AS last_touch_conv_2plus
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null') AND (length(`history.SourceCode`) > 1)
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

# 7 Last All

sources_in_chains_q7 = '''SELECT
    history.SourceCode[-1] AS SourceCode,
    count() /
    (
        SELECT count()
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') 
    ) AS last_touch_total,
    countIf(Conversions > 0) /
    (
        SELECT countIf(Conversions > 0)
        FROM {database}.{metr_combined_table_name}
        WHERE (`history.SourceCode`[-1] != 'null') 
    ) AS last_touch_conv_total
FROM {database}.{metr_combined_table_name}
WHERE (`history.SourceCode`[-1] != 'null') 
GROUP BY SourceCode
FORMAT TabSeparatedWithNames'''

def sum_dicts(*ds):
    return dict(reduce(lambda a, b: a + b, [list(x.items()) for x in ds]))

class DataPreparer:
    def __init__(self, table_preffix, raw_visits_table, database, start_date, end_date, goals, my_ch_client, iter_sample=100, visits_diff_sample=10):
        self.tables_preffix = table_preffix
        self.raw_visits_table = raw_visits_table
        self.database = database
        self.start_date = start_date
        self.end_date = end_date

        self.goals = goals

        self.conversion_statement = self.conversion_formula()
        self.revenue_statement = self.revenue_formula()

        self.iter_sample = iter_sample
        self.visits_diff_sample = visits_diff_sample

        self.metr_raw_table_name = self.tables_preffix + '_raw'
        self.metr_combined_table_name = self.tables_preffix + '_combined'
        
        self.my_ch_client = my_ch_client
        
    def my_get_data(self, query, format_dict=None, debug=False):
        final_dict = sum_dicts(self.__dict__, format_dict) if format_dict else self.__dict__
        final_query = query.format(**final_dict)
        if debug:
            print(final_query)
        return self.my_ch_client.get_clickhouse_data(final_query)

    def my_get_df(self, query, format_dict=None, debug=False):
        final_dict = sum_dicts(self.__dict__, format_dict) if format_dict else self.__dict__
        final_query = query.format(**final_dict)
        if debug:
            print(final_query)
        return self.my_ch_client.get_clickhouse_df(final_query)
                  
    def visit_diff_percentile(self, percentile=0.95):
        new_dict = sum_dicts(self.__dict__, locals())
        return float(self.my_ch_client.get_clickhouse_data(visit_diff_percentile_q.format(**new_dict)))
        
    def conversion_formula(self):
        if self.goals == []:
            return "1"
        elif self.goals == [-1]:
            return "length(EPurchase.ID)"
        else:
            return 'length(arrayFilter(x -> has([{}], x), Goals.ID))'.format(','.join(map(str, self.goals)))

    def revenue_formula(self):
        # Тут не приводятся курсы валют, предполагаем, что в Revenue - рубли
        if self.goals == []:
            return "0"
        elif self.goals == [-1]:
            return "round(arraySum(arrayMap(x -> (if(isFinite(x), x, 0)), arrayMap(Revenue -> (multiIf(Revenue < 0, 0, Revenue > 112170000000, 0, Revenue / 1.) / 1000000), `EPurchase.Revenue`))), 2)" 
        else:
            return 'round(arraySum((arrayFilter(x, y -> has([{}], y), Goals.Price, Goals.ID)) / 1000., 2)'.format(','.join(map(str, self.goals)))

    def prepare_visits_data(self, debug=False):
        self.my_get_data(metr_raw_drop_query, debug=debug)
        self.my_get_data(metr_raw_create_query, debug=debug)
        for ddate in tqdm.tqdm(pd.date_range(start=self.start_date, end=self.end_date)):
            portion_start_date = ddate.date()
            portion_end_date = ddate.date()
            self.my_get_data(metr_raw_insert_query, format_dict=locals(), debug=debug)

    def combine_visits(self, debug=False):
        self.my_get_data(metr_combine_drop_query, debug=debug)
        self.my_get_data(metr_combine_create_query, debug=debug)

        visit_max_timediff = self.visit_diff_percentile(0.95)
    
        for i in tqdm.tqdm(range(self.iter_sample)):
            self.my_get_data(metr_combine_insert_final_query, format_dict=locals(), debug=debug)

    def simple_attributions(self, debug=False):
        lc_df  = self.my_get_df(last_click_attr_q,       format_dict=locals(), debug=debug).set_index('SourceCode')
        fc_df  = self.my_get_df(first_click_attr_q,      format_dict=locals(), debug=debug).set_index('SourceCode')
        lsc_df = self.my_get_df(last_sign_click_attr_q,  format_dict=locals(), debug=debug).set_index('SourceCode')
        lsd_df = self.my_get_df(last_sign_direct_attr_q, format_dict=locals(), debug=debug).set_index('SourceCode')
        lin_df = self.my_get_df(linear_attr_q,           format_dict=locals(), debug=debug).set_index('SourceCode')

        res_df = lc_df.merge(fc_df, how='outer', on='SourceCode') \
                      .merge(lsc_df, how='outer', on='SourceCode') \
                      .merge(lsd_df, how='outer', on='SourceCode') \
                      .merge(lin_df, how='outer', on='SourceCode') \
                      .fillna(0).reset_index().rename(columns={'index': 'SourceCode'})
        source_names = pd.read_csv('source_names.csv', sep='\t')
        res_df = res_df.merge(source_names, on='SourceCode', how='left')
        res_df['SourceName'] = res_df['SourceName'].fillna('Неизвестный источник')
        return res_df

    def associated_conversions(self, debug=False):
        ass_df  = self.my_get_df(associated_convs_q,       format_dict=locals(), debug=debug)
        source_names = pd.read_csv('source_names.csv', sep='\t')
        res_df = ass_df.merge(source_names, on='SourceCode', how='left')
        res_df['SourceName'] = res_df['SourceName'].fillna('Неизвестный источник')
        return res_df

    def source_in_chains_metrics(self, debug=False):
        new_dict = sum_dicts(self.__dict__, locals())

        tmp_df1 = self.my_get_df(sources_in_chains_q1, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df2 = self.my_get_df(sources_in_chains_q2, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df3 = self.my_get_df(sources_in_chains_q3, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df4 = self.my_get_df(sources_in_chains_q4, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df5 = self.my_get_df(sources_in_chains_q5, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df6 = self.my_get_df(sources_in_chains_q6, format_dict=locals(), debug=debug).set_index('SourceCode')
        tmp_df7 = self.my_get_df(sources_in_chains_q7, format_dict=locals(), debug=debug).set_index('SourceCode')

        res_df = tmp_df1 \
            .merge(tmp_df2, how='outer', on='SourceCode') \
            .merge(tmp_df3, how='outer', on='SourceCode') \
            .merge(tmp_df4, how='outer', on='SourceCode') \
            .merge(tmp_df5, how='outer', on='SourceCode') \
            .merge(tmp_df6, how='outer', on='SourceCode') \
            .merge(tmp_df7, how='outer', on='SourceCode') \
            .fillna(0).reset_index().rename(columns={'index': 'SourceCode'})
            
        source_names = pd.read_csv('source_names.csv', sep='\t')
        res_df = res_df.merge(source_names, on='SourceCode', how='left')
        res_df['SourceName'] = res_df['SourceName'].fillna('Неизвестный источник')
        return res_df

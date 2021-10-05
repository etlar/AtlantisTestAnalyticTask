"""
Atlantis Test Task Main Script

Written by: Egor Bagaev
"""
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging



logging.basicConfig(format='%(filename)s[LINE:%(lineno)d]# \
    %(levelname)-8s [%(asctime)s]  %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(level=logging.INFO)


PATH_TO_DB = 'E:/atlantis_test_task/test.db'
DT_FORMAT = '%Y-%m-%d %H:%M:%S'


def median_count_lenght_of_session(session_df: pd.DataFrame):
    """
    Function for calculating average session duration, session length, number of player sessions

    arguments:
        - session_df = pandas data frame with player session data
    return:
        - data = average session duration, session length, number of player sessions
    """
    duration = session_df.groupby(['user_id'])['duration'].median().median()
    session_count = session_df.groupby('user_id')['close_time'].count()
    all_time_sessions = session_df.groupby('user_id')['duration'].sum()
    data = {'duration': duration,  'session_count':session_count, 'all_time_sessions': all_time_sessions}
    return data


def days_in_game(session_df: pd.DataFrame):
    """
    Function for calculating days in game players
    
    arguments:
        - session_df = pandas data frame with player session data
    return:
        - data = dictionary with days in game players
    """
    session_df['open_date'] = session_df['open_time'].dt.date
    days_in_game = session_df.groupby('user_id')['open_date'].nunique()
    df_dg = pd.DataFrame(data=days_in_game.values, columns=['days_in_game'])
    data = {'days_in_game': df_dg}
    return data


def arpdau_dau_wau_sticky_factor(session_df: pd.DataFrame, payment_df: pd.DataFrame):
    """
    Function for calculating ARPDAU, DAU, WAU and sticky factor
    arguments:
        - session_df = pandas data frame with player session data
        - payment_df = pandas data frame with payment data
    return:
        - data = dictionary with ARPDAU, DAU, WAU, DAU_by_week, sticky factor metrics
    """
    dau = session_df.groupby(pd.Grouper(key='open_time', freq='D'))['user_id'].nunique()
    wau = session_df.groupby(pd.Grouper(key='open_time', freq='W'))['user_id'].nunique()
    dau_by_week = session_df.groupby([pd.Grouper(key='open_time', freq='W'), \
        pd.Grouper(key='open_time', freq='D')])['user_id'].nunique().mean(level=0) # by week
    sticky_factor = dau_by_week.mean(level=0)/wau
    day_amount = payment_df.groupby(pd.Grouper(key='time', freq='D'))['amount'].sum()
    arpdau = day_amount / dau
    df_arpdau = pd.DataFrame(data=arpdau, columns=['value'])
    data = {'ARPDAU': df_arpdau ,'DAU':dau, 'WAU': wau, 'DAU_by_week': dau_by_week, 'sticky_factor': sticky_factor}
    return data 


def retention(session_df: pd.DataFrame, profile_df: pd.DataFrame):
    """
    Function for calculating retention rate
    arguments:
        - session_df = pandas data frame with player session data
        - profile_df = pandas data frame with player profile data
    return:
        - data = dictionary with retention
    """
    session_close = session_df.merge(profile_df[['user_id','reg_time']], how='left')
    session_close['first_day'] = session_close['reg_time'].dt.date
    # how much time elapsed between registration and session
    session_close['delta_t'] = session_close['open_time'] - session_close['reg_time'] 
    session_close['n_day'] = session_close['delta_t'].dt.days
    # number of users on day n
    count_n_day_users = session_close.groupby('n_day')['user_id'].nunique()
    # you need to remove those who have not passed n days from the date of registration
    # the actual date (in real time it is today) will be the maximum observation date, that is, the maximum close_time
    report_date = session_close['close_time'].max()
    # how many days have passed since registration
    profile_df['days_since_reg'] = (report_date - profile_df['reg_time']).dt.days
    retention = [count_n_day_users[nday] / profile_df.loc[profile_df['days_since_reg'] >= nday, 'user_id'].nunique() for nday in range(count_n_day_users.index.max())]
    df_retention = pd.DataFrame(data=retention, columns=['retention'])
    df_retention['days'] = df_retention.index
    df_retention['retention'] = df_retention['retention'] * 100
    data = {'retention' : df_retention}
    return data 


def paying_share_and_gross_revenue(payment_df: pd.DataFrame,  profile_df: pd.DataFrame):
    """
    Function for calculating paying share and gross revenue
    arguments:
        - payment_df = pandas data frame with payment data
        - profile_df = pandas data frame with player profile data
    return:
        - data = dictionary with paying share and gross revenue
    """
    paying_players = payment_df['user_id'].nunique()
    paying_share = paying_players / profile_df['user_id'].nunique()
    gross_revenue = payment_df['amount'].sum()
    data = {'paying_share': paying_share, 'gross_revenue': gross_revenue}
    return data


def ltv(payment_df: pd.DataFrame,  profile_df: pd.DataFrame):
    """
    Function for calculating LTV
    arguments:
        - payment_df = pandas data frame with payment data
        - profile_df = pandas data frame with player profile data
    return:
        - data = dictionary with LTV
    """
    payment_table = payment_df.merge(profile_df[['user_id','reg_time']], how='left')
    payment_table['days_since_reg'] = (payment_table['time'] - payment_table['reg_time']).dt.days
    amount_per_day = payment_table.groupby('days_since_reg').sum().cumsum()
    ltv_pivot = payment_table.pivot_table(values='amount', index=pd.Grouper(key='reg_time', freq='D'), columns='days_since_reg', aggfunc='sum', fill_value=0).cumsum(axis=1)
    reg_per_day = profile_df.groupby(pd.Grouper(key='reg_time', freq='D'))['user_id'].nunique()
    reg_per_day.name = 'new_players'
    ltv = ltv_pivot.div(reg_per_day,axis=0).mean(axis=0)
    data = {'ltv': ltv}
    return data


if __name__ == "__main__":
    log.info('The script is running, wait for the end of the calculations...')
    # connnection with DB and upload to DataFrame
    with sqlite3.connect(PATH_TO_DB) as conn:
        profile_table =  pd.read_sql("SELECT * FROM profile", conn)
        payment_table = pd.read_sql("SELECT * FROM payment", conn)
        level_up_table = pd.read_sql("SELECT * FROM level_up", conn)
        quest_start_table = pd.read_sql("SELECT * FROM quest_start", conn)
        quest_complete_table = pd.read_sql("SELECT * FROM quest_complete", conn)
        session_close = pd.read_sql("SELECT * FROM session_close", conn)
    
    # convert the required columns to datetime
    session_close['open_time'] = pd.to_datetime(pd.to_datetime(session_close['open_time']).dt.strftime(DT_FORMAT))
    session_close['close_time'] = pd.to_datetime(pd.to_datetime(session_close['close_time']).dt.strftime(DT_FORMAT))
    profile_table['reg_time'] = pd.to_datetime(pd.to_datetime(profile_table['reg_time']).dt.strftime(DT_FORMAT))
    payment_table['time'] = pd.to_datetime(pd.to_datetime(payment_table['time']).dt.strftime(DT_FORMAT))

    
   
    


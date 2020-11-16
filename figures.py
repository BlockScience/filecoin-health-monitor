# %%

# Dependences
import json
import requests as req
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
from time import time

# Prepare SQL connection string to be used on the functions
CONN_STRING_PATH = 'config/sentinel-conn-string.txt'

with open(CONN_STRING_PATH, 'r') as fid:
    conn_string = fid.read()


def simple_time_series(fig_df: pd.DataFrame, VIZ_PARAMS: dict):
    if len(fig_df) > 0:
        fig = px.line(fig_df,
                      x='time',
                      y='value',
                      **VIZ_PARAMS
                      )
    else:
        fig = None
    return fig


# Visualizations
def relative_token_distribution(connection):
    QUERY = """
            WITH td AS (
                SELECT 
                ce.circulating_fil::NUMERIC / 1e18 AS circulating_fil,
                ce.vested_fil::NUMERIC / 1e18 AS vested_fil,
                ce.mined_fil::NUMERIC / 1e18 AS mined_fil,
                ce.burnt_fil::NUMERIC / 1e18 AS burnt_fil,
                ce.locked_fil::NUMERIC / 1e18 AS locked_fil,
                min(to_timestamp(b.timestamp)) AS timestamp
                FROM chain_economics ce
                LEFT JOIN block_headers b 
                ON b.parent_state_root = ce.parent_state_root
                GROUP BY ce.parent_state_root
                ORDER BY timestamp asc
            ), s AS (
                SELECT
                (td.circulating_fil + 
                td.vested_fil + 
                td.mined_fil + 
                td.burnt_fil + 
                td.locked_fil)
                as supply_fil,
                td.timestamp AS timestamp
                FROM td
            )
            SELECT
            AVG(td.circulating_fil / s.supply_fil) AS fil_circulating_fraction,
            AVG(td.vested_fil / s.supply_fil) AS fil_vested_fraction,
            AVG(td.mined_fil / s.supply_fil) AS fil_mined_fraction,
            AVG(td.burnt_fil / s.supply_fil) AS fil_burnt_fraction,
            AVG(td.locked_fil / s.supply_fil) AS fil_locked_fraction,
            MIN(td.timestamp) AS timestamp
            FROM td
            JOIN s ON s.timestamp = td.timestamp
            GROUP BY date_trunc('hour', td.timestamp)
            ORDER BY timestamp
            """

    df = (pd.read_sql(QUERY, connection)
          )

    fig_df = df.melt(id_vars=['timestamp'])
    if len(fig_df) > 0:
        fig = px.line(fig_df,
                      x='timestamp',
                      y='value',
                      color='variable',
                      title='Relative token distribution',
                      labels={'value': '% of FIL supply',
                              'timestamp': 'Timestamp',
                              'variable': 'Token status'})
    else:
        fig = None
    return fig


def absolute_token_distribution(connection):
    QUERY = """
            WITH td AS (
                SELECT 
                ce.circulating_fil::NUMERIC / 1e18 AS circulating_fil,
                ce.vested_fil::NUMERIC / 1e18 AS vested_fil,
                ce.mined_fil::NUMERIC / 1e18 AS mined_fil,
                ce.burnt_fil::NUMERIC / 1e18 AS burnt_fil,
                ce.locked_fil::NUMERIC / 1e18 AS locked_fil,
                min(to_timestamp(b.timestamp)) AS timestamp
                FROM chain_economics ce
                LEFT JOIN block_headers b 
                ON b.parent_state_root = ce.parent_state_root
                GROUP BY ce.parent_state_root
                ORDER BY timestamp asc
            )
            SELECT
            AVG(td.circulating_fil) AS fil_circulating_fraction,
            AVG(td.vested_fil) AS fil_vested_fraction,
            AVG(td.mined_fil) AS fil_mined_fraction,
            AVG(td.burnt_fil) AS fil_burnt_fraction,
            AVG(td.locked_fil) AS fil_locked_fraction,
            MIN(td.timestamp) AS time
            FROM td
            GROUP BY date_trunc('hour', td.timestamp)
            ORDER BY time
            """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='ms'))
          )

    fig_df = df.melt(id_vars=['time'])

    if len(fig_df) > 0:
        fig = px.line(fig_df,
                      x='time',
                      y='value',
                      color='variable',
                      title='Absolute token distribution',
                      labels={'value': 'FIL',
                              'time': 'Timestamp',
                              'variable': 'Token status'})
    else:
        fig = None
    return fig


def fil_price(connection):
    r = req.get(
        'https://api.coingecko.com/api/v3/coins/filecoin/market_chart?vs_currency=usd&days=max')
    d = json.loads(r.content)['prices']
    fig_df = (pd.DataFrame(d, columns=['timestamp', 'price'])
                .assign(timestamp=lambda df: pd.to_datetime(df.timestamp, unit='ms')))

    fig = px.line(fig_df.query('timestamp > "2020-09-01"'),
                  x='timestamp',
                  y='price',
                  title='Historical Filecoin price in USD',
                  labels={'timestamp': 'Timestamp',
                          'price': 'FIL / USD'})
    return fig


def reward_vesting_per_day(connection):
    query = """
    SELECT ce.circulating_fil::NUMERIC / 1e18 AS circulating_fil,
            ce.vested_fil::NUMERIC / 1e18 AS vested_fil,
            ce.mined_fil::NUMERIC / 1e18 AS mined_fil,
            ce.burnt_fil::NUMERIC / 1e18 AS burnt_fil,
            ce.locked_fil::NUMERIC / 1e18 AS locked_fil,
            sectors.activation_epoch,
            SUM(sectors.initial_pledge::NUMERIC / 1e18) as new_total_initial_pledge,
            COUNT(sectors.sector_id) * 32 as new_gb_added,
            AVG(sectors.initial_pledge::NUMERIC / 1e18) as average_new_sector_initial_pledge,
            AVG(sectors.expected_day_reward::NUMERIC / 1e18) as average_exepected_day_reward,
            MIN(b.timestamp) AS timestamp
    FROM chain_economics ce
    LEFT JOIN block_headers b ON b.parent_state_root = ce.parent_state_root
    LEFT join sector_info sectors on sectors.state_root = ce.parent_state_root 
    GROUP BY ce.parent_state_root, sectors.activation_epoch
    ORDER by timestamp ASC
    """

    df = (pd.read_sql(query, connection)
            .assign(timestamp=lambda df: pd.to_datetime(df.timestamp, unit='s'))
            .set_index('timestamp')
            .sort_index()
          )

    daily_ip = (df.new_total_initial_pledge
                .resample('1d')
                .sum())

    daily_gb = (df.new_gb_added
                .resample('1d')
                .sum())

    daily_df = (df.resample('1d').mean()
                .assign(new_mined_fil=lambda df: df.mined_fil.diff().fillna(0))
                .assign(new_total_initial_pledge=daily_ip)
                .assign(new_gb_added=daily_gb)
                )

    VESTING_PERIOD = 180
    new_miner_vested_fil = (daily_df.new_mined_fil
                                    .rolling(VESTING_PERIOD, min_periods=1)
                                    .apply(lambda x: sum(x / VESTING_PERIOD)))

    daily_df = (daily_df.assign(new_miner_vested_fil=new_miner_vested_fil)
                .assign(total_miner_vested_fil=lambda df: df.new_miner_vested_fil.cumsum())
                .assign(vested_fil_per_gb=lambda df: df.new_miner_vested_fil))

    if len(daily_df) > 0:
        fig = px.line(daily_df,
                      x=daily_df.index,
                      y=daily_df.new_miner_vested_fil,
                      title=r"new_miner_vested / new_ip, daily",
                      log_y=True)
    else:
        fig = None
    return fig


def absolute_qa_power_distribution(connection):
    QUERY = """
        SELECT 
        AVG(total_qa_bytes_power::numeric) * 2^(-50) AS total_power,
        AVG(total_qa_bytes_committed::numeric) * 2^(-50) as total_committed,
        AVG(qa_smoothed_position_estimate::numeric) * 2^(-128) * 2^(-50) AS position_estimate,
        MIN(bh.timestamp) AS time
        FROM chain_powers cp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cp.state_root
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
            """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='QA Power distribution',
                  labels={'value': 'Filwatts',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def network_RB_power_distribution(connection):
    QUERY = """
        SELECT 
        avg(total_raw_bytes_power::numeric) * 2^(-50) AS total_power,
        avg(total_raw_bytes_committed::numeric) * 2^(-50) AS total_committed,
        min(bh.timestamp) AS time
        FROM chain_powers cp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cp.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='RB Power distribution',
                  labels={'value': 'Bytes',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def relative_qa_power_distribution(connection):
    QUERY = """
        SELECT 
        AVG(total_qa_bytes_committed::numeric / total_qa_bytes_power::numeric) as total_committed,
        AVG(qa_smoothed_position_estimate::numeric * 2^(-128) / total_qa_bytes_power::numeric) AS position_estimate,
        MIN(bh.timestamp) AS time
        FROM chain_powers cp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cp.state_root
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
            """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='QA Power distribution rel. to the realized power)',
                  labels={'value': '/% QA Power',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def qa_power_velocity_estimate(connection):
    QUERY = """
        SELECT 
        AVG(cp.qa_smoothed_velocity_estimate::numeric * 2^(-128) * 2^(-50)) AS velocity_estimate,
        MIN(bh.timestamp) AS time
        FROM chain_powers cp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cp.state_root
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
            """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='QA Power Velocity Estimate',
                  labels={'value': 'Filwatts / Epoch',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def per_epoch_reward_actual(connection):

    QUERY = """
       SELECT
        AVG((cr.new_reward::numeric * 1e-18)) as Per_Epoch_Reward_Actual,
        MIN(bh.timestamp) AS time
        FROM chain_rewards cr
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cr.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        ORDER BY time ASC
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='Per Epoch Reward Actual',
                  labels={'value': 'FIL',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def per_epoch_reward_estimate(connection):

    QUERY = """
        SELECT
        avg((cr.new_reward_smoothed_position_estimate::numeric * 2^(-128) * 1e-18)) as Per_Epoch_Reward_Position_Estimate,
        min(bh.timestamp) AS time
        FROM chain_rewards cr
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cr.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        ORDER BY time ASC
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='Per Epoch Reward Position Estimate',
                  labels={'value': 'FIL',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig


def per_epoch_reward_velocity_estimate(connection):
    QUERY = """
        SELECT
        avg((cr.new_reward_smoothed_velocity_estimate::numeric * 2^(-128) * 1e-18)) as Per_Epoch_Reward_Velocity_Estimate,
        min(bh.timestamp) AS time
        FROM chain_rewards cr
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cr.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        ORDER BY time ASC
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                  x='time',
                  y='value',
                  color='variable',
                  title='Per Epoch Reward Velocity Estimate',
                  labels={'value': 'FIL / epoch',
                          'time': 'Timestamp',
                          'variable': 'Metric'})
    return fig

# TODO


def upcoming_sector_expiration_by_epoch(connection):
    QUERY = """
        SELECT 
        COUNT(info.expiration_epoch) AS Upcoming_Sector_Expiration,
        MIN(bh.timestamp) AS time
        FROM miner_sector_infos as info
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = info.state_root
        WHERE
        to_timestamp(info.expiration_epoch) > Now()
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )

    fig_df = df
    if len(fig_df) > 0:
        fig = px.line(fig_df,
                      x='time',
                      y='Upcoming_Sector_Expiration',
                      title='Upcoming Sector Expiration',
                      labels={'value': 'Sectors',
                              'time': 'Timestamp'})
    else:
        fig = None
    return fig


def number_of_deals_made(connection):
    QUERY = """
        SELECT 
        COUNT(deal_id) as number_of_deals_made,
        MIN(bh.timestamp) AS date
        FROM market_deal_states as info
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = info.state_root
        WHERE
        info.last_update_epoch > 0
        GROUP BY date_trunc('day',  to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection).sort_values('date'))

    df['number_of_deals_made_cumulated'] = df.number_of_deals_made.cumsum()

    fig = px.line(df,
                  x='date',
                  y=['number_of_deals_made', 'number_of_deals_made_cumulated'],
                  title='Number of Deals Made',
                  labels={'value': 'Number of Deals',
                          'date': 'Timestamp'})
    return fig


def number_of_terminated_deals(connection):
    QUERY = """
        SELECT 
        COUNT(deal_id) as number_of_terminated_deals,
        MIN(bh.timestamp) AS date
        FROM market_deal_states as info
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = info.state_root
        WHERE
        info.slash_epoch > 0
        GROUP BY date_trunc('day',  to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection).sort_values('date'))

    df['number_of_terminated_deals_cumulated'] = df.number_of_terminated_deals.cumsum()

    if len(df) > 0:
        fig = px.line(df,
                      x='date',
                      y=['number_of_terminated_deals',
                          'number_of_terminated_deals_cumulated'],
                      title='Number of Terminated Deals',
                      labels={'value': 'Number of Terminated Deals',
                              'date': 'Timestamp'})
    else:
        fig = None
    return fig


def verified_client_deals_proportion(connection):
    QUERY = """
        SELECT
        COUNT(mdp.is_verified) filter (where mdp.is_verified::BOOLEAN) / COUNT(mdp.deal_id) AS verified_fraction,
        MIN(to_timestamp(bh.timestamp)) AS time
        FROM market_deal_proposals as mdp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = mdp.state_root
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection))

    if len(df) == 0:
        return None
    else:
        fig = px.line(df,
                      x='time',
                      y='verified_fraction',
                      title='Fraction of Verified Deals')
        return fig

def initial_storage_pledge_per_32gib(connection):
    QUERY = """
        WITH estimate AS (
                SELECT
                (cr.new_reward_smoothed_position_estimate::float
                + 20 * (24 * 60 * 2) * new_reward_smoothed_velocity_estimate::float)
                / (2^(128) * 1e18 * 2^(-35) * cp.total_qa_bytes_power::float) AS projection,
                cr.state_root AS state_root
                FROM chain_rewards cr
                LEFT JOIN chain_powers cp
                ON cp.state_root = cr.state_root
        )
        SELECT
        AVG(est.projection) AS value,
        to_timestamp(MIN(bh.timestamp)) AS time
        FROM estimate est
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = est.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )
    VIZ_PARAMS = {'title': 'Initial Storage Pledge per 32 GiB of QA power',
                  'labels': {'value': 'FIL / (32 GiB QA Power)'}}
    fig = simple_time_series(df, VIZ_PARAMS)
    return fig


def projection_of_the_fault_fee_per_unit_of_qa_power(connection):
    QUERY = """
        WITH estimate AS (
                SELECT
                (cr.new_reward_smoothed_position_estimate::float
                + 2.14 * (24 * 60 * 2) * new_reward_smoothed_velocity_estimate::float)
                / (2^(128) * 1e18 * 2^(-50) * cp.total_qa_bytes_power::float) AS projection,
                cr.state_root AS state_root
                FROM chain_rewards cr
                LEFT JOIN chain_powers cp
                ON cp.state_root = cr.state_root
        )
        SELECT
        AVG(est.projection) AS value,
        to_timestamp(MIN(bh.timestamp)) AS time
        FROM estimate est
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = est.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        """
    df = (pd.read_sql(QUERY, connection)
          .assign(time=lambda df: pd.to_datetime(df.time, unit='s'))
          )
    VIZ_PARAMS = {'title': 'Fault Fee per unit of QA power',
                  'labels': {'value': 'FIL / Filwatts'}}
    return simple_time_series(df, VIZ_PARAMS)


def time_measure(f):
    t1 = time()
    out = f()
    t2 = time()
    print(f"{f.__name__} execution time: {t2 - t1 :.1f}s")
    return out


def time_measure_with_conn(f, connection):
    t1 = time()
    out = f(connection)
    t2 = time()
    print(f"{f.__name__} execution time: {t2 - t1 :.1f}s")
    return out

# %%


FIGURES_FUNCTIONS = [
    relative_token_distribution, 
    absolute_token_distribution,
    fil_price, 
    network_RB_power_distribution, 
    absolute_qa_power_distribution, 
    relative_qa_power_distribution, 
    qa_power_velocity_estimate,
    per_epoch_reward_actual,  
    per_epoch_reward_estimate, 
    per_epoch_reward_velocity_estimate, 
    upcoming_sector_expiration_by_epoch,  
    number_of_deals_made, 
    verified_client_deals_proportion,  
    number_of_terminated_deals,
    reward_vesting_per_day, 
    initial_storage_pledge_per_32gib, 
    projection_of_the_fault_fee_per_unit_of_qa_power
]

connection = create_engine(conn_string, pool_recycle=3600).connect()

# Visualizations to be show on the Dash App, order-sensitive.
FIGURES = [time_measure_with_conn(f, connection) for f in FIGURES_FUNCTIONS]

# %%

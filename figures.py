# Dependences
import json
import requests as req
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

# Prepare SQL connection string to be used on the functions
CONN_STRING_PATH = 'config/sentinel-conn-string.txt'

with open(CONN_STRING_PATH, 'r') as fid:
    conn_string = fid.read()

# Visualizations

def relative_token_distribution():
    connection = create_engine(conn_string, pool_recycle=3600).connect()
    QUERY = """
            with td as (
                SELECT 
                ce.circulating_fil::NUMERIC / 1e18 AS circulating_fil,
                ce.vested_fil::NUMERIC / 1e18 AS vested_fil,
                ce.mined_fil::NUMERIC / 1e18 AS mined_fil,
                ce.burnt_fil::NUMERIC / 1e18 AS burnt_fil,
                ce.locked_fil::NUMERIC / 1e18 AS locked_fil,
                min(to_timestamp(b.timestamp)) AS timestamp
                FROM chain_economics ce
                LEFT JOIN blocks b 
                ON b.parentstateroot = ce.parent_state_root
                GROUP BY ce.parent_state_root
                ORDER BY timestamp asc
            ), s as (
            select 
                (td.circulating_fil + 
                td.vested_fil + 
                td.mined_fil + 
                td.burnt_fil + 
                td.locked_fil)
                as supply_fil,
                td.timestamp as timestamp
                from td
            )
            select 
            avg(td.circulating_fil / s.supply_fil) as fil_circulating_fraction,
            avg(td.vested_fil / s.supply_fil) as fil_vested_fraction,
            avg(td.mined_fil / s.supply_fil) as fil_mined_fraction,
            avg(td.burnt_fil / s.supply_fil) as fil_burnt_fraction,
            avg(td.locked_fil / s.supply_fil) as fil_locked_fraction,
            min(td.timestamp) as timestamp
            from td
            join s on s.timestamp = td.timestamp
            group by date_trunc('hour', td.timestamp)
            order by timestamp
            
            """

    df = (pd.read_sql(QUERY, connection)
            )

    fig_df = df.melt(id_vars=['timestamp'])
    fig = px.line(fig_df,
                x='timestamp',
                y='value',
                color='variable',
                title='Relative token distribution',
                labels={'value': '% of FIL supply',
                        'timestamp': 'Timestamp',
                        'variable': 'Token status'})

    return fig


def absolute_token_distribution():
    connection = create_engine(conn_string, pool_recycle=3600).connect()

    QUERY = """
            SELECT 
            avg(ce.circulating_fil::NUMERIC) / 1e18 AS circulating_fil,
            avg(ce.vested_fil::NUMERIC) / 1e18 AS vested_fil,
            avg(ce.mined_fil::NUMERIC) / 1e18 AS mined_fil,
            avg(ce.burnt_fil::NUMERIC) / 1e18 AS burnt_fil,
            avg(ce.locked_fil::NUMERIC) / 1e18 AS locked_fil,
            min(to_timestamp(b.timestamp)) AS time
            FROM chain_economics ce
            LEFT JOIN blocks b 
            ON b.parentstateroot = ce.parent_state_root
            GROUP BY ce.parent_state_root
            ORDER BY time asc
            """

    df = (pd.read_sql(QUERY, connection)
            .assign(time=lambda df: pd.to_datetime(df.time, unit='ms'))
            )

    fig_df = df.melt(id_vars=['time'])
    fig = px.line(fig_df,
                x='time',
                y='value',
                color='variable',
                title='Absolute token distribution',
                labels={'value': 'FIL',
                        'time': 'Timestamp',
                        'variable': 'Token status'})
    return fig


def fil_price():
    r = req.get('https://api.coingecko.com/api/v3/coins/filecoin/market_chart?vs_currency=usd&days=max')
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


def reward_vesting_per_day():
    connection = create_engine(conn_string, pool_recycle=3600).connect()

    QUERY = f"""
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
    LEFT JOIN blocks b ON b.parentstateroot = ce.parent_state_root
    LEFT join sector_info sectors on sectors.state_root = ce.parent_state_root 
    GROUP BY ce.parent_state_root, sectors.activation_epoch
    order by timestamp ASC
    """

    df = (pd.read_sql(QUERY, connection)
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

    fig = px.line(daily_df,
                x=daily_df.index,
                y=daily_df.new_miner_vested_fil,
                title=r"new_miner_vested / new_ip, daily",
                log_y=True)
    return fig

# Visualizations to be show on the Dash App, order-sensitive.
FIGURES = [#relative_token_distribution(),
           #absolute_token_distribution(),
           fil_price(),
           #reward_vesting_per_day()
           ]
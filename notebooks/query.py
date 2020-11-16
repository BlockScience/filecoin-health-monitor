# %% [markdown]
# # FIL sentinel query
#
# Notebook for performing quick queries

# %%
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import requests as req
import json
from IPython import get_ipython
get_ipython().run_line_magic('load_ext', 'autotime')


# %%

pd.options.plotting.backend = "plotly"

CONN_STRING_PATH = '../config/sentinel-conn-string.txt'

with open(CONN_STRING_PATH, 'r') as fid:
    conn_string = fid.read()

# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        SELECT
        COUNT(mdp.is_verified) filter (where mdp.is_verified::BOOLEAN) / COUNT(mdp.deal_id) AS verified_fraction,
        MIN(bh.timestamp) AS time
        FROM market_deal_proposals as mdp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = mdp.state_root
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
        """

df = (pd.read_sql(QUERY, connection)
      )

print(df.head(10))

# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        SELECT 
        COUNT(deal_id) as Number_of_deals_made,
        to_timestamp(MIN(bh.timestamp)) AS Date
        FROM market_deal_states as info
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = info.state_root
        WHERE
        info.last_update_epoch > 0
        GROUP BY date_trunc('hour',  to_timestamp(bh.timestamp))
        """

df = (pd.read_sql(QUERY, connection)
      )

print(df.shape)

px.box(df, x='time', y='projection')

# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        SELECT 
        COUNT(deal_id) as number_of_deals_made,
        to_timestamp(MIN(bh.timestamp)) AS date
        FROM market_deal_states as info
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = info.state_root
        WHERE
        info.last_update_epoch > 0
        GROUP BY date_trunc('day',  to_timestamp(bh.timestamp))
        """

df = (pd.read_sql(QUERY, connection)
      )

print(df.shape)
px.line(df,
        x='date',
        y='number_of_deals_made')

# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        SELECT
        cr.new_reward_smoothed_position_estimate::float as position,
        2.14 * (24 * 60 * 2) * new_reward_smoothed_velocity_estimate::float AS fee,
        bh.timestamp AS time
        FROM chain_rewards cr
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cr.state_root
        LEFT JOIN chain_powers cp
        ON cp.state_root = bh.parent_state_root
        """

df = (pd.read_sql(QUERY, connection)
      )
# %%

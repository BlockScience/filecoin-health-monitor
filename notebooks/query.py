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
        (cr.new_reward_smoothed_position_estimate::float
        + 2.14 * (24 * 60 * 2) * new_reward_smoothed_velocity_estimate::float)
         / (2^128 * cp.total_qa_bytes_power::float) AS projection,
        bh.timestamp AS time
        FROM chain_rewards cr
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cr.state_root
        LEFT JOIN chain_powers cp
        ON cp.state_root = bh.parent_state_root
        """

df = (pd.read_sql(QUERY, connection)
      )

print(df.shape)

px.box(df, x='time', y='projection')

# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        WITH estimate AS (
                SELECT
                (cr.new_reward_smoothed_position_estimate::float
                + 2.14 * (24 * 60 * 2) * new_reward_smoothed_velocity_estimate::float)
                / (2^(128) * cp.total_qa_bytes_power::float) AS projection,
                cr.state_root AS state_root
                FROM chain_rewards cr
                LEFT JOIN chain_powers cp
                ON cp.state_root = cr.state_root
        )
        SELECT
        AVG(est.projection) AS projection,
        to_timestamp(MIN(bh.timestamp)) AS time
        FROM estimate est
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = est.state_root
        GROUP BY date_trunc('hour', to_timestamp(bh.timestamp))
        """

df = (pd.read_sql(QUERY, connection)
      )

print(df.shape)
px.line(df,
        x='time',
        y='projection_avg',
        error_y='projection_std')

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

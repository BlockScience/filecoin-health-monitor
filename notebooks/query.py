# %% [markdown]
# # FIL sentinel query
# 
# Notebook for performing quick queries

# %%
from IPython import get_ipython
get_ipython().run_line_magic('load_ext', 'autotime')


# %%
import json
import requests as req
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

pd.options.plotting.backend = "plotly"

CONN_STRING_PATH = '../config/sentinel-conn-string.txt'

with open(CONN_STRING_PATH, 'r') as fid:
    conn_string = fid.read()


# %%
connection = create_engine(conn_string, pool_recycle=3600).connect()
QUERY = """
        SELECT 
        total_qa_bytes_power::numeric * 2^(-50) AS qaNP,
        total_qa_bytes_committed::numeric * 2^(-50) as qaNP_committed,
        qa_smoothed_position_estimate::numeric * 2^(-128) * 2^(-50) AS qaNP_estimate,
        bh.timestamp
        FROM chain_powers cp
        LEFT JOIN block_headers bh
        ON bh.parent_state_root = cp.state_root
        ORDER BY bh.timestamp ASC
        limit 1000
        """

df = (pd.read_sql(QUERY, connection)
        )

df.head(2)
# %%

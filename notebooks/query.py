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
        avg(ce.circulating_fil::NUMERIC) / 1e18 AS circulating_fil,
        avg(ce.vested_fil::NUMERIC) / 1e18 AS vested_fil,
        avg(ce.mined_fil::NUMERIC) / 1e18 AS mined_fil,
        avg(ce.burnt_fil::NUMERIC) / 1e18 AS burnt_fil,
        avg(ce.locked_fil::NUMERIC) / 1e18 AS locked_fil,
        min(to_timestamp(b.timestamp)) AS time
        FROM chain_economics ce
        LEFT JOIN block_headers b 
        ON b.parent_state_root = ce.parent_state_root
        GROUP BY ce.parent_state_root
        ORDER BY time ASC
        LIMIT 100
        """

df = (pd.read_sql(QUERY, connection)
        )

df.head(2)
# %%

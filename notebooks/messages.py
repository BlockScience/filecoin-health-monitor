# %%

from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import requests as req
import json
from IPython import get_ipython
get_ipython().run_line_magic('load_ext', 'autotime')
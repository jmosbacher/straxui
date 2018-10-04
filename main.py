from os.path import dirname, join

import pandas as pd
from datetime import date
from random import randint
from concurrent.futures import ThreadPoolExecutor
from bokeh.io import curdoc
from bokeh.layouts import row, column, widgetbox
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import PreText, Select, Button, TextInput, DataTable, DateFormatter, TableColumn, Tabs, Panel, NumberEditor
from bokeh.plotting import figure
from bokeh.document import without_document_lock
from tornado import gen
from straxrpc.client import StraxClient
from functools import partial
from pages import page_classes
import json
import numpy as np
from collections import defaultdict

strax = StraxClient('localhost:50051')
try:
    dataframe_names = strax.search_dataframe_names("*")
except:
    dataframe_names = ['event_basics']

doc = curdoc()
executor = ThreadPoolExecutor(max_workers=2)

with open(join(dirname(__file__), "data","plot_templates.json"), "rb") as f:
    plot_templates = {t["name"]:t for t in json.load(f)}
random_src = {"x":90*np.random.rand(100), "y": np.random.rand(100), 
        "time":  10.*np.random.rand(100), "length":800.*np.random.rand(100),
        "xs":[90*np.random.rand((10)) for _ in range(100)], "ys": [np.random.rand(10) for _ in range(100)]}
sources = defaultdict(list)
sources["__random__"].append(random_src)

shared_state = {
    "executor": executor, 
    "doc": doc,
    "dataframe_names": dataframe_names,
    "strax_ctx": strax,
    "plot_templates": plot_templates,
    "sources": sources,
}


pages = []
for klass in page_classes:
    page = klass(shared_state)
    pages.append(page)

def update_pages():
    for p in pages:
        p.update()

# tabs = Tabs(tabs=[explore_panel,load_data_panel, plot_data_panel, rpc_server_details])
panels = [Panel(child=page.create_page(), title=page.title) for page in pages]
tabs = Tabs(tabs=panels)
doc.add_periodic_callback(update_pages, 2000)
doc.add_root(tabs)
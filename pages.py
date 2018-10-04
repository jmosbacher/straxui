import pandas as pd
from datetime import date
from random import randint
from concurrent.futures import ThreadPoolExecutor
from bokeh.io import curdoc
from bokeh.layouts import row, column, widgetbox
from bokeh.models import ColumnDataSource, CustomJS
from bokeh.models.widgets import PreText, Select, Button, TextInput, DataTable, DateFormatter, TableColumn, Tabs, Panel, NumberEditor, Slider
from bokeh.plotting import figure
from bokeh.palettes import Spectral5, Plasma256
from bokeh.document import without_document_lock
from tornado import gen
# from straxrpc.client import StraxClient
from functools import partial
from copy import copy
import json
import numpy as np
import time

class Page:
    """
    Base class for a page on the main app.
    accepts a shared state dict from the app.
    All blocking code must be done using the executor.
    """
    title = "Page"

    def __init__(self, shared_state: dict, width=1200):
        self.shared_state = shared_state
        self.width = width
        self.init()

    def init(self):
        '''
        Called once at initialization after shared state is loaded
        '''
        pass

    def create_page(self):
        '''
        This method is called after initialization to build the document.
        Must return a valid bokeh layout object (row/column)
        '''
        raise NotImplementedError

    def update(self):
        '''
        This method is called every time there is a change in the shared state
        '''
        pass

class ExplorePage(Page):
    """

    """
    title = "Explore"

    def init(self):
        self.pattern_result_display = PreText(text='No Matches to show.', width=600, height=200)
        self.pattern_selector = TextInput(value="", title="Search Pattern:", width=600, height=60)
        dataframe_names = self.shared_state.get("dataframe_names")
        self.dataframe_selector = Select(title="Dataframe", value="", options=dataframe_names)
        self.df_title = PreText(text="", width=600, height=20)
        df_column_names = ["Field name", "Data type", "Comment",]
        self.df_source = ColumnDataSource({name:[] for name in df_column_names})
        columns = [TableColumn(field=name, title=name) for name in df_column_names]
        self.df_table = DataTable(source=self.df_source, columns=columns, width=600, height=300, editable=False)

    def build_pattern_search(self):
        def pattern_changed(attr, old, new):
            ctx = self.shared_state.get("strax_ctx")
            matches = ctx.search_field(new)
            if matches:
                self.pattern_result_display.text = '\n'.join(matches)
            else:
                self.pattern_result_display.text = 'No Matches to show.'
        
        self.pattern_selector.on_change('value', pattern_changed)
        self.pattern_selector.value = "s1*"
        
        return column(self.pattern_selector, self.pattern_result_display)

    
    def build_data_info(self):
        def dataframe_changed(attr, old, new):
            self.df_title.text = 'Columns for {}:'.format(new)
            ctx = self.shared_state.get("strax_ctx")
            df = ctx.data_info(new)
            self.df_source.data = df.astype("str").to_dict(orient='list')

        self.dataframe_selector.on_change('value', dataframe_changed)
        self.dataframe_selector.value = self.dataframe_selector.options[0]
        return column(self.dataframe_selector, self.df_title, self.df_table)

    def create_page(self):
        pattern_search = self.build_pattern_search()
        data_info = self.build_data_info()
        return row(pattern_search, data_info, width=self.width)

    def update(self):
        self.dataframe_selector.options = self.shared_state.get("dataframe_names")

class LoadDataPage(Page):
    """

    """
    title = "Load Tables"
    def init(self):
        self.dataframe_names = self.shared_state.get('dataframe_names')
        self.run_id_selector = Select(title="Run ID:", value="170621_0617", options=["170621_0617", "180423_1021"], width=200)
        # self.load_dataframe_selector = Select(title="Dataframe", value="", options=self.dataframe_names)
        self.dataframe_selector = Select(title="Dataframe", value="", options=self.dataframe_names, width=200)
        if len(self.dataframe_names):
            self.dataframe_selector.value = self.dataframe_names[0]
        self.load_df_button = Button(label="Load", button_type="primary", width=150)
        self.download_df_button = Button(label="Download table as CSV", button_type="primary", width=150)
        self.download_df_button.disabled = True

        df_column_names = ['column 1', 'column 2', 'columns 3']
        self.df_source = ColumnDataSource({name:[] for name in df_column_names})
        columns = [TableColumn(field=name, title=name) for name in df_column_names]
        self.df_table = DataTable(source=self.df_source, columns=columns, width=1200, height=400, editable=False)
        self.next_button = Button(label="Next >>", button_type="primary", width=50, disabled = True)
        self.back_button = Button(label="<< Prev", button_type="primary", width=50, disabled = True)
        self.current_position = Slider(start=0, end=2, value=0, step=1, title="Chunk", disabled=True, width=200)
        self.current_name = ""

    def build_selection_bar(self):
        def disable_button():
            self.load_df_button.label = "Working..."
            self.load_df_button.disabled = True

        def enable_button():
            self.load_df_button.disabled = False
            self.load_df_button.label = "Load"

        def switch_table_source(name, idx):
            try:
                srcs = self.shared_state['sources'][name]
                self.current_name = name
                new = idx%len(srcs)
                self.current_position.end = len(srcs)
                data = srcs[new]
                self.current_position.value = new
                self.df_source.data = data
                if new:
                    self.back_button.disabled = False
                else:
                    self.back_button.disabled = True
                if new<len(srcs):
                    self.next_button.disabled = False
                    self.current_position.disabled = False
                else:
                    self.next_button.disabled = True
                    self.current_position.disabled = True
                
                #self.df_table.columns = [TableColumn(field=n, title=n) for n in data]
            except:
                print("failed to get data")
            finally:
                
                enable_button()

        def save_source(name, data):
            #data = {k: [] for k in keys} #df.to_dict(orient='list')
            # name = "{}_{}".format(self.dataframe_selector.value, self.run_id_selector.value)
            self.shared_state['sources'][name].append(data)

        def reset_source(keys):
            data = {k: [] for k in keys}
            self.df_table.columns = [TableColumn(field=n, title=n) for n in keys]
            self.df_source.data = data
            self.current_position.value = 0
            

        def stream_df(doc, ctx, run_id, dfname):
            try:
                name = "{}_{}".format(dfname, run_id)
                for i, df in enumerate(ctx.get_df_iter(run_id, dfname)):
                    df = df.dropna(how="any")
                    # df = df.fillna(-999)
                    data = df.to_dict(orient="list")
                    doc.add_next_tick_callback(partial(save_source, name, data))
                    if not i:
                        doc.add_next_tick_callback(partial(switch_table_source,name, 0))

            except Exception as e:
                print(e)
            finally:
                doc.add_next_tick_callback(enable_button)
            
        def stream_array(doc, ctx, run_id, dfname):
            try:
                name = "{}_{}".format(dfname, run_id)
                for i, arr in enumerate(ctx.get_array_iter(run_id, dfname)):
                    data = {n: arr[n].tolist() for n in arr.dtype.names}
                    doc.add_next_tick_callback(partial(save_source, name, data))
                    if not i:
                        doc.add_next_tick_callback(partial(switch_table_source,name, 0))
           
            except Exception as e:
                print(e)
            finally:
                doc.add_next_tick_callback(enable_button)

        @gen.coroutine
        @without_document_lock
        def load_dataframe_pressed():
            doc = self.shared_state.get('doc')
            executor = self.shared_state.get('executor')
            ctx = self.shared_state.get("strax_ctx")
            dfname = self.dataframe_selector.value
            run_id = self.run_id_selector.value
            info = ctx.data_info(dfname)
            name = "{}_{}".format(dfname, run_id)
            doc.add_next_tick_callback(partial(reset_source, list(info["Field name"])))
            if name in self.shared_state['sources']:
                doc.add_next_tick_callback(partial(switch_table_source, name))
            else:
                yield executor.submit(stream_array, doc, ctx, run_id, dfname)
            # else:
            #     yield executor.submit(stream_df, doc, ctx, run_id, dfname)
    
        self.load_df_button.on_click(load_dataframe_pressed)
        self.load_df_button.on_click(disable_button)

        def next_pressed():
            switch_table_source(self.current_name, self.current_position.value+1)
        self.next_button.on_click(next_pressed)
        def back_pressed():
            switch_table_source(self.current_name, self.current_position.value-1)
        self.back_button.on_click(back_pressed)

        selectors = row(widgetbox(self.dataframe_selector), widgetbox(self.run_id_selector),widgetbox(self.load_df_button),widgetbox(self.download_df_button), width=1200)
        buttons = row( widgetbox(self.back_button),
        widgetbox( self.current_position), widgetbox(self.next_button),  width=1000)
        
        
        return column(selectors, buttons, width=1200)

    def build_table(self):
        return widgetbox(self.df_table)
        
    def create_page(self):
        selection_bar = self.build_selection_bar()
        table = self.build_table()
        return column(selection_bar, table, width=self.width)

    def update(self):
        pass

class PlotColumnsPage(Page):
    
    plot_options = {
        "tools":'wheel_zoom,save,pan,box_zoom,tap,box_select,lasso_select,reset',
        'width': 600,
        'height': 500,
    }
    selection_options = [
       #(Title, name, catagories)
        ("X Axis", "x", None),
        ("Y Axis", "y", None),
        ("Size", "size", list(np.arange(1, 10, 0.5))),
        ("Color", "color", Plasma256),
        ("Opacity", "alpha", list(np.arange(0, 1, 0.05)) ),
    ]
    title = "Plot Columns"
    sources = {}

    def init(self):
        
        self.plot_template_selector = Select(title="Plot Templat", value="", options=[])
        self.src_selector = Select(title="Source", value="", options=[])
        self.source = ColumnDataSource()
        self.current_position = Slider(start=0, end=2, value=0, step=1, title="Chunk", disabled=True, width=200)
        self.current_name = ""
        #Select(title="Chunk", value=0, options=[0], width=30)
        self.next_button = Button(label="Next >>", button_type="primary", width=50, disabled = True)
        self.back_button = Button(label="<< Prev", button_type="primary", width=50, disabled = True)
        # self.run_id_selector = Select(title="Run ID", value="", options=[])
        self.column_selectors = [Select(title=s[0], value="", options=[]) for s in self.selection_options]
        self.plot_button = Button(label="Plot", button_type="primary", width=150)
        
        self.update()
    
    def numeric_columns(self, name):
        # FIXME: optimize, dont create a dataframe every time
        srcs = self.shared_state["sources"][name]
        if not srcs:
            return []
        data = srcs[0]
        
        return [k for k in data.keys() if np.isscalar(data[k][0])]

    def build_selection_bar(self):
        def source_changed(attr, old, new):
            #FIXME implement caching of options and choices
            srcs = self.shared_state["sources"][new]
            self.current_position.end = len(srcs)
            columns = self.numeric_columns(new)
            # print(columns)
            for i, selector in enumerate(self.column_selectors):
                selector.options = ["None"] + columns
                if selector.value in columns:
                    pass
                elif self.selection_options[i][1] in columns:
                    selector.value = self.selection_options[i][1]
                else:
                    selector.value = "None"
                # FIXME: Make this error proof

        self.src_selector.on_change("value", source_changed)
        self.src_selector.value = "__random__"

        def refresh_plot():
            self.plot_layout.children[0] = self.build_plot()
        self.plot_button.on_click(refresh_plot)
        data_loading = column(widgetbox(self.plot_template_selector, self.src_selector))
        plot_options = column(*[widgetbox(s) for s in self.column_selectors])
        return column(data_loading, plot_options, widgetbox(self.plot_button))

    def build_plot(self):
        if self.plot_template_selector.value:
            template_name = self.plot_template_selector.value
        else:
            template_name = self.plot_template_selector.options[0]
        template = self.shared_state["plot_templates"][template_name]

        fig = figure(**template["figure"])
        srcs = self.shared_state["sources"][self.src_selector.value]
        idx = self.current_position.value
        if idx<len(srcs) and srcs:
            data = srcs[idx]
        else:
            data = srcs["__random__"][0]
        self.source.data = data
        df = self.source.to_df()
        
        options = dict(x="x", y="y", size=1, color="blue", alpha=0.5)
        for (title, name, cats), selector in zip(self.selection_options, self.column_selectors):
            if selector.value in df.columns:
                if cats is None:
                    pass
                else:
                    if len(set(df[selector.value])) > len(cats):
                        groups = pd.qcut(df[selector.value].values, len(cats), duplicates='drop')
                    else:
                        groups = pd.Categorical(df[selector.value])
                    vals = [cats[xx] for xx in groups.codes]
                    self.source.data[name] = vals
                options[name] = selector.value
        fig.circle(**options, source=self.source)

        
        def switch_table_source(name, idx):
            try:
                srcs = self.shared_state['sources'][name]
                self.current_name = name
                new = idx%len(srcs)
                self.current_position.end = len(srcs)
                data = srcs[new]
                self.current_position.value = new
                self.df_source.data = data
                if new:
                    self.back_button.disabled = False
                else:
                    self.back_button.disabled = True
                if new<len(srcs):
                    self.next_button.disabled = False
                    self.current_position.disabled = False
                else:
                    self.next_button.disabled = True
                    self.current_position.disabled = True
                #self.df_table.columns = [TableColumn(field=n, title=n) for n in data]
            except:
                print("failed to get data")
            
        def next_pressed():
            switch_table_source(self.current_name, self.current_position.value+1)
        self.next_button.on_click(next_pressed)

        def back_pressed():
            switch_table_source(self.current_name, self.current_position.value-1)
        self.back_button.on_click(back_pressed)
        buttons = row( widgetbox(self.back_button),
            widgetbox( self.current_position), widgetbox(self.next_button), )

        return column(buttons, fig)

    def create_page(self):
        selection_bar = self.build_selection_bar()
        fig = self.build_plot()
        self.plot_layout = column(fig)
        return row(selection_bar, self.plot_layout, width=self.width)
      
    def update(self):
        self.src_selector.options = list(self.shared_state["sources"].keys())
        self.plot_template_selector.options = [t for t in self.shared_state["plot_templates"]]
        # self.df_selector.value = self.shared_state["dataframe_names"][0]

class StraxServerPage(Page):
    title = "Strax Settings"

    def init(self):
        self.address_selector = TextInput(value="localhost:50051", title="Strax server:", width=200, height=60)
        dataframe_names = self.shared_state.get('dataframe_names')
        self.strax_config_dataframe = Select(title="Load config for", value="", options=dataframe_names)
        strax_config_column_names = ['option', 'default', 'current', 'applies_to', 'help']
        self.strax_config_source = ColumnDataSource({x:[] for x in strax_config_column_names})
        strax_config_columns = [TableColumn(field=name, title=name) for name in strax_config_column_names]
        self.strax_config_table = DataTable(source=self.strax_config_source, columns=strax_config_columns, width=1000, height=400)

    def create_page(self):
        def address_changed(attr, old, new):
            try:
                ctx = self.shared_state.get('strax_ctx')
                ctx.addr = self.address_selector.value
            except:
                self.address_selector.value = old
        self.address_selector.on_change('value', address_changed)

        def dataframe_changed(attr, old, new):
            ctx = self.shared_state.get('strax_ctx')
            df = ctx.show_config(new)
            data = df.to_dict(orient='list')
            self.strax_config_table.columns = [TableColumn(field=name, title=name) for name in data]
            self.strax_config_source.data = data
        self.strax_config_dataframe.on_change('value', dataframe_changed)
        self.strax_config_dataframe.value = self.strax_config_dataframe.options[0]
        return column(widgetbox(self.address_selector), widgetbox(self.strax_config_dataframe), widgetbox(self.strax_config_table), width=self.width)

    def update(self):
        pass

class PlotTemplatesPage(Page):
    title = 'Plot Templates'

    def init(self):
        self.plot_templates = self.shared_state["plot_templates"]
        # self.json_viewer = TextInput(value=self.text, title="Templates", width=1000, height=600)
        self.template_selector = Select(title=None, value="", options=[t for t in self.plot_templates],height=60, width=200)
        self.build_plot_button = Button(label="Build plot", button_type="primary", disabled=True,height=60, width=180)
        self.json_viewer = PreText(text="Template values: \n", width=700, height=500)
        # self.json_viewer.js_on_change("value", cb)
        self.json_viewer.disabled = False
        
    def create_page(self):
        def template_changed(attr, old, new):
            text = "Template values: \n"
            if new in self.plot_templates:
                text += json.dumps(self.plot_templates[new], sort_keys=True, indent=4)
            self.json_viewer.text = text
        self.template_selector.on_change("value", template_changed)
        self.template_selector.value = self.template_selector.options[0]
        selection = row(widgetbox(self.template_selector), widgetbox(self.build_plot_button))
        return column(selection ,widgetbox(self.json_viewer, width=self.width), width=self.width)

    def update(self):
        self.template_selector.options = [t for t in self.shared_state["plot_templates"]]
        

page_classes = [ExplorePage, LoadDataPage, PlotColumnsPage, StraxServerPage, PlotTemplatesPage]
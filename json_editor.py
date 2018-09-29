from bokeh.core.properties import Float, Instance, Tuple, Bool, Enum, String, Any
from bokeh.models import InputWidget
from bokeh.models.callbacks import Callback
from bokeh.core.enums import SliderCallbackPolicy

from bokeh.layouts import column
from bokeh.models import Slider, CustomJS, ColumnDataSource
from bokeh.io import show
from bokeh.plotting import Figure


class JsonEditor(InputWidget):
    # The special class attribute ``__implementation__`` should contain a string
    # of JavaScript (or CoffeeScript) code that implements the JavaScript side
    # of the custom extension model or a string name of a JavaScript (or
    # CoffeeScript) file with the implementation.

    __implementation__ = 'json_editor.ts'
    __javascript__ = ["straxui/static/jsoneditor.min.js"]
    __css__ = ["straxui/static/jsoneditor.min.css"]

    # Below are all the "properties" for this model. Bokeh properties are
    # class attributes that define the fields (and their types) that can be
    # communicated automatically between Python and the browser. Properties
    # also support type validation. More information about properties in
    # can be found here:
    #
    #    https://bokeh.pydata.org/en/latest/docs/reference/core.html#bokeh-core-properties

    disable = Bool(default=True, help="""
    Enable or disable the editor.
    """)

    json = Any(default={}, help="""
    The actual json string to edit.
    """)

    

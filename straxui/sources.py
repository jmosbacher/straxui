from bokeh.models import ColumnDataSource
import time
from threading import Timer

class StreamSource:
    def __init__(self):
        self.source = ColumnDataSource()
        self.max_rows = 100
        self.interval = 1

    def stream(self):
        self.timer = Timer(self.interval, self.next)
    
    def reset_source(self):
        pass

    def next(self):
        pass
import {div, empty} from "core/dom"
import * as p from "core/properties"
// import {LayoutDOM, LayoutDOMView} from "models/layouts/layout_dom"
import {InputWidget, InputWidgetView} from "models/widgets/input_widget"
export class JsonEditorView extends InputWidgetView {

  initialize(options) {
    super.initialize(options)

    this.render()

    // Set BokehJS listener so that when the Bokeh slider has a change
    // event, we can process the new data
    this.connect(this.model.change, () => this.render())
  }

  render() {
    // BokehjS Views create <div> elements by default, accessible as
    // ``this.el``. Many Bokeh views ignore this default <div>, and instead
    // do things like draw to the HTML canvas. In this case though, we change
    // the contents of the <div>, based on the current slider value.
    empty(this.el)
    var json = this.model.json
    // container = document.getElementById("jsoneditor")
    var options = {mode: 'view'}
    var container = div({class: "jsoneditor-parent", id:"jsoneditor",})
    var editor = new JSONEditor(container, options)
    editor.set(json)
    this.el.appendChild(container)
    
  }
}

export class JsonEditor extends InputWidget {

  // If there is an associated view, this is typically boilerplate.
  default_view = JsonEditorView

  // The ``type`` class attribute should generally match exactly the name
  // of the corresponding Python class.
  type = "JsonEditor"
}

// The @define block adds corresponding "properties" to the JS model. These
// should normally line up 1-1 with the Python model class. Most property
// types have counterparts, e.g. bokeh.core.properties.String will be
// ``p.String`` in the JS implementation. Any time the JS type system is not
// yet as complete, you can use ``p.Any`` as a "wildcard" property type.
JsonEditor.define({
  json:   [ p.Any ],
})
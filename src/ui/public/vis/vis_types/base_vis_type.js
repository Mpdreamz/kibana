import { VisSchemasProvider } from '../schemas';
import { CATEGORY } from '../vis_category';
import _ from 'lodash';


export function VisTypeProvider(Private) {
  const VisTypeSchemas = Private(VisSchemasProvider);

  class VisType {
    constructor(opts) {
      opts = opts || {};

      const _defaults = {
        // name, title, description, icon, image
        category: CATEGORY.OTHER,
        visualization: null,       // must be a class with render/resize/destroy methods
        visConfig: {
          defaults: {},            // default configuration
        },
        requestHandler: 'courier',    // select one from registry or pass a function
        responseHandler: 'none',      // ...
        editor: 'default',
        editorConfig: {
          //optionTabs: {},          // default editor needs a list of option tabs
          optionsTemplate: '',      // default editor needs an optionsTemplate if optionsTab is not provided
          collections: {},         // collections used for configuration (list of positions, ...)
        },
        options: {                // controls the visualize editor
          showTimePicker: true,
          showQueryBar: true,
          showFilterBar: true,
          hierarchicalData: false  // we should get rid of this i guess ?
        },
        schemas: new VisTypeSchemas(),            // default editor needs a list of schemas ...
        isExperimental: false
      };

      _.defaultsDeep(this, opts, _defaults);

      if (!this.name) throw('vis_type must define its name');
      if (!this.title) throw('vis_type must define its title');
      if (!this.description) throw('vis_type must define its description');
      if (!this.icon && !this.image) throw('vis_type must define its icon or image');

      if (!this.editorConfig.optionTabs) {
        this.editorConfig.optionTabs = [
          { name: 'options', title: 'Options', editor: this.editorConfig.optionsTemplate }
        ];
      }

      this.requiresSearch = !(this.requestHandler === 'none');
    }

    render(vis, $el, uiState, esResponse) {
      if (!this.visualization) {
        throw new Error('vis_type render function not implemented');
      }
      this.visualization.render(vis, $el, uiState, esResponse);
    }

    destroy() {
      this.visualization.destroy();
    }
  }

  return VisType;
}
import { Tag } from '@markdoc/markdoc';
import { Tabs } from '../../components/markdoc/Tabs';
import { Tab } from '../../components/markdoc/Tab';

export const tabs = {
  render: Tabs,
  attributes: {
    group: {
      type: String
    },
    hideTabs: {
      type: Boolean
    },
    hideContent: {
      type: Boolean
    }
  },
  transform(node, config) {
    const attributes = node.transformAttributes(config);
    const labels = node
      .transformChildren(config)
      .filter((child) => child && child.name === 'Tab')
      .map((tab) => (typeof tab === 'object' ? tab.attributes.label : null));

    return new Tag(this.render, { ...attributes, labels }, node.transformChildren(config));
  }
};

export const tab = {
  render: Tab,
  attributes: {
    label: {
      type: String
    },
  }
};

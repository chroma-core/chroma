import { Tag } from '@markdoc/markdoc';
import { CodeTabs } from '../../components/markdoc/CodeTabs';
import { CodeTab } from '../../components/markdoc/CodeTab';
// import { CodeBlock } from '../../components/CodeBlock';

export const codetabs = {
  render: CodeTabs,
  attributes: {
    group: {
      type: String
    },
    customHeader: {
      type: String
    }
  },
  transform(node, config) {
    const attributes = node.transformAttributes(config);
    const labels = node
      .transformChildren(config)
      .filter((child) => child && child.name === 'Codetab')
      .map((tab) => (typeof tab === 'object' ? tab.attributes.label : null));

    return new Tag(this.render, { ...attributes, labels }, node.transformChildren(config));
  }
};

export const codetab = {
  render: CodeTab,
  attributes: {
    label: {
      type: String
    },
  }
};

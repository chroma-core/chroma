import {Tag, nodes} from '@markdoc/markdoc';
import {CodeBlock} from '../../components/CodeBlock';

export const fence = {
  render: CodeBlock,
  attributes: {
    filename: {
      type: String,
    },
    codetab: {
      type: Boolean
    },
    ...nodes.fence.attributes,
  },
  transform(node, config) {
    const attributes = node.transformAttributes(config);
    const {language} = node.children[0].attributes;
    const content = node.attributes.content;
    return new Tag(this.render, {...attributes, language}, [content]);
  },
};

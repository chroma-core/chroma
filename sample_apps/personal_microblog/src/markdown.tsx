export function remarkCustom() {
  /**
   * @param {Root} tree
   * @return {undefined}
   */
  function visit(tree: { children: any; type: string; value: string }) {
    if (tree.type == "code") {
      tree.value = tree.value.trim();
      return tree;
    }
    if (tree.type == "paragraph" && tree.children.length > 0) {
      tree.children = tree.children.flatMap((node: any) => {
        if (node.type == "text") {
          return replaceMentions(node.value);
        } else {
          return node;
        }
      });
    } else if (tree.children) {
      tree.children = tree.children.map((node: any) => visit(node));
    }
    return tree;
  }
  return visit;
}

function replaceMentions(body: string): any[] {
  const parts: any[] = [];
  const mentionRegex = /(@\w+)/g;
  let lastIndex = 0;

  if (!body || body.length === 0) {
    return [];
  }

  if (!mentionRegex.test(body)) {
    return [{ type: "text", value: body }];
  }

  body.replace(mentionRegex, (match, mention, offset) => {
    // Push text before the mention
    if (lastIndex < offset) {
      const content = body.slice(lastIndex, offset);
      parts.push({
        type: "text",
        value: content,
      });
    }
    // Push the mention
    parts.push({
      type: "strong",
      children: [{ type: "text", value: mention }],
    });
    lastIndex = offset + match.length;
    return "";
  });

  // Push any remaining text
  if (lastIndex < body.length) {
    const content = body.slice(lastIndex);
    parts.push({
      type: "text",
      value: content,
    });
  }
  return parts;
}

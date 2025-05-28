import { JSX } from "react";
import { PostModel } from "./types";
import { Mention } from "./components/markup";

function groupPostsByMonthAndYear(
  posts: PostModel[]
): { month: string; posts: PostModel[] }[] {
  const groupedPosts: { [key: string]: PostModel[] } = {};
  posts.forEach((post) => {
    const date = new Date(post.date);
    const monthYear = `${date.getMonth()}-${date.getFullYear()}`;
    if (!groupedPosts[monthYear]) {
      groupedPosts[monthYear] = [];
    }
    groupedPosts[monthYear].push(post);
  });

  const orderedPostGroups = Object.keys(groupedPosts).map((monthYear) => ({
    month: monthYear,
    posts: groupedPosts[monthYear],
  }));

  orderedPostGroups.sort((a, b) => a.month.localeCompare(b.month));

  return orderedPostGroups;
}

export function remarkMentions() {
  /**
   * @param {Root} tree
   * @return {undefined}
   */
  function visit(tree) {
    console.log(tree);
    if (tree.type == "code") {
      tree.value = tree.value.trim();
      console.log(tree.value);
      return tree;
    }
    if (tree.type == "paragraph" && tree.children.length > 0) {
      tree.children = tree.children.flatMap((node) => {
        if (node.type == "text") {
          return replaceMentions(node.value);
        } else {
          return node;
        }
      });
    } else if (tree.children) {
      tree.children = tree.children.map((node) => visit(node));
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

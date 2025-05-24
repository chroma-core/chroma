const TagPath = require("./TagPath");

class TagPathMatcher{
  constructor(stack,node){
    this.stack = stack;
    this.node= node;
  }

  match(path){
    const tagPath = new TagPath(path);
    return tagPath.match(this.stack, this.node);
  }
}

module.exports = TagPathMatcher;
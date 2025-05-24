// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

const INLINE_FUNC_DEF_REGEX = /@inline[\s\n\r]+(\w+)[\s\n\r]+([0-9a-zA-Z_]+)\s*\(([^)]*)\)\s*{(([^}]|[\n\r])*)}/gm;
const FUNC_CALL_REGEX = '(\\w+)?\\s+([_0-9a-zA-Z]+)\\s+=\\s+__FUNC__\\((.*)\\)\\s*;';
/**
 * GLSL preprocessor responsible for resolving @inline directives
 */
export function replaceInlines(script: string): string {
  const inlineDefs: { [name: string]: { params: Array<{ type: string; name: string } | null>; body: string } } = {};
  let match;
  while ((match = INLINE_FUNC_DEF_REGEX.exec(script)) !== null) {
    const params = match[3]
      .split(',')
      .map((s) => {
        const tokens = s.trim().split(' ');
        if (tokens && tokens.length === 2) {
          return { type: tokens[0], name: tokens[1] };
        }
        return null;
      })
      .filter((v) => v !== null);
    inlineDefs[match[2]] = { params, body: match[4] };
  }
  for (const name in inlineDefs) {
    const regexString = FUNC_CALL_REGEX.replace('__FUNC__', name);
    const regex = new RegExp(regexString, 'gm');
    while ((match = regex.exec(script)) !== null) {
      const type = match[1];
      const variable = match[2];
      const params = match[3].split(',');
      const declLine = type ? `${type} ${variable};` : '';
      let newBody: string = inlineDefs[name].body;
      let paramRedecLine = '';
      inlineDefs[name].params.forEach((v, i) => {
        if (v) {
          paramRedecLine += `${v.type} ${v.name} = ${params[i]};\n`;
        }
      });
      newBody = `${paramRedecLine}\n ${newBody}`;
      newBody = newBody.replace('return', `${variable} = `);
      const replacement = `
      ${declLine}
      {
        ${newBody}
      }
      `;
      script = script.replace(match[0], replacement);
    }
  }
  script = script.replace(INLINE_FUNC_DEF_REGEX, '');
  return script;
}

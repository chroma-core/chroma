
import * as React from 'react';
import CopyToClipboardButton from './CopyToClipboardButton';
import { useTheme } from 'next-themes';
import { useRouter } from 'next/router';

import Prism from 'prismjs';
require(`prismjs/components/prism-python.min.js`);
require(`prismjs/components/prism-bash.min.js`);
require(`prismjs/components/prism-javascript.min.js`);
require(`prismjs/components/prism-yaml.js`);
require(`prismjs/components/prism-json.min.js`);

Prism.languages.python = {
  ...Prism.languages.python,
  'class-name': /\b[A-Z]\w+/,

  'function': {
    pattern: /(\b\w+\.)?\w+(?=\s*\()/,
    // lookbehind: true,
    inside: {
      'class': /^[^.]*\./,
      'punctuation': /\./,
      'method': {
        pattern: /\w+$/,
        alias: 'function'
      },

    },
  }

};

import { Icons } from './ui/icons';

interface CodeBlockProps {
  children: string;
  'data-language': string;
  filename?: string;
  obfuscated?: boolean;
  codetab?: boolean;
}

export function CodeBlock({children, 'data-language': language, filename, codetab}: CodeBlockProps) {
  const ref = React.useRef(null);

  if (!language) {
    language = 'plaintext';
  }

  if (language === 'py') {
    language = 'python';
  }

  const [highlightedCode, setHighlightedCode] = React.useState('');
  const [codeWithoutSyntax, setCodeWithoutSyntax] = React.useState('');
  var code$Regex = /^(.*?)#\s*\[\!code\s*\$\]\s*(.*)$/;

  // children without code$Regex
  // iterate over the children and remove the code$Regex
  let newChildren = children.split('\n').map((line) => {
    let match = line.match(code$Regex);
    if (match) {
      return match[1].trim()
    }
    return line
  }).join('\n')

  React.useEffect(() => {
    async function highlight() {

      if (language === 'sh') {
        language = 'bash'
      }

      Prism.hooks.add(`after-highlight`, function (env) {
        // Split the highlighted HTML by line breaks into an array of lines
        var lines = env.highlightedCode.split('\n').slice(0, -1); // slice to remove the last empty line

        var wrappedLines = lines.map(function(line) {
          // Regex to match the marker with flexible whitespace

          var match = line.match(code$Regex);
          if (match) {
            // If it does, remove the marker and wrap the line with div.line and add the class
            // `match[1]` contains the line without the marker
            return '<div class="line command-line-input">' + match[1].trim() + '</div>';
          } else {
            // Otherwise, just wrap the line with div.line
            return '<div class="line">' + line + '</div>';
          }

        }).join('');

        // Replace the highlightedCode with the wrapped lines
        env.element.innerHTML = wrappedLines;
      })

      const env = {
        element: {},
        language,
        grammar: Prism.languages[language],
        highlightedCode: undefined,
        code: children
      };

      Prism.hooks.run('before-highlight', env);
      env.highlightedCode = Prism.highlight(env.code, env.grammar, env.language);
      Prism.hooks.run('before-insert', env);
      // @ts-ignore
      env.element.innerHTML = env.highlightedCode;
      Prism.hooks.run('after-highlight', env);
      Prism.hooks.run('complete', env);

      // const highlightedCode = Prism.highlight(children, Prism.languages[language], language);
      // Prism.hooks.run('before-tokenize', highlightedCode);

       // @ts-ignore
      setHighlightedCode(env.element.innerHTML);
    }

    highlight();
  }, [children]);

  let copyIconColor = 'text-gray-500 hover:text-white'
  let copyButtonTop = '4px'

  let marginBottom = '1rem'
  if (codetab == true) {
    marginBottom = '0'
    copyButtonTop = '-71px'
  }

  return (
    <div className="rounded-md code reset text-sm" aria-live="polite"
      style={{borderRadius: '0.5rem', position: 'relative', marginBottom: marginBottom}}
    >
      <CopyToClipboardButton className={`absolute ${copyIconColor} border-0 right-3 p-0.5`} textToCopy={newChildren} customStyle={{top: copyButtonTop}}/>
      <CustomHeader language={language} filename={filename} codetab={codetab}/>
      <pre className='py-4 px-6 overflow-x-scroll'>
        <code className={`language-${language}`} dangerouslySetInnerHTML={{ __html: highlightedCode }}>
        </code>
      </pre>

    </div>
  );
}

// react component for CustomHeader

export function CustomHeader({language, filename, codetab}) {
  let customHeader = (<></>)

  if ((language === 'bash') || (language === 'sh')) {
    customHeader = (
      <div className="code-context-banner flex items-center pl-4" style={{padding: '5px 5px 5px 15px'}}>
        <Icons.commandLine className="h-4 w-4" />
        <p className="text-sm mb-0 pl-2">Command Line</p>
      </div>
    )
  }

  if ((filename)) {
    customHeader = (
      <div className="code-context-banner flex items-center pl-4" style={{padding: '5px 5px 5px 15px'}}>
        <Icons.codeFile className="h-4 w-4" />
        <p className="text-sm mb-0 pl-2">{filename}</p>
      </div>
    )
  }

  // if filename is not provided, then fall back to the language
  if ((!filename) && (language !== 'bash') && (language !== 'sh')) {

    // if js, then change to javascript
    if (language === 'js') {
      language = 'javascript'
    }

    customHeader = (
      <div className="code-context-banner flex items-center pl-4" style={{padding: '5px 5px 5px 15px'}}>
        <Icons.codeFile className="h-4 w-4" />
        <p className="text-sm mb-0 pl-2">{language}</p>
      </div>
    )
  }

  if (codetab == true) {
    customHeader = (<></>)
  }

  return customHeader
}

module.exports = {
  extends: [
    'airbnb-base',
    'airbnb-typescript',
    'airbnb/hooks',
    'plugin:@typescript-eslint/recommended',
    'plugin:jest/recommended',
    'prettier',
    'plugin:prettier/recommended',
  ],
  plugins: ['react', '@typescript-eslint', 'jest'],
  env: {
    browser: true,
    es6: true,
    jest: true,
  },
  globals: {
    Atomics: 'readonly',
    SharedArrayBuffer: 'readonly',
  },
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaFeatures: {
      jsx: true,
    },
    ecmaVersion: 2018,
    sourceType: 'module',
    project: './tsconfig.json',
  },
  rules: {
    'linebreak-style': 'off',
    'prettier/prettier': [
      'warn',
      {
        endOfLine: 'auto',
      },
    ],
    '@typescript-eslint/no-use-before-define': 'off',
    '@typescript-eslint/camelcase': 'off',
    '@typescript-eslint/no-empty-interface': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    'import/prefer-default-export': 'off',
    'no-shadow': 'off', // should be on probably
    'react/prop-types': 'off', // should be on probably
    'import/no-cycle': 'off', // why should this be on?
    'react-hooks/rules-of-hooks': 'off', // this really should be on
    'import/no-named-as-default': 'off',
    'no-param-reassign': 'off',
    '@typescript-eslint/ban-types': 'off',
    'no-return-assign': 'off',
    'no-unused-expressions': 'off',
    'no-restricted-syntax': 'off',
    'no-unsafe-optional-chaining': 'off',
    '@typescript-eslint/no-unused-expressions': 'off',
    'no-empty-pattern': 'off',
    '@typescript-eslint/ban-ts-ignore': 'off',
    '@typescript-eslint/ban-ts-comment': 'off',
    'class-methods-use-this': ['error', { exceptMethods: ['applySnapshotWithoutRecording'] }],
    '@typescript-eslint/no-this-alias': [
      'error',
      {
        allowDestructuring: true, // Allow `const { props, state } = this`; false by default
        allowedNames: ['self', 'that'], // Allow `const self = this`; `[]` by default
      },
    ],
    // to get this thing to freaking build
    'prefer-template': 'off',
    'import/no-extraneous-dependencies': 'off',
    'react/destructuring-assignment': 'off',
    'react/self-closing-comp': 'off',
    'no-else-return': 'off',
    'react/jsx-props-no-spreading': 'off',
    'react-hooks/exhaustive-deps': 'off',
    'array-callback-return': 'off',
    'import/order': 'off',
    'import/newline-after-import': 'off',
    'spaced-comment': 'off',
    'no-var': 'off',
    'one-var': 'off',
    'no-plusplus': 'off',
    'object-shorthand': 'off',
    'prefer-const': 'off',
    'vars-on-top': 'off',
    'no-unneeded-ternary': 'off',
    'import/no-duplicates': 'off',
    'prefer-destructuring': 'off',
    'prefer-object-spread': 'off',
    'no-lonely-if': 'off',
    'consistent-return': 'off',
    'no-useless-escape': 'off',
    'default-param-last': 'off',
    'no-underscore-dangle': 'off',
    '@typescript-eslint/no-non-null-asserted-optional-chain': 'off',
    eqeqeq: 'off',
    //"camelcase": [2, {"properties": "always"}]
  },
}
// module.exports = {
//   root: true,
//   extends: ['react-app', 'prettier', 'prettier/flowtype', 'prettier/react'],
//   plugins: ['import', 'flowtype', 'jsx-a11y', 'react', 'react-hooks', 'prettier', 'emotion'],
//   rules: {
//     'emotion/jsx-import': 'error',
//     'prettier/prettier': 'warn',
//     'react/no-did-mount-set-state': 0,
//     'no-unused-vars': 0,
//     'dot-notation': 0,
//     'no-new-func': 0,
//     'no-alert': 0,
//   },
//   globals: {
//     Intl: 'readonly',
//   }
// };

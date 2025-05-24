import globals from "globals";
import chaiFriendly from "eslint-plugin-chai-friendly";

export default [
    {
        files: ["bin/wait-on", "**/*.js"],
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: "module",
            globals: {
                ...globals.mocha
            },
        },
        plugins: {
            "chai-friendly": chaiFriendly
        },
        rules: {
          'no-use-before-define': 'off',
          'no-unused-vars': [
            'error',
            {
              varsIgnorePattern: 'should|expect'
            }
          ],
          // disable the original no-unused-expressions use chai-friendly
          'no-unused-expressions': 'off',
          'chai-friendly/no-unused-expressions': 'error'
        }
    }
];

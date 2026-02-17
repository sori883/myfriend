// @ts-check
import eslintJs from "@eslint/js";
import configPrettier from "eslint-config-prettier";

import pluginCdk from "eslint-plugin-awscdk";
import pluginTypeScript from "typescript-eslint";
import pluginEslintN from "eslint-plugin-n";

const config = [
  // base
  {
    ignores: [
      "cdk.out",
      "node_modules",
      "*.js",
      ".prettierrc.mjs",
      "eslint.config.mjs",
      "build"
    ],
  },

  // TypeScript
  {
    name: "eslint/recommended",
    rules: eslintJs.configs.recommended.rules,
  },
  ...pluginTypeScript.configs.recommended,

  // CDK
  {
    name: "prettier/config",
    ...pluginCdk.configs.recommended,
  },
  
  // Prettier
  {
    name: "prettier/config",
    ...configPrettier,
  },

  // カスタムルール
  {
    name: "project-custom",
    plugins: {
      n: pluginEslintN,
    },
    rules: {
      "quotes": ["error", "single"],
      "n/no-process-env": "error",
      "semi": ["error", "always"],
      "@typescript-eslint/consistent-type-imports": [
        "warn",
        { prefer: "type-imports", fixStyle: "separate-type-imports" },
      ],
    },
  },

  // 特定のファイルだけprocess.envを許可する
  {
    name: "allow-process-env-in-validate-env",
    files: ["parameter/validate-dotenv.ts"],
    rules: {
      "n/no-process-env": "off",
    },
  },
];

export default config;
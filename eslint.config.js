import { defineConfig, globalIgnores } from "eslint/config";
import globals from "globals";
import tseslint from "typescript-eslint";

export default defineConfig([
  { files: ["**/*.{ts}"] },
  { files: ["**/*.{ts}"], languageOptions: { globals: globals.node } },
  tseslint.configs.recommended,
  globalIgnores(["dist"]),
]);

import type {ESLint, Linter} from 'eslint';

export = formatterPretty;

/**
Pretty formatter for [ESLint](https://eslint.org).

@param results - Lint result for the individual files.
@param data - Extended information related to the analysis results.
@returns The formatted output.
*/
declare function formatterPretty(
	results: readonly formatterPretty.LintResult[],
	data?: ESLint.LintResultData
): string;

declare namespace formatterPretty {
	interface LintResult {
		readonly filePath: string;
		readonly errorCount: number;
		readonly warningCount: number;
		readonly messages: readonly LintMessage[];
	}

	type Severity = Linter.Severity | 'warning' | 'error';

	interface LintMessage {
		readonly severity: Severity;
		readonly fatal?: boolean;
		readonly line?: number;
		readonly column?: number;
		readonly message: string;
		readonly ruleId?: string | null;
	}
}

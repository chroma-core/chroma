/**
 * Represents tokens that our language understands in parsing.
 */
export const TOKEN_TYPES = Object.freeze({
	Text: "Text", // The text between Jinja statements or expressions

	NumericLiteral: "NumericLiteral", // e.g., 123
	BooleanLiteral: "BooleanLiteral", // true or false
	NullLiteral: "NullLiteral", // none
	StringLiteral: "StringLiteral", // 'string'
	Identifier: "Identifier", // Variables, functions, etc.
	Equals: "Equals", // =
	OpenParen: "OpenParen", // (
	CloseParen: "CloseParen", // )
	OpenStatement: "OpenStatement", // {%
	CloseStatement: "CloseStatement", // %}
	OpenExpression: "OpenExpression", // {{
	CloseExpression: "CloseExpression", // }}
	OpenSquareBracket: "OpenSquareBracket", // [
	CloseSquareBracket: "CloseSquareBracket", // ]
	OpenCurlyBracket: "OpenCurlyBracket", // {
	CloseCurlyBracket: "CloseCurlyBracket", // }
	Comma: "Comma", // ,
	Dot: "Dot", // .
	Colon: "Colon", // :
	Pipe: "Pipe", // |

	CallOperator: "CallOperator", // ()
	AdditiveBinaryOperator: "AdditiveBinaryOperator", // + -
	MultiplicativeBinaryOperator: "MultiplicativeBinaryOperator", // * / %
	ComparisonBinaryOperator: "ComparisonBinaryOperator", // < > <= >= == !=
	UnaryOperator: "UnaryOperator", // ! - +

	// Keywords
	Set: "Set",
	If: "If",
	For: "For",
	In: "In",
	Is: "Is",
	NotIn: "NotIn",
	Else: "Else",
	EndSet: "EndSet",
	EndIf: "EndIf",
	ElseIf: "ElseIf",
	EndFor: "EndFor",
	And: "And",
	Or: "Or",
	Not: "UnaryOperator",
	Macro: "Macro",
	EndMacro: "EndMacro",
	Break: "Break",
	Continue: "Continue",
});

export type TokenType = keyof typeof TOKEN_TYPES;

/**
 * Constant lookup for keywords and known identifiers + symbols.
 */
const KEYWORDS = Object.freeze({
	set: TOKEN_TYPES.Set,
	for: TOKEN_TYPES.For,
	in: TOKEN_TYPES.In,
	is: TOKEN_TYPES.Is,
	if: TOKEN_TYPES.If,
	else: TOKEN_TYPES.Else,
	endset: TOKEN_TYPES.EndSet,
	endif: TOKEN_TYPES.EndIf,
	elif: TOKEN_TYPES.ElseIf,
	endfor: TOKEN_TYPES.EndFor,
	and: TOKEN_TYPES.And,
	or: TOKEN_TYPES.Or,
	not: TOKEN_TYPES.Not,
	"not in": TOKEN_TYPES.NotIn,
	macro: TOKEN_TYPES.Macro,
	endmacro: TOKEN_TYPES.EndMacro,
	break: TOKEN_TYPES.Break,
	continue: TOKEN_TYPES.Continue,

	// Literals
	true: TOKEN_TYPES.BooleanLiteral,
	false: TOKEN_TYPES.BooleanLiteral,
	none: TOKEN_TYPES.NullLiteral,

	// NOTE: According to the Jinja docs: The special constants true, false, and none are indeed lowercase.
	// Because that caused confusion in the past, (True used to expand to an undefined variable that was considered false),
	// all three can now also be written in title case (True, False, and None). However, for consistency, (all Jinja identifiers are lowercase)
	// you should use the lowercase versions.
	True: TOKEN_TYPES.BooleanLiteral,
	False: TOKEN_TYPES.BooleanLiteral,
	None: TOKEN_TYPES.NullLiteral,
});

/**
 * Represents a single token in the template.
 */
export class Token {
	/**
	 * Constructs a new Token.
	 * @param {string} value The raw value as seen inside the source code.
	 * @param {TokenType} type The type of token.
	 */
	constructor(
		public value: string,
		public type: TokenType
	) {}
}

function isWord(char: string): boolean {
	return /\w/.test(char);
}

function isInteger(char: string): boolean {
	return /[0-9]/.test(char);
}

/**
 * A data structure which contains a list of rules to test
 */
const ORDERED_MAPPING_TABLE: [string, TokenType][] = [
	// Control sequences
	["{%", TOKEN_TYPES.OpenStatement],
	["%}", TOKEN_TYPES.CloseStatement],
	["{{", TOKEN_TYPES.OpenExpression],
	["}}", TOKEN_TYPES.CloseExpression],
	// Single character tokens
	["(", TOKEN_TYPES.OpenParen],
	[")", TOKEN_TYPES.CloseParen],
	["{", TOKEN_TYPES.OpenCurlyBracket],
	["}", TOKEN_TYPES.CloseCurlyBracket],
	["[", TOKEN_TYPES.OpenSquareBracket],
	["]", TOKEN_TYPES.CloseSquareBracket],
	[",", TOKEN_TYPES.Comma],
	[".", TOKEN_TYPES.Dot],
	[":", TOKEN_TYPES.Colon],
	["|", TOKEN_TYPES.Pipe],
	// Comparison operators
	["<=", TOKEN_TYPES.ComparisonBinaryOperator],
	[">=", TOKEN_TYPES.ComparisonBinaryOperator],
	["==", TOKEN_TYPES.ComparisonBinaryOperator],
	["!=", TOKEN_TYPES.ComparisonBinaryOperator],
	["<", TOKEN_TYPES.ComparisonBinaryOperator],
	[">", TOKEN_TYPES.ComparisonBinaryOperator],
	// Arithmetic operators
	["+", TOKEN_TYPES.AdditiveBinaryOperator],
	["-", TOKEN_TYPES.AdditiveBinaryOperator],
	["*", TOKEN_TYPES.MultiplicativeBinaryOperator],
	["/", TOKEN_TYPES.MultiplicativeBinaryOperator],
	["%", TOKEN_TYPES.MultiplicativeBinaryOperator],
	// Assignment operator
	["=", TOKEN_TYPES.Equals],
];

const ESCAPE_CHARACTERS = new Map([
	["n", "\n"], // New line
	["t", "\t"], // Horizontal tab
	["r", "\r"], // Carriage return
	["b", "\b"], // Backspace
	["f", "\f"], // Form feed
	["v", "\v"], // Vertical tab
	["'", "'"], // Single quote
	['"', '"'], // Double quote
	["\\", "\\"], // Backslash
]);

export interface PreprocessOptions {
	trim_blocks?: boolean;
	lstrip_blocks?: boolean;
}

function preprocess(template: string, options: PreprocessOptions = {}): string {
	// According to https://jinja.palletsprojects.com/en/3.0.x/templates/#whitespace-control

	// In the default configuration:
	//  - a single trailing newline is stripped if present
	//  - other whitespace (spaces, tabs, newlines etc.) is returned unchanged
	if (template.endsWith("\n")) {
		template = template.slice(0, -1);
	}

	// Replace all comments with a placeholder
	// This ensures that comments don't interfere with the following options
	template = template.replace(/{#.*?#}/gs, "{##}");

	if (options.lstrip_blocks) {
		// The lstrip_blocks option can also be set to strip tabs and spaces from the
		// beginning of a line to the start of a block. (Nothing will be stripped if
		// there are other characters before the start of the block.)
		template = template.replace(/^[ \t]*({[#%])/gm, "$1");
	}

	if (options.trim_blocks) {
		// If an application configures Jinja to trim_blocks, the first newline after
		// a template tag is removed automatically (like in PHP).
		template = template.replace(/([#%]})\n/g, "$1");
	}

	return template
		.replace(/{##}/g, "") // Remove comments
		.replace(/-%}\s*/g, "%}")
		.replace(/\s*{%-/g, "{%")
		.replace(/-}}\s*/g, "}}")
		.replace(/\s*{{-/g, "{{");
}

/**
 * Generate a list of tokens from a source string.
 */
export function tokenize(source: string, options: PreprocessOptions = {}): Token[] {
	const tokens: Token[] = [];
	const src: string = preprocess(source, options);

	let cursorPosition = 0;

	const consumeWhile = (predicate: (char: string) => boolean): string => {
		let str = "";
		while (predicate(src[cursorPosition])) {
			// Check for escaped characters
			if (src[cursorPosition] === "\\") {
				// Consume the backslash
				++cursorPosition;
				// Check for end of input
				if (cursorPosition >= src.length) throw new SyntaxError("Unexpected end of input");

				// Add the escaped character
				const escaped = src[cursorPosition++];
				const unescaped = ESCAPE_CHARACTERS.get(escaped);
				if (unescaped === undefined) {
					throw new SyntaxError(`Unexpected escaped character: ${escaped}`);
				}
				str += unescaped;
				continue;
			}

			str += src[cursorPosition++];
			if (cursorPosition >= src.length) throw new SyntaxError("Unexpected end of input");
		}
		return str;
	};

	// Build each token until end of input
	main: while (cursorPosition < src.length) {
		// First, consume all text that is outside of a Jinja statement or expression
		const lastTokenType = tokens.at(-1)?.type;
		if (
			lastTokenType === undefined ||
			lastTokenType === TOKEN_TYPES.CloseStatement ||
			lastTokenType === TOKEN_TYPES.CloseExpression
		) {
			let text = "";
			while (
				cursorPosition < src.length &&
				// Keep going until we hit the next Jinja statement or expression
				!(src[cursorPosition] === "{" && (src[cursorPosition + 1] === "%" || src[cursorPosition + 1] === "{"))
			) {
				// Consume text
				text += src[cursorPosition++];
			}

			// There is some text to add
			if (text.length > 0) {
				tokens.push(new Token(text, TOKEN_TYPES.Text));
				continue;
			}
		}

		// Consume (and ignore) all whitespace inside Jinja statements or expressions
		consumeWhile((char) => /\s/.test(char));

		// Handle multi-character tokens
		const char = src[cursorPosition];

		// Check for unary operators
		if (char === "-" || char === "+") {
			const lastTokenType = tokens.at(-1)?.type;
			if (lastTokenType === TOKEN_TYPES.Text || lastTokenType === undefined) {
				throw new SyntaxError(`Unexpected character: ${char}`);
			}
			switch (lastTokenType) {
				case TOKEN_TYPES.Identifier:
				case TOKEN_TYPES.NumericLiteral:
				case TOKEN_TYPES.BooleanLiteral:
				case TOKEN_TYPES.NullLiteral:
				case TOKEN_TYPES.StringLiteral:
				case TOKEN_TYPES.CloseParen:
				case TOKEN_TYPES.CloseSquareBracket:
					// Part of a binary operator
					// a - 1, 1 - 1, true - 1, "apple" - 1, (1) - 1, a[1] - 1
					// Continue parsing normally
					break;

				default: {
					// Is part of a unary operator
					// (-1), [-1], (1 + -1), not -1, -apple
					++cursorPosition; // consume the unary operator

					// Check for numbers following the unary operator
					const num = consumeWhile(isInteger);
					tokens.push(
						new Token(`${char}${num}`, num.length > 0 ? TOKEN_TYPES.NumericLiteral : TOKEN_TYPES.UnaryOperator)
					);
					continue;
				}
			}
		}

		// Try to match one of the tokens in the mapping table
		for (const [char, token] of ORDERED_MAPPING_TABLE) {
			const slice = src.slice(cursorPosition, cursorPosition + char.length);
			if (slice === char) {
				tokens.push(new Token(char, token));
				cursorPosition += char.length;
				continue main;
			}
		}

		if (char === "'" || char === '"') {
			++cursorPosition; // Skip the opening quote
			const str = consumeWhile((c) => c !== char);
			tokens.push(new Token(str, TOKEN_TYPES.StringLiteral));
			++cursorPosition; // Skip the closing quote
			continue;
		}

		if (isInteger(char)) {
			const num = consumeWhile(isInteger);
			tokens.push(new Token(num, TOKEN_TYPES.NumericLiteral));
			continue;
		}
		if (isWord(char)) {
			const word = consumeWhile(isWord);

			// Check for special/reserved keywords
			// NOTE: We use Object.hasOwn() to avoid matching `.toString()` and other Object methods
			const type = Object.hasOwn(KEYWORDS, word) ? KEYWORDS[word as keyof typeof KEYWORDS] : TOKEN_TYPES.Identifier;

			// Special case of not in:
			// If the previous token was a "not", and this token is "in"
			// then we want to combine them into a single token
			if (type === TOKEN_TYPES.In && tokens.at(-1)?.type === TOKEN_TYPES.Not) {
				tokens.pop();
				tokens.push(new Token("not in", TOKEN_TYPES.NotIn));
			} else {
				tokens.push(new Token(word, type));
			}

			continue;
		}

		throw new SyntaxError(`Unexpected character: ${char}`);
	}
	return tokens;
}

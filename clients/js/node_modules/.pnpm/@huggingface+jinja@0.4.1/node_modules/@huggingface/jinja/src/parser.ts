import type { Token, TokenType } from "./lexer";
import { TOKEN_TYPES } from "./lexer";
import type { Statement } from "./ast";
import {
	Program,
	If,
	For,
	Break,
	Continue,
	SetStatement,
	MemberExpression,
	CallExpression,
	Identifier,
	NumericLiteral,
	StringLiteral,
	BooleanLiteral,
	NullLiteral,
	ArrayLiteral,
	ObjectLiteral,
	BinaryExpression,
	FilterExpression,
	TestExpression,
	UnaryExpression,
	SliceExpression,
	KeywordArgumentExpression,
	TupleLiteral,
	Macro,
	SelectExpression,
} from "./ast";

/**
 * Generate the Abstract Syntax Tree (AST) from a list of tokens.
 * Operator precedence can be found here: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_precedence#table
 */
export function parse(tokens: Token[]): Program {
	const program = new Program([]);
	let current = 0;

	/**
	 * Consume the next token if it matches the expected type, otherwise throw an error.
	 * @param type The expected token type
	 * @param error The error message to throw if the token does not match the expected type
	 * @returns The consumed token
	 */
	function expect(type: string, error: string): Token {
		const prev = tokens[current++];
		if (!prev || prev.type !== type) {
			throw new Error(`Parser Error: ${error}. ${prev.type} !== ${type}.`);
		}
		return prev;
	}

	function parseAny(): Statement {
		switch (tokens[current].type) {
			case TOKEN_TYPES.Text:
				return parseText();
			case TOKEN_TYPES.OpenStatement:
				return parseJinjaStatement();
			case TOKEN_TYPES.OpenExpression:
				return parseJinjaExpression();
			default:
				throw new SyntaxError(`Unexpected token type: ${tokens[current].type}`);
		}
	}

	function not(...types: TokenType[]): boolean {
		return current + types.length <= tokens.length && types.some((type, i) => type !== tokens[current + i].type);
	}

	function is(...types: TokenType[]): boolean {
		return current + types.length <= tokens.length && types.every((type, i) => type === tokens[current + i].type);
	}

	function parseText(): StringLiteral {
		return new StringLiteral(expect(TOKEN_TYPES.Text, "Expected text token").value);
	}

	function parseJinjaStatement(): Statement {
		// Consume {% %} tokens
		expect(TOKEN_TYPES.OpenStatement, "Expected opening statement token");

		let result;
		switch (tokens[current].type) {
			case TOKEN_TYPES.Set:
				++current;
				result = parseSetStatement();
				expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");
				break;

			case TOKEN_TYPES.If:
				++current;
				result = parseIfStatement();
				expect(TOKEN_TYPES.OpenStatement, "Expected {% token");
				expect(TOKEN_TYPES.EndIf, "Expected endif token");
				expect(TOKEN_TYPES.CloseStatement, "Expected %} token");
				break;

			case TOKEN_TYPES.Macro:
				++current;
				result = parseMacroStatement();
				expect(TOKEN_TYPES.OpenStatement, "Expected {% token");
				expect(TOKEN_TYPES.EndMacro, "Expected endmacro token");
				expect(TOKEN_TYPES.CloseStatement, "Expected %} token");
				break;

			case TOKEN_TYPES.For:
				++current;
				result = parseForStatement();
				expect(TOKEN_TYPES.OpenStatement, "Expected {% token");
				expect(TOKEN_TYPES.EndFor, "Expected endfor token");
				expect(TOKEN_TYPES.CloseStatement, "Expected %} token");
				break;
			case TOKEN_TYPES.Break:
				++current;
				expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");
				result = new Break();
				break;
			case TOKEN_TYPES.Continue:
				++current;
				expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");
				result = new Continue();
				break;
			default:
				throw new SyntaxError(`Unknown statement type: ${tokens[current].type}`);
		}

		return result;
	}

	function parseJinjaExpression(): Statement {
		// Consume {{ }} tokens
		expect(TOKEN_TYPES.OpenExpression, "Expected opening expression token");

		const result = parseExpression();

		expect(TOKEN_TYPES.CloseExpression, "Expected closing expression token");
		return result;
	}

	// NOTE: `set` acts as both declaration statement and assignment expression
	function parseSetStatement(): Statement {
		const left = parseExpression();

		if (is(TOKEN_TYPES.Equals)) {
			++current;
			const value = parseExpression();

			return new SetStatement(left, value, []);
		} else {
			// parsing multiline set here
			const body: Statement[] = [];
			expect(TOKEN_TYPES.CloseStatement, "Expected %} token");
			while (
				!(tokens[current]?.type === TOKEN_TYPES.OpenStatement && tokens[current + 1]?.type === TOKEN_TYPES.EndSet)
			) {
				const another = parseAny();
				body.push(another);
			}
			expect(TOKEN_TYPES.OpenStatement, "Expected {% token");
			expect(TOKEN_TYPES.EndSet, "Expected endset token");

			return new SetStatement(left, null, body);
		}
	}

	function parseIfStatement(): If {
		const test = parseExpression();

		expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");

		const body: Statement[] = [];
		const alternate: Statement[] = [];

		// Keep parsing if body until we reach the first {% elif %} or {% else %} or {% endif %}
		while (
			!(
				tokens[current]?.type === TOKEN_TYPES.OpenStatement &&
				(tokens[current + 1]?.type === TOKEN_TYPES.ElseIf ||
					tokens[current + 1]?.type === TOKEN_TYPES.Else ||
					tokens[current + 1]?.type === TOKEN_TYPES.EndIf)
			)
		) {
			body.push(parseAny());
		}

		// Alternate branch: Check for {% elif %} or {% else %}
		if (
			tokens[current]?.type === TOKEN_TYPES.OpenStatement &&
			tokens[current + 1]?.type !== TOKEN_TYPES.EndIf // There is some body
		) {
			++current; // eat {% token
			if (is(TOKEN_TYPES.ElseIf)) {
				expect(TOKEN_TYPES.ElseIf, "Expected elseif token");
				alternate.push(parseIfStatement());
			} else {
				// tokens[current]?.type === TokenType.Else
				expect(TOKEN_TYPES.Else, "Expected else token");
				expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");

				// keep going until we hit {% endif %}
				while (
					!(tokens[current]?.type === TOKEN_TYPES.OpenStatement && tokens[current + 1]?.type === TOKEN_TYPES.EndIf)
				) {
					alternate.push(parseAny());
				}
			}
		}

		return new If(test, body, alternate);
	}

	function parseMacroStatement(): Macro {
		const name = parsePrimaryExpression();
		if (name.type !== "Identifier") {
			throw new SyntaxError(`Expected identifier following macro statement`);
		}
		const args = parseArgs();
		expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");

		// Body of macro
		const body: Statement[] = [];

		// Keep going until we hit {% endmacro
		while (not(TOKEN_TYPES.OpenStatement, TOKEN_TYPES.EndMacro)) {
			body.push(parseAny());
		}

		return new Macro(name as Identifier, args, body);
	}

	function parseExpressionSequence(primary = false): Statement {
		const fn = primary ? parsePrimaryExpression : parseExpression;
		const expressions = [fn()];
		const isTuple = is(TOKEN_TYPES.Comma);
		while (isTuple) {
			++current; // consume comma
			expressions.push(fn());
			if (!is(TOKEN_TYPES.Comma)) {
				break;
			}
		}
		return isTuple ? new TupleLiteral(expressions) : expressions[0];
	}

	function parseForStatement(): For {
		// e.g., `message` in `for message in messages`
		const loopVariable = parseExpressionSequence(true); // should be an identifier/tuple
		if (!(loopVariable instanceof Identifier || loopVariable instanceof TupleLiteral)) {
			throw new SyntaxError(`Expected identifier/tuple for the loop variable, got ${loopVariable.type} instead`);
		}

		expect(TOKEN_TYPES.In, "Expected `in` keyword following loop variable");

		// `messages` in `for message in messages`
		const iterable = parseExpression();

		expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");

		// Body of for loop
		const body: Statement[] = [];

		// Keep going until we hit {% endfor or {% else
		while (not(TOKEN_TYPES.OpenStatement, TOKEN_TYPES.EndFor) && not(TOKEN_TYPES.OpenStatement, TOKEN_TYPES.Else)) {
			body.push(parseAny());
		}

		// (Optional) else block
		const alternative: Statement[] = [];
		if (is(TOKEN_TYPES.OpenStatement, TOKEN_TYPES.Else)) {
			++current; // consume {%
			++current; // consume else
			expect(TOKEN_TYPES.CloseStatement, "Expected closing statement token");

			// keep going until we hit {% endfor
			while (not(TOKEN_TYPES.OpenStatement, TOKEN_TYPES.EndFor)) {
				alternative.push(parseAny());
			}
		}

		return new For(loopVariable, iterable, body, alternative);
	}

	function parseExpression(): Statement {
		// Choose parse function with lowest precedence
		return parseIfExpression();
	}

	function parseIfExpression(): Statement {
		const a = parseLogicalOrExpression();
		if (is(TOKEN_TYPES.If)) {
			// Ternary expression
			++current; // consume if
			const predicate = parseLogicalOrExpression();

			if (is(TOKEN_TYPES.Else)) {
				// Ternary expression with else
				++current; // consume else
				const b = parseLogicalOrExpression();
				return new If(predicate, [a], [b]);
			} else {
				// Select expression on iterable
				return new SelectExpression(a, predicate);
			}
		}
		return a;
	}

	function parseLogicalOrExpression(): Statement {
		let left = parseLogicalAndExpression();
		while (is(TOKEN_TYPES.Or)) {
			const operator = tokens[current];
			++current;
			const right = parseLogicalAndExpression();
			left = new BinaryExpression(operator, left, right);
		}
		return left;
	}

	function parseLogicalAndExpression(): Statement {
		let left = parseLogicalNegationExpression();
		while (is(TOKEN_TYPES.And)) {
			const operator = tokens[current];
			++current;
			const right = parseLogicalNegationExpression();
			left = new BinaryExpression(operator, left, right);
		}
		return left;
	}

	function parseLogicalNegationExpression(): Statement {
		let right: UnaryExpression | undefined;

		// Try parse unary operators
		while (is(TOKEN_TYPES.Not)) {
			// not not ...
			const operator = tokens[current];
			++current;
			const arg = parseLogicalNegationExpression(); // not test.x === not (test.x)
			right = new UnaryExpression(operator, arg);
		}

		return right ?? parseComparisonExpression();
	}

	function parseComparisonExpression(): Statement {
		// NOTE: membership has same precedence as comparison
		// e.g., ('a' in 'apple' == 'b' in 'banana') evaluates as ('a' in ('apple' == ('b' in 'banana')))
		let left = parseAdditiveExpression();
		while (is(TOKEN_TYPES.ComparisonBinaryOperator) || is(TOKEN_TYPES.In) || is(TOKEN_TYPES.NotIn)) {
			const operator = tokens[current];
			++current;
			const right = parseAdditiveExpression();
			left = new BinaryExpression(operator, left, right);
		}
		return left;
	}
	function parseAdditiveExpression(): Statement {
		let left = parseMultiplicativeExpression();
		while (is(TOKEN_TYPES.AdditiveBinaryOperator)) {
			const operator = tokens[current];
			++current;
			const right = parseMultiplicativeExpression();
			left = new BinaryExpression(operator, left, right);
		}
		return left;
	}

	function parseCallMemberExpression(): Statement {
		// Handle member expressions recursively

		const member = parseMemberExpression(parsePrimaryExpression()); // foo.x

		if (is(TOKEN_TYPES.OpenParen)) {
			// foo.x()
			return parseCallExpression(member);
		}
		return member;
	}

	function parseCallExpression(callee: Statement): Statement {
		let expression: Statement = new CallExpression(callee, parseArgs());

		expression = parseMemberExpression(expression); // foo.x().y

		if (is(TOKEN_TYPES.OpenParen)) {
			// foo.x()()
			expression = parseCallExpression(expression);
		}

		return expression;
	}

	function parseArgs(): Statement[] {
		// add (x + 5, foo())
		expect(TOKEN_TYPES.OpenParen, "Expected opening parenthesis for arguments list");

		const args = parseArgumentsList();

		expect(TOKEN_TYPES.CloseParen, "Expected closing parenthesis for arguments list");
		return args;
	}
	function parseArgumentsList(): Statement[] {
		// comma-separated arguments list

		const args = [];
		while (!is(TOKEN_TYPES.CloseParen)) {
			let argument = parseExpression();

			if (is(TOKEN_TYPES.Equals)) {
				// keyword argument
				// e.g., func(x = 5, y = a or b)
				++current; // consume equals
				if (!(argument instanceof Identifier)) {
					throw new SyntaxError(`Expected identifier for keyword argument`);
				}
				const value = parseExpression();
				argument = new KeywordArgumentExpression(argument, value);
			}
			args.push(argument);
			if (is(TOKEN_TYPES.Comma)) {
				++current; // consume comma
			}
		}
		return args;
	}

	function parseMemberExpressionArgumentsList(): Statement {
		// NOTE: This also handles slice expressions colon-separated arguments list
		// e.g., ['test'], [0], [:2], [1:], [1:2], [1:2:3]

		const slices: (Statement | undefined)[] = [];
		let isSlice = false;
		while (!is(TOKEN_TYPES.CloseSquareBracket)) {
			if (is(TOKEN_TYPES.Colon)) {
				// A case where a default is used
				// e.g., [:2] will be parsed as [undefined, 2]
				slices.push(undefined);
				++current; // consume colon
				isSlice = true;
			} else {
				slices.push(parseExpression());
				if (is(TOKEN_TYPES.Colon)) {
					++current; // consume colon after expression, if it exists
					isSlice = true;
				}
			}
		}
		if (slices.length === 0) {
			// []
			throw new SyntaxError(`Expected at least one argument for member/slice expression`);
		}

		if (isSlice) {
			if (slices.length > 3) {
				throw new SyntaxError(`Expected 0-3 arguments for slice expression`);
			}
			return new SliceExpression(...slices);
		}

		return slices[0] as Statement; // normal member expression
	}

	function parseMemberExpression(object: Statement): Statement {
		while (is(TOKEN_TYPES.Dot) || is(TOKEN_TYPES.OpenSquareBracket)) {
			const operator = tokens[current]; // . or [
			++current;
			let property: Statement;
			const computed = operator.type !== TOKEN_TYPES.Dot;
			if (computed) {
				// computed (i.e., bracket notation: obj[expr])
				property = parseMemberExpressionArgumentsList();
				expect(TOKEN_TYPES.CloseSquareBracket, "Expected closing square bracket");
			} else {
				// non-computed (i.e., dot notation: obj.expr)
				property = parsePrimaryExpression(); // should be an identifier
				if (property.type !== "Identifier") {
					throw new SyntaxError(`Expected identifier following dot operator`);
				}
			}
			object = new MemberExpression(object, property, computed);
		}
		return object;
	}

	function parseMultiplicativeExpression(): Statement {
		let left = parseTestExpression();

		// Multiplicative operators have higher precedence than test expressions
		// e.g., (4 * 4 is divisibleby(2)) evaluates as (4 * (4 is divisibleby(2)))

		while (is(TOKEN_TYPES.MultiplicativeBinaryOperator)) {
			const operator = tokens[current];
			++current;
			const right = parseTestExpression();
			left = new BinaryExpression(operator, left, right);
		}
		return left;
	}

	function parseTestExpression(): Statement {
		let operand = parseFilterExpression();

		while (is(TOKEN_TYPES.Is)) {
			// Support chaining tests
			++current; // consume is
			const negate = is(TOKEN_TYPES.Not);
			if (negate) {
				++current; // consume not
			}

			let filter = parsePrimaryExpression();
			if (filter instanceof BooleanLiteral) {
				// Special case: treat boolean literals as identifiers
				filter = new Identifier(filter.value.toString());
			} else if (filter instanceof NullLiteral) {
				filter = new Identifier("none");
			}
			if (!(filter instanceof Identifier)) {
				throw new SyntaxError(`Expected identifier for the test`);
			}
			// TODO: Add support for non-identifier tests
			operand = new TestExpression(operand, negate, filter);
		}
		return operand;
	}

	function parseFilterExpression(): Statement {
		let operand = parseCallMemberExpression();

		while (is(TOKEN_TYPES.Pipe)) {
			// Support chaining filters
			++current; // consume pipe
			let filter = parsePrimaryExpression(); // should be an identifier
			if (!(filter instanceof Identifier)) {
				throw new SyntaxError(`Expected identifier for the filter`);
			}
			if (is(TOKEN_TYPES.OpenParen)) {
				filter = parseCallExpression(filter);
			}
			operand = new FilterExpression(operand, filter as Identifier | CallExpression);
		}
		return operand;
	}

	function parsePrimaryExpression(): Statement {
		// Primary expression: number, string, identifier, function call, parenthesized expression
		const token = tokens[current];
		switch (token.type) {
			case TOKEN_TYPES.NumericLiteral:
				++current;
				return new NumericLiteral(Number(token.value));
			case TOKEN_TYPES.StringLiteral:
				++current;
				return new StringLiteral(token.value);
			case TOKEN_TYPES.BooleanLiteral:
				++current;
				return new BooleanLiteral(token.value.toLowerCase() === "true");
			case TOKEN_TYPES.NullLiteral:
				++current;
				return new NullLiteral(null);
			case TOKEN_TYPES.Identifier:
				++current;
				return new Identifier(token.value);
			case TOKEN_TYPES.OpenParen: {
				++current; // consume opening parenthesis
				const expression = parseExpressionSequence();
				if (tokens[current].type !== TOKEN_TYPES.CloseParen) {
					throw new SyntaxError(`Expected closing parenthesis, got ${tokens[current].type} instead`);
				}
				++current; // consume closing parenthesis
				return expression;
			}
			case TOKEN_TYPES.OpenSquareBracket: {
				++current; // consume opening square bracket

				const values = [];
				while (!is(TOKEN_TYPES.CloseSquareBracket)) {
					values.push(parseExpression());

					if (is(TOKEN_TYPES.Comma)) {
						++current; // consume comma
					}
				}
				++current; // consume closing square bracket

				return new ArrayLiteral(values);
			}
			case TOKEN_TYPES.OpenCurlyBracket: {
				++current; // consume opening curly bracket

				const values = new Map();
				while (!is(TOKEN_TYPES.CloseCurlyBracket)) {
					const key = parseExpression();
					expect(TOKEN_TYPES.Colon, "Expected colon between key and value in object literal");
					const value = parseExpression();
					values.set(key, value);

					if (is(TOKEN_TYPES.Comma)) {
						++current; // consume comma
					}
				}
				++current; // consume closing curly bracket

				return new ObjectLiteral(values);
			}
			default:
				throw new SyntaxError(`Unexpected token: ${token.type}`);
		}
	}

	while (current < tokens.length) {
		program.body.push(parseAny());
	}

	return program;
}

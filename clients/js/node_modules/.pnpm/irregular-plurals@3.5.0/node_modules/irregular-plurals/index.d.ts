import irregularPluralsJson = require('./irregular-plurals.json');

declare const irregularPlurals: ReadonlyMap<
	keyof typeof irregularPluralsJson,
	string
>;

export = irregularPlurals;

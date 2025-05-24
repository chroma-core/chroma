import { dirname, join } from 'path';
import { readFileSync } from 'fs';
import hasTypes from 'hastypes';
import semver from 'semver';
import { execSync } from 'child_process';
import { createRequire } from 'module';
import { pathToFileURL } from 'url';

const packageJSONpath = join(process.cwd(), 'package.json');

const require = createRequire(pathToFileURL(packageJSONpath));

const { dependencies, devDependencies } = JSON.parse(readFileSync(packageJSONpath));

const typesPackagesPresent = Object.entries(devDependencies).filter(([name]) => name.startsWith('@types/'));

console.log(`Found ${typesPackagesPresent.length} \`@types/\` packages...`);

const typesPackagesToRemove = Promise.all(typesPackagesPresent.filter(([x]) => x !== '@types/node').map(async ([name, version]) => {
	const actualName = name.replace('@types/', '');
	let actualVersion;
	try {
		actualVersion = JSON.parse(readFileSync(join(process.cwd(), 'node_modules', actualName, 'package.json'))).version;
	} catch (e) {
		console.error(e, join(actualName, '/package.json'));
		return [name, , true];
	}
	const expectedVersion = `${semver.major(actualVersion)}.${semver.minor(actualVersion)}`;
	const specifier = `${actualName}@${expectedVersion}`;

	return [name, expectedVersion, await hasTypes(specifier)];
})).then((x) => x.filter(([, , hasTypes]) => hasTypes === true));// .then((x) => x.map(([name, expectedVersion]) => [name, expectedVersion]));

typesPackagesToRemove.then((x) => {
	console.log(`Found ${x.length} \`@types/\` packages to remove...`);
	console.log(x);
	if (x.length > 0) {
		execSync(`npm uninstall --save ${x.map(([name, version]) => `"${name}@${version}"`).join(' ')}`, { cwd: process.cwd() });
	}
});

// const typesPackagesToAdd = Promise.all(
// 	Object.entries(dependencies)
// 		.filter(([name]) => !typesPackagesPresent.includes(`@types/${name}`))
// 		.map(async ([name, version]) => {
// 			const actualVersion = require(`${name}/package.json`).version;
// 			const expectedVersion = `${semver.major(actualVersion)}.${semver.minor(actualVersion)}`;
// 			console.log(specifier);
// 		})
// )

/**
 * This file contains the necessary scripts and tasks for generating type definition files (.d.ts) and supporting
 * all environments for the chromadb package. It utilizes Gulp task runner to compile TypeScript code,
 * generate root .js files for each folder, update package.json, copy .d.ts files, and perform cleanup tasks.
 *
 * The main tasks provided by this file are:
 * - compile: Compiles the TypeScript code using different build targets (node and esm).
 * - watch: Watches for changes in the TypeScript code and recompiles the code.
 * - generateRootJS: Generates root .js files for each folder in src/embeddings to support sub-packages.
 * - updatePackageJSON: Updates the package.json file with package.exports entries and file list dynamically.
 * - dts: Generates type definition files and copies them to the appropriate locations.
 * - cleanup: Performs cleanup tasks like deleting temporary files and folders.
 */
const babel = require("gulp-babel");
const gulp = require("gulp");
const path = require("path");
const watch = require("gulp-watch");
const rimraf = require("rimraf").rimraf;
const fs = require("fs");
const { exec } = require("child_process");

const BUILD = process.env.BUILD_TARGET || "node";

const PRESETS = {
  esm: [
    "@babel/preset-typescript",
    [
      "@babel/preset-env",
      {
        useBuiltIns: "entry",
        targets: { esmodules: true },
        modules: false,
      },
    ],
  ],
  node: [
    "@babel/preset-typescript",
    [
      "@babel/preset-env",
      {
        targets: { node: "14" },
        modules: "commonjs",
      },
    ],
  ],
};

function compileTask(stream, outputDir) {
  return stream
    .pipe(
      babel({
        presets: PRESETS[BUILD],
      })
    )
    .pipe(gulp.dest(path.join(outputDir, BUILD)));
}

function readFoldersAndFiles(dir, fileExtension = ".js") {
  return fs.readdirSync(dir).reduce(
    (acc, item) => {
      const fullPath = path.join(dir, item);
      const isDirectory = fs.statSync(fullPath).isDirectory();

      if (isDirectory) {
        acc.folders.push(item);
      } else if (item.endsWith(fileExtension)) {
        acc.files.push(path.parse(item).name);
      }

      return acc;
    },
    { folders: [], files: [] }
  );
}

gulp.task("compile", function () {
  return compileTask(gulp.src(["src/**/*.ts", "!src/**/*.d.ts"]), "lib", "esm");
});

gulp.task("compile:cjs", function () {
  return compileTask(gulp.src(["src/**/*.ts", "!src/**/*.d.ts"]), "lib", "cjs");
});

gulp.task("compile:esm", function () {
  return compileTask(gulp.src(["src/**/*.ts", "!src/**/*.d.ts"]), "lib", "esm");
});

gulp.task("watch", function () {
  return compileTask(
    watch(["src/**/*.ts", "!src/**/*.d.ts"], {
      ignoreInitial: false,
      verbose: true,
    }),
    "lib"
  );
});

// Generate root .js files for each folder in lib and src/embeddings
gulp.task("generateRootJS", function (cb) {
  ["src/embeddings"].forEach((baseDir) => {
    fs.readdirSync(baseDir).forEach((folder) => {
      const fullPath = path.join(baseDir, folder);
      if (fs.statSync(fullPath).isDirectory()) {
        // Special handling for 'embeddings' folders
        if (folder === "embeddings") {
          const embeddingDirs = fs.readdirSync(fullPath);
          embeddingDirs.forEach((embeddingFolder) => {
            const embeddingPath = path.join(fullPath, embeddingFolder);
            if (fs.statSync(embeddingPath).isDirectory()) {
              const files = fs.readdirSync(embeddingPath);
              files.forEach((file) => {
                if (file.endsWith(".js")) {
                  const filenameWithoutExt = path.parse(file).name;
                  fs.writeFileSync(
                    `${embeddingFolder}.js`,
                    `module.exports = require('./${baseDir}/${folder}/${embeddingFolder}/${filenameWithoutExt}');`
                  );
                }
              });
            }
          });
        } else {
          // General case for other folders
          fs.writeFileSync(
            `${folder}.js`,
            `module.exports = require('./${baseDir}/${folder}/index.js');`
          );
        }
      }
    });
  });
  cb();
});

gulp.task("updatePackageJSON", function (cb) {
  const packageJSON = require("./package.json");

  const packageExports = {
    ".": {
      types: "./lib/node/index.d.ts",
      require: "./lib/node/index.js",
      import: "./lib/esm/index.js",
    },
    "./lib/*": {
      default: "./lib/*",
    },
  };

  const updateExportsAndFiles = (rootDir, type) => {
    const { folders } = readFoldersAndFiles(`${rootDir}/embeddings`);
    folders.forEach((folder) => {
      const { files } = readFoldersAndFiles(`${rootDir}/embeddings/${folder}`);
      files.forEach((file) => {
        packageExports[`./${folder}`] = {
          types: `./${folder}.d.ts`,
          require: `./lib/node/embeddings/${folder}/${file}.js`,
          import: `./lib/esm/embeddings/${folder}/${file}.js`,
        };
      });
    });
  };

  updateExportsAndFiles("./lib/esm", "esm");
  updateExportsAndFiles("./lib/node", "node");

  packageJSON.exports = packageExports;

  // Update files array dynamically
  packageJSON.files = [
    "lib",
    "LICENSE",
    "README.md",
    ...readFoldersAndFiles("./lib/node/embeddings").folders.map(
      (folder) => `${folder}.js`
    ),
    ...readFoldersAndFiles("./lib/node/embeddings").folders.map(
      (folder) => `${folder}.d.ts`
    ),
  ];

  fs.writeFileSync("./package.json", JSON.stringify(packageJSON, null, 2));

  cb();
});

// Copy index.d.ts to appropriate locations
gulp.task("dts:copy-root", function (cb) {
  const rootDir = "src";
  const { folders } = readFoldersAndFiles(`${rootDir}/embeddings`);
  folders.forEach((folder) => {
    const { files } = readFoldersAndFiles(
      `${rootDir}/embeddings/${folder}`,
      ".d.ts"
    );
    files.forEach((dtsFile) => {
      const dtsContent = fs.readFileSync(
        path.join(`${rootDir}/embeddings/${folder}`, `${dtsFile}.ts`),
        "utf8"
      );
      fs.writeFileSync(`${folder}.d.ts`, dtsContent);
    });
  });

  cb();
});

gulp.task("dts:generate", function (cb) {
  let tsFiles = [];
  const rootDir = "src";

  const { folders } = readFoldersAndFiles(`${rootDir}/embeddings`);
  folders.forEach((folder) => {
    const { files } = readFoldersAndFiles(`src/embeddings/${folder}`, "ts");
    tsFiles = [
      ...tsFiles,
      ...files.map((filename) => `src/embeddings/${folder}/${filename}.ts`),
    ];
  });

  const cmd = `yarn dts-bundle-generator --export-referenced-types --project tsconfig.json src/index.ts ${tsFiles.join(
    " "
  )}`;

  exec(cmd, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error executing command: ${error}`);
      return;
    }
    console.log(stdout);
    console.error(stderr);
    cb();
  });
});

gulp.task("dts:copy", function () {
  return gulp
    .src("src/**/*.d.ts")
    .pipe(gulp.dest("lib/esm"))
    .pipe(gulp.dest("lib/node"));
});

gulp.task("dts:cleanup", async function (cb) {
  await rimraf("src/**/*.d.ts", { glob: true });
  cb();
});

// We dynamically create files for embedding functions, after npm publish we want to delete them
gulp.task("build:cleanup", async function (cb) {
  await rimraf("./*.d.ts", { glob: true });
  const { folders: embeddingFunctionFolders } =
    readFoldersAndFiles(`./src/embeddings`);
  await Promise.all(
    embeddingFunctionFolders.map((folder) =>
      rimraf(`${folder}.js`, { glob: true })
    )
  );
  cb();
});

gulp.task("check-package-json-files", function (cb) {
  const pkg = require("./package.json");

  if (!pkg.files) {
    console.error('No "files" field in package.json');
    cb(new Error('No "files" field in package.json'));
    return;
  }

  let allExist = true;

  for (const fileOrDir of pkg.files) {
    const fullPath = path.join(__dirname, fileOrDir);

    if (!fs.existsSync(fullPath)) {
      allExist = false;
      console.error(`Missing: ${fullPath}`);
    }
  }

  if (!allExist) {
    cb(
      new Error(
        'One or more files or directories listed in the "files" field of package.json do not exist.'
      )
    );
    return;
  }

  console.log('All files and folders in package.json "files" field exist.');
  cb();
});

gulp.task(
  "build",
  gulp.series(gulp.parallel("compile:cjs", "compile:esm"), "generateRootJS")
);
gulp.task(
  "dts",
  gulp.series("dts:generate", "dts:copy", "dts:copy-root", "dts:cleanup")
);
gulp.task("cleanup", gulp.parallel("dts:cleanup", "build:cleanup"));
gulp.task("default", gulp.series("build"));

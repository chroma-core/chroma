(function webpackUniversalModuleDefinition(root, factory) {
	if(typeof exports === 'object' && typeof module === 'object')
		module.exports = factory();
	else if(typeof define === 'function' && define.amd)
		define([], factory);
	else {
		var a = factory();
		for(var i in a) (typeof exports === 'object' ? exports : root)[i] = a[i];
	}
})(this, function() {
return /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};

/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {

/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId])
/******/ 			return installedModules[moduleId].exports;

/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			exports: {},
/******/ 			id: moduleId,
/******/ 			loaded: false
/******/ 		};

/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);

/******/ 		// Flag the module as loaded
/******/ 		module.loaded = true;

/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}


/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;

/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;

/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "/";

/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(1);
	module.exports = __webpack_require__(332);


/***/ }),
/* 1 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {"use strict";

	__webpack_require__(2);

	__webpack_require__(328);

	__webpack_require__(329);

	if (global._babelPolyfill) {
	  throw new Error("only one instance of babel-polyfill is allowed");
	}
	global._babelPolyfill = true;

	var DEFINE_PROPERTY = "defineProperty";
	function define(O, key, value) {
	  O[key] || Object[DEFINE_PROPERTY](O, key, {
	    writable: true,
	    configurable: true,
	    value: value
	  });
	}

	define(String.prototype, "padLeft", "".padStart);
	define(String.prototype, "padRight", "".padEnd);

	"pop,reverse,shift,keys,values,entries,indexOf,every,some,forEach,map,filter,find,findIndex,includes,join,slice,concat,push,splice,unshift,sort,lastIndexOf,reduce,reduceRight,copyWithin,fill".split(",").forEach(function (key) {
	  [][key] && define(Array, key, Function.call.bind([][key]));
	});
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 2 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(3);
	__webpack_require__(53);
	__webpack_require__(54);
	__webpack_require__(55);
	__webpack_require__(56);
	__webpack_require__(58);
	__webpack_require__(60);
	__webpack_require__(61);
	__webpack_require__(62);
	__webpack_require__(63);
	__webpack_require__(64);
	__webpack_require__(65);
	__webpack_require__(66);
	__webpack_require__(67);
	__webpack_require__(68);
	__webpack_require__(70);
	__webpack_require__(72);
	__webpack_require__(74);
	__webpack_require__(76);
	__webpack_require__(79);
	__webpack_require__(80);
	__webpack_require__(81);
	__webpack_require__(85);
	__webpack_require__(87);
	__webpack_require__(89);
	__webpack_require__(92);
	__webpack_require__(93);
	__webpack_require__(94);
	__webpack_require__(95);
	__webpack_require__(97);
	__webpack_require__(98);
	__webpack_require__(99);
	__webpack_require__(100);
	__webpack_require__(101);
	__webpack_require__(102);
	__webpack_require__(103);
	__webpack_require__(105);
	__webpack_require__(106);
	__webpack_require__(107);
	__webpack_require__(109);
	__webpack_require__(110);
	__webpack_require__(111);
	__webpack_require__(113);
	__webpack_require__(115);
	__webpack_require__(116);
	__webpack_require__(117);
	__webpack_require__(118);
	__webpack_require__(119);
	__webpack_require__(120);
	__webpack_require__(121);
	__webpack_require__(122);
	__webpack_require__(123);
	__webpack_require__(124);
	__webpack_require__(125);
	__webpack_require__(126);
	__webpack_require__(127);
	__webpack_require__(132);
	__webpack_require__(133);
	__webpack_require__(137);
	__webpack_require__(138);
	__webpack_require__(139);
	__webpack_require__(140);
	__webpack_require__(142);
	__webpack_require__(143);
	__webpack_require__(144);
	__webpack_require__(145);
	__webpack_require__(146);
	__webpack_require__(147);
	__webpack_require__(148);
	__webpack_require__(149);
	__webpack_require__(150);
	__webpack_require__(151);
	__webpack_require__(152);
	__webpack_require__(153);
	__webpack_require__(154);
	__webpack_require__(155);
	__webpack_require__(156);
	__webpack_require__(158);
	__webpack_require__(159);
	__webpack_require__(161);
	__webpack_require__(162);
	__webpack_require__(168);
	__webpack_require__(169);
	__webpack_require__(171);
	__webpack_require__(172);
	__webpack_require__(173);
	__webpack_require__(177);
	__webpack_require__(178);
	__webpack_require__(179);
	__webpack_require__(180);
	__webpack_require__(181);
	__webpack_require__(183);
	__webpack_require__(184);
	__webpack_require__(185);
	__webpack_require__(186);
	__webpack_require__(189);
	__webpack_require__(191);
	__webpack_require__(192);
	__webpack_require__(193);
	__webpack_require__(195);
	__webpack_require__(197);
	__webpack_require__(199);
	__webpack_require__(201);
	__webpack_require__(202);
	__webpack_require__(203);
	__webpack_require__(207);
	__webpack_require__(208);
	__webpack_require__(209);
	__webpack_require__(211);
	__webpack_require__(221);
	__webpack_require__(225);
	__webpack_require__(226);
	__webpack_require__(228);
	__webpack_require__(229);
	__webpack_require__(233);
	__webpack_require__(234);
	__webpack_require__(236);
	__webpack_require__(237);
	__webpack_require__(238);
	__webpack_require__(239);
	__webpack_require__(240);
	__webpack_require__(241);
	__webpack_require__(242);
	__webpack_require__(243);
	__webpack_require__(244);
	__webpack_require__(245);
	__webpack_require__(246);
	__webpack_require__(247);
	__webpack_require__(248);
	__webpack_require__(249);
	__webpack_require__(250);
	__webpack_require__(251);
	__webpack_require__(252);
	__webpack_require__(253);
	__webpack_require__(254);
	__webpack_require__(256);
	__webpack_require__(257);
	__webpack_require__(258);
	__webpack_require__(259);
	__webpack_require__(260);
	__webpack_require__(262);
	__webpack_require__(263);
	__webpack_require__(264);
	__webpack_require__(266);
	__webpack_require__(267);
	__webpack_require__(268);
	__webpack_require__(269);
	__webpack_require__(270);
	__webpack_require__(271);
	__webpack_require__(272);
	__webpack_require__(273);
	__webpack_require__(275);
	__webpack_require__(276);
	__webpack_require__(278);
	__webpack_require__(279);
	__webpack_require__(280);
	__webpack_require__(281);
	__webpack_require__(284);
	__webpack_require__(285);
	__webpack_require__(287);
	__webpack_require__(288);
	__webpack_require__(289);
	__webpack_require__(290);
	__webpack_require__(292);
	__webpack_require__(293);
	__webpack_require__(294);
	__webpack_require__(295);
	__webpack_require__(296);
	__webpack_require__(297);
	__webpack_require__(298);
	__webpack_require__(299);
	__webpack_require__(300);
	__webpack_require__(301);
	__webpack_require__(303);
	__webpack_require__(304);
	__webpack_require__(305);
	__webpack_require__(306);
	__webpack_require__(307);
	__webpack_require__(308);
	__webpack_require__(309);
	__webpack_require__(310);
	__webpack_require__(311);
	__webpack_require__(312);
	__webpack_require__(313);
	__webpack_require__(315);
	__webpack_require__(316);
	__webpack_require__(317);
	__webpack_require__(318);
	__webpack_require__(319);
	__webpack_require__(320);
	__webpack_require__(321);
	__webpack_require__(322);
	__webpack_require__(323);
	__webpack_require__(324);
	__webpack_require__(325);
	__webpack_require__(326);
	__webpack_require__(327);
	module.exports = __webpack_require__(9);


/***/ }),
/* 3 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// ECMAScript 6 symbols shim
	var global = __webpack_require__(4);
	var has = __webpack_require__(5);
	var DESCRIPTORS = __webpack_require__(6);
	var $export = __webpack_require__(8);
	var redefine = __webpack_require__(18);
	var META = __webpack_require__(25).KEY;
	var $fails = __webpack_require__(7);
	var shared = __webpack_require__(21);
	var setToStringTag = __webpack_require__(26);
	var uid = __webpack_require__(19);
	var wks = __webpack_require__(27);
	var wksExt = __webpack_require__(28);
	var wksDefine = __webpack_require__(29);
	var enumKeys = __webpack_require__(30);
	var isArray = __webpack_require__(45);
	var anObject = __webpack_require__(12);
	var isObject = __webpack_require__(13);
	var toObject = __webpack_require__(46);
	var toIObject = __webpack_require__(33);
	var toPrimitive = __webpack_require__(16);
	var createDesc = __webpack_require__(17);
	var _create = __webpack_require__(47);
	var gOPNExt = __webpack_require__(50);
	var $GOPD = __webpack_require__(52);
	var $GOPS = __webpack_require__(43);
	var $DP = __webpack_require__(11);
	var $keys = __webpack_require__(31);
	var gOPD = $GOPD.f;
	var dP = $DP.f;
	var gOPN = gOPNExt.f;
	var $Symbol = global.Symbol;
	var $JSON = global.JSON;
	var _stringify = $JSON && $JSON.stringify;
	var PROTOTYPE = 'prototype';
	var HIDDEN = wks('_hidden');
	var TO_PRIMITIVE = wks('toPrimitive');
	var isEnum = {}.propertyIsEnumerable;
	var SymbolRegistry = shared('symbol-registry');
	var AllSymbols = shared('symbols');
	var OPSymbols = shared('op-symbols');
	var ObjectProto = Object[PROTOTYPE];
	var USE_NATIVE = typeof $Symbol == 'function' && !!$GOPS.f;
	var QObject = global.QObject;
	// Don't use setters in Qt Script, https://github.com/zloirock/core-js/issues/173
	var setter = !QObject || !QObject[PROTOTYPE] || !QObject[PROTOTYPE].findChild;

	// fallback for old Android, https://code.google.com/p/v8/issues/detail?id=687
	var setSymbolDesc = DESCRIPTORS && $fails(function () {
	  return _create(dP({}, 'a', {
	    get: function () { return dP(this, 'a', { value: 7 }).a; }
	  })).a != 7;
	}) ? function (it, key, D) {
	  var protoDesc = gOPD(ObjectProto, key);
	  if (protoDesc) delete ObjectProto[key];
	  dP(it, key, D);
	  if (protoDesc && it !== ObjectProto) dP(ObjectProto, key, protoDesc);
	} : dP;

	var wrap = function (tag) {
	  var sym = AllSymbols[tag] = _create($Symbol[PROTOTYPE]);
	  sym._k = tag;
	  return sym;
	};

	var isSymbol = USE_NATIVE && typeof $Symbol.iterator == 'symbol' ? function (it) {
	  return typeof it == 'symbol';
	} : function (it) {
	  return it instanceof $Symbol;
	};

	var $defineProperty = function defineProperty(it, key, D) {
	  if (it === ObjectProto) $defineProperty(OPSymbols, key, D);
	  anObject(it);
	  key = toPrimitive(key, true);
	  anObject(D);
	  if (has(AllSymbols, key)) {
	    if (!D.enumerable) {
	      if (!has(it, HIDDEN)) dP(it, HIDDEN, createDesc(1, {}));
	      it[HIDDEN][key] = true;
	    } else {
	      if (has(it, HIDDEN) && it[HIDDEN][key]) it[HIDDEN][key] = false;
	      D = _create(D, { enumerable: createDesc(0, false) });
	    } return setSymbolDesc(it, key, D);
	  } return dP(it, key, D);
	};
	var $defineProperties = function defineProperties(it, P) {
	  anObject(it);
	  var keys = enumKeys(P = toIObject(P));
	  var i = 0;
	  var l = keys.length;
	  var key;
	  while (l > i) $defineProperty(it, key = keys[i++], P[key]);
	  return it;
	};
	var $create = function create(it, P) {
	  return P === undefined ? _create(it) : $defineProperties(_create(it), P);
	};
	var $propertyIsEnumerable = function propertyIsEnumerable(key) {
	  var E = isEnum.call(this, key = toPrimitive(key, true));
	  if (this === ObjectProto && has(AllSymbols, key) && !has(OPSymbols, key)) return false;
	  return E || !has(this, key) || !has(AllSymbols, key) || has(this, HIDDEN) && this[HIDDEN][key] ? E : true;
	};
	var $getOwnPropertyDescriptor = function getOwnPropertyDescriptor(it, key) {
	  it = toIObject(it);
	  key = toPrimitive(key, true);
	  if (it === ObjectProto && has(AllSymbols, key) && !has(OPSymbols, key)) return;
	  var D = gOPD(it, key);
	  if (D && has(AllSymbols, key) && !(has(it, HIDDEN) && it[HIDDEN][key])) D.enumerable = true;
	  return D;
	};
	var $getOwnPropertyNames = function getOwnPropertyNames(it) {
	  var names = gOPN(toIObject(it));
	  var result = [];
	  var i = 0;
	  var key;
	  while (names.length > i) {
	    if (!has(AllSymbols, key = names[i++]) && key != HIDDEN && key != META) result.push(key);
	  } return result;
	};
	var $getOwnPropertySymbols = function getOwnPropertySymbols(it) {
	  var IS_OP = it === ObjectProto;
	  var names = gOPN(IS_OP ? OPSymbols : toIObject(it));
	  var result = [];
	  var i = 0;
	  var key;
	  while (names.length > i) {
	    if (has(AllSymbols, key = names[i++]) && (IS_OP ? has(ObjectProto, key) : true)) result.push(AllSymbols[key]);
	  } return result;
	};

	// 19.4.1.1 Symbol([description])
	if (!USE_NATIVE) {
	  $Symbol = function Symbol() {
	    if (this instanceof $Symbol) throw TypeError('Symbol is not a constructor!');
	    var tag = uid(arguments.length > 0 ? arguments[0] : undefined);
	    var $set = function (value) {
	      if (this === ObjectProto) $set.call(OPSymbols, value);
	      if (has(this, HIDDEN) && has(this[HIDDEN], tag)) this[HIDDEN][tag] = false;
	      setSymbolDesc(this, tag, createDesc(1, value));
	    };
	    if (DESCRIPTORS && setter) setSymbolDesc(ObjectProto, tag, { configurable: true, set: $set });
	    return wrap(tag);
	  };
	  redefine($Symbol[PROTOTYPE], 'toString', function toString() {
	    return this._k;
	  });

	  $GOPD.f = $getOwnPropertyDescriptor;
	  $DP.f = $defineProperty;
	  __webpack_require__(51).f = gOPNExt.f = $getOwnPropertyNames;
	  __webpack_require__(44).f = $propertyIsEnumerable;
	  $GOPS.f = $getOwnPropertySymbols;

	  if (DESCRIPTORS && !__webpack_require__(22)) {
	    redefine(ObjectProto, 'propertyIsEnumerable', $propertyIsEnumerable, true);
	  }

	  wksExt.f = function (name) {
	    return wrap(wks(name));
	  };
	}

	$export($export.G + $export.W + $export.F * !USE_NATIVE, { Symbol: $Symbol });

	for (var es6Symbols = (
	  // 19.4.2.2, 19.4.2.3, 19.4.2.4, 19.4.2.6, 19.4.2.8, 19.4.2.9, 19.4.2.10, 19.4.2.11, 19.4.2.12, 19.4.2.13, 19.4.2.14
	  'hasInstance,isConcatSpreadable,iterator,match,replace,search,species,split,toPrimitive,toStringTag,unscopables'
	).split(','), j = 0; es6Symbols.length > j;)wks(es6Symbols[j++]);

	for (var wellKnownSymbols = $keys(wks.store), k = 0; wellKnownSymbols.length > k;) wksDefine(wellKnownSymbols[k++]);

	$export($export.S + $export.F * !USE_NATIVE, 'Symbol', {
	  // 19.4.2.1 Symbol.for(key)
	  'for': function (key) {
	    return has(SymbolRegistry, key += '')
	      ? SymbolRegistry[key]
	      : SymbolRegistry[key] = $Symbol(key);
	  },
	  // 19.4.2.5 Symbol.keyFor(sym)
	  keyFor: function keyFor(sym) {
	    if (!isSymbol(sym)) throw TypeError(sym + ' is not a symbol!');
	    for (var key in SymbolRegistry) if (SymbolRegistry[key] === sym) return key;
	  },
	  useSetter: function () { setter = true; },
	  useSimple: function () { setter = false; }
	});

	$export($export.S + $export.F * !USE_NATIVE, 'Object', {
	  // 19.1.2.2 Object.create(O [, Properties])
	  create: $create,
	  // 19.1.2.4 Object.defineProperty(O, P, Attributes)
	  defineProperty: $defineProperty,
	  // 19.1.2.3 Object.defineProperties(O, Properties)
	  defineProperties: $defineProperties,
	  // 19.1.2.6 Object.getOwnPropertyDescriptor(O, P)
	  getOwnPropertyDescriptor: $getOwnPropertyDescriptor,
	  // 19.1.2.7 Object.getOwnPropertyNames(O)
	  getOwnPropertyNames: $getOwnPropertyNames,
	  // 19.1.2.8 Object.getOwnPropertySymbols(O)
	  getOwnPropertySymbols: $getOwnPropertySymbols
	});

	// Chrome 38 and 39 `Object.getOwnPropertySymbols` fails on primitives
	// https://bugs.chromium.org/p/v8/issues/detail?id=3443
	var FAILS_ON_PRIMITIVES = $fails(function () { $GOPS.f(1); });

	$export($export.S + $export.F * FAILS_ON_PRIMITIVES, 'Object', {
	  getOwnPropertySymbols: function getOwnPropertySymbols(it) {
	    return $GOPS.f(toObject(it));
	  }
	});

	// 24.3.2 JSON.stringify(value [, replacer [, space]])
	$JSON && $export($export.S + $export.F * (!USE_NATIVE || $fails(function () {
	  var S = $Symbol();
	  // MS Edge converts symbol values to JSON as {}
	  // WebKit converts symbol values to JSON as null
	  // V8 throws on boxed symbols
	  return _stringify([S]) != '[null]' || _stringify({ a: S }) != '{}' || _stringify(Object(S)) != '{}';
	})), 'JSON', {
	  stringify: function stringify(it) {
	    var args = [it];
	    var i = 1;
	    var replacer, $replacer;
	    while (arguments.length > i) args.push(arguments[i++]);
	    $replacer = replacer = args[1];
	    if (!isObject(replacer) && it === undefined || isSymbol(it)) return; // IE8 returns string on undefined
	    if (!isArray(replacer)) replacer = function (key, value) {
	      if (typeof $replacer == 'function') value = $replacer.call(this, key, value);
	      if (!isSymbol(value)) return value;
	    };
	    args[1] = replacer;
	    return _stringify.apply($JSON, args);
	  }
	});

	// 19.4.3.4 Symbol.prototype[@@toPrimitive](hint)
	$Symbol[PROTOTYPE][TO_PRIMITIVE] || __webpack_require__(10)($Symbol[PROTOTYPE], TO_PRIMITIVE, $Symbol[PROTOTYPE].valueOf);
	// 19.4.3.5 Symbol.prototype[@@toStringTag]
	setToStringTag($Symbol, 'Symbol');
	// 20.2.1.9 Math[@@toStringTag]
	setToStringTag(Math, 'Math', true);
	// 24.3.3 JSON[@@toStringTag]
	setToStringTag(global.JSON, 'JSON', true);


/***/ }),
/* 4 */
/***/ (function(module, exports) {

	// https://github.com/zloirock/core-js/issues/86#issuecomment-115759028
	var global = module.exports = typeof window != 'undefined' && window.Math == Math
	  ? window : typeof self != 'undefined' && self.Math == Math ? self
	  // eslint-disable-next-line no-new-func
	  : Function('return this')();
	if (typeof __g == 'number') __g = global; // eslint-disable-line no-undef


/***/ }),
/* 5 */
/***/ (function(module, exports) {

	var hasOwnProperty = {}.hasOwnProperty;
	module.exports = function (it, key) {
	  return hasOwnProperty.call(it, key);
	};


/***/ }),
/* 6 */
/***/ (function(module, exports, __webpack_require__) {

	// Thank's IE8 for his funny defineProperty
	module.exports = !__webpack_require__(7)(function () {
	  return Object.defineProperty({}, 'a', { get: function () { return 7; } }).a != 7;
	});


/***/ }),
/* 7 */
/***/ (function(module, exports) {

	module.exports = function (exec) {
	  try {
	    return !!exec();
	  } catch (e) {
	    return true;
	  }
	};


/***/ }),
/* 8 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var core = __webpack_require__(9);
	var hide = __webpack_require__(10);
	var redefine = __webpack_require__(18);
	var ctx = __webpack_require__(23);
	var PROTOTYPE = 'prototype';

	var $export = function (type, name, source) {
	  var IS_FORCED = type & $export.F;
	  var IS_GLOBAL = type & $export.G;
	  var IS_STATIC = type & $export.S;
	  var IS_PROTO = type & $export.P;
	  var IS_BIND = type & $export.B;
	  var target = IS_GLOBAL ? global : IS_STATIC ? global[name] || (global[name] = {}) : (global[name] || {})[PROTOTYPE];
	  var exports = IS_GLOBAL ? core : core[name] || (core[name] = {});
	  var expProto = exports[PROTOTYPE] || (exports[PROTOTYPE] = {});
	  var key, own, out, exp;
	  if (IS_GLOBAL) source = name;
	  for (key in source) {
	    // contains in native
	    own = !IS_FORCED && target && target[key] !== undefined;
	    // export native or passed
	    out = (own ? target : source)[key];
	    // bind timers to global for call from export context
	    exp = IS_BIND && own ? ctx(out, global) : IS_PROTO && typeof out == 'function' ? ctx(Function.call, out) : out;
	    // extend global
	    if (target) redefine(target, key, out, type & $export.U);
	    // export
	    if (exports[key] != out) hide(exports, key, exp);
	    if (IS_PROTO && expProto[key] != out) expProto[key] = out;
	  }
	};
	global.core = core;
	// type bitmap
	$export.F = 1;   // forced
	$export.G = 2;   // global
	$export.S = 4;   // static
	$export.P = 8;   // proto
	$export.B = 16;  // bind
	$export.W = 32;  // wrap
	$export.U = 64;  // safe
	$export.R = 128; // real proto method for `library`
	module.exports = $export;


/***/ }),
/* 9 */
/***/ (function(module, exports) {

	var core = module.exports = { version: '2.6.10' };
	if (typeof __e == 'number') __e = core; // eslint-disable-line no-undef


/***/ }),
/* 10 */
/***/ (function(module, exports, __webpack_require__) {

	var dP = __webpack_require__(11);
	var createDesc = __webpack_require__(17);
	module.exports = __webpack_require__(6) ? function (object, key, value) {
	  return dP.f(object, key, createDesc(1, value));
	} : function (object, key, value) {
	  object[key] = value;
	  return object;
	};


/***/ }),
/* 11 */
/***/ (function(module, exports, __webpack_require__) {

	var anObject = __webpack_require__(12);
	var IE8_DOM_DEFINE = __webpack_require__(14);
	var toPrimitive = __webpack_require__(16);
	var dP = Object.defineProperty;

	exports.f = __webpack_require__(6) ? Object.defineProperty : function defineProperty(O, P, Attributes) {
	  anObject(O);
	  P = toPrimitive(P, true);
	  anObject(Attributes);
	  if (IE8_DOM_DEFINE) try {
	    return dP(O, P, Attributes);
	  } catch (e) { /* empty */ }
	  if ('get' in Attributes || 'set' in Attributes) throw TypeError('Accessors not supported!');
	  if ('value' in Attributes) O[P] = Attributes.value;
	  return O;
	};


/***/ }),
/* 12 */
/***/ (function(module, exports, __webpack_require__) {

	var isObject = __webpack_require__(13);
	module.exports = function (it) {
	  if (!isObject(it)) throw TypeError(it + ' is not an object!');
	  return it;
	};


/***/ }),
/* 13 */
/***/ (function(module, exports) {

	module.exports = function (it) {
	  return typeof it === 'object' ? it !== null : typeof it === 'function';
	};


/***/ }),
/* 14 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = !__webpack_require__(6) && !__webpack_require__(7)(function () {
	  return Object.defineProperty(__webpack_require__(15)('div'), 'a', { get: function () { return 7; } }).a != 7;
	});


/***/ }),
/* 15 */
/***/ (function(module, exports, __webpack_require__) {

	var isObject = __webpack_require__(13);
	var document = __webpack_require__(4).document;
	// typeof document.createElement is 'object' in old IE
	var is = isObject(document) && isObject(document.createElement);
	module.exports = function (it) {
	  return is ? document.createElement(it) : {};
	};


/***/ }),
/* 16 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.1.1 ToPrimitive(input [, PreferredType])
	var isObject = __webpack_require__(13);
	// instead of the ES6 spec version, we didn't implement @@toPrimitive case
	// and the second argument - flag - preferred type is a string
	module.exports = function (it, S) {
	  if (!isObject(it)) return it;
	  var fn, val;
	  if (S && typeof (fn = it.toString) == 'function' && !isObject(val = fn.call(it))) return val;
	  if (typeof (fn = it.valueOf) == 'function' && !isObject(val = fn.call(it))) return val;
	  if (!S && typeof (fn = it.toString) == 'function' && !isObject(val = fn.call(it))) return val;
	  throw TypeError("Can't convert object to primitive value");
	};


/***/ }),
/* 17 */
/***/ (function(module, exports) {

	module.exports = function (bitmap, value) {
	  return {
	    enumerable: !(bitmap & 1),
	    configurable: !(bitmap & 2),
	    writable: !(bitmap & 4),
	    value: value
	  };
	};


/***/ }),
/* 18 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var hide = __webpack_require__(10);
	var has = __webpack_require__(5);
	var SRC = __webpack_require__(19)('src');
	var $toString = __webpack_require__(20);
	var TO_STRING = 'toString';
	var TPL = ('' + $toString).split(TO_STRING);

	__webpack_require__(9).inspectSource = function (it) {
	  return $toString.call(it);
	};

	(module.exports = function (O, key, val, safe) {
	  var isFunction = typeof val == 'function';
	  if (isFunction) has(val, 'name') || hide(val, 'name', key);
	  if (O[key] === val) return;
	  if (isFunction) has(val, SRC) || hide(val, SRC, O[key] ? '' + O[key] : TPL.join(String(key)));
	  if (O === global) {
	    O[key] = val;
	  } else if (!safe) {
	    delete O[key];
	    hide(O, key, val);
	  } else if (O[key]) {
	    O[key] = val;
	  } else {
	    hide(O, key, val);
	  }
	// add fake Function#toString for correct work wrapped methods / constructors with methods like LoDash isNative
	})(Function.prototype, TO_STRING, function toString() {
	  return typeof this == 'function' && this[SRC] || $toString.call(this);
	});


/***/ }),
/* 19 */
/***/ (function(module, exports) {

	var id = 0;
	var px = Math.random();
	module.exports = function (key) {
	  return 'Symbol('.concat(key === undefined ? '' : key, ')_', (++id + px).toString(36));
	};


/***/ }),
/* 20 */
/***/ (function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__(21)('native-function-to-string', Function.toString);


/***/ }),
/* 21 */
/***/ (function(module, exports, __webpack_require__) {

	var core = __webpack_require__(9);
	var global = __webpack_require__(4);
	var SHARED = '__core-js_shared__';
	var store = global[SHARED] || (global[SHARED] = {});

	(module.exports = function (key, value) {
	  return store[key] || (store[key] = value !== undefined ? value : {});
	})('versions', []).push({
	  version: core.version,
	  mode: __webpack_require__(22) ? 'pure' : 'global',
	  copyright: '© 2019 Denis Pushkarev (zloirock.ru)'
	});


/***/ }),
/* 22 */
/***/ (function(module, exports) {

	module.exports = false;


/***/ }),
/* 23 */
/***/ (function(module, exports, __webpack_require__) {

	// optional / simple context binding
	var aFunction = __webpack_require__(24);
	module.exports = function (fn, that, length) {
	  aFunction(fn);
	  if (that === undefined) return fn;
	  switch (length) {
	    case 1: return function (a) {
	      return fn.call(that, a);
	    };
	    case 2: return function (a, b) {
	      return fn.call(that, a, b);
	    };
	    case 3: return function (a, b, c) {
	      return fn.call(that, a, b, c);
	    };
	  }
	  return function (/* ...args */) {
	    return fn.apply(that, arguments);
	  };
	};


/***/ }),
/* 24 */
/***/ (function(module, exports) {

	module.exports = function (it) {
	  if (typeof it != 'function') throw TypeError(it + ' is not a function!');
	  return it;
	};


/***/ }),
/* 25 */
/***/ (function(module, exports, __webpack_require__) {

	var META = __webpack_require__(19)('meta');
	var isObject = __webpack_require__(13);
	var has = __webpack_require__(5);
	var setDesc = __webpack_require__(11).f;
	var id = 0;
	var isExtensible = Object.isExtensible || function () {
	  return true;
	};
	var FREEZE = !__webpack_require__(7)(function () {
	  return isExtensible(Object.preventExtensions({}));
	});
	var setMeta = function (it) {
	  setDesc(it, META, { value: {
	    i: 'O' + ++id, // object ID
	    w: {}          // weak collections IDs
	  } });
	};
	var fastKey = function (it, create) {
	  // return primitive with prefix
	  if (!isObject(it)) return typeof it == 'symbol' ? it : (typeof it == 'string' ? 'S' : 'P') + it;
	  if (!has(it, META)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return 'F';
	    // not necessary to add metadata
	    if (!create) return 'E';
	    // add missing metadata
	    setMeta(it);
	  // return object ID
	  } return it[META].i;
	};
	var getWeak = function (it, create) {
	  if (!has(it, META)) {
	    // can't set metadata to uncaught frozen object
	    if (!isExtensible(it)) return true;
	    // not necessary to add metadata
	    if (!create) return false;
	    // add missing metadata
	    setMeta(it);
	  // return hash weak collections IDs
	  } return it[META].w;
	};
	// add metadata on freeze-family methods calling
	var onFreeze = function (it) {
	  if (FREEZE && meta.NEED && isExtensible(it) && !has(it, META)) setMeta(it);
	  return it;
	};
	var meta = module.exports = {
	  KEY: META,
	  NEED: false,
	  fastKey: fastKey,
	  getWeak: getWeak,
	  onFreeze: onFreeze
	};


/***/ }),
/* 26 */
/***/ (function(module, exports, __webpack_require__) {

	var def = __webpack_require__(11).f;
	var has = __webpack_require__(5);
	var TAG = __webpack_require__(27)('toStringTag');

	module.exports = function (it, tag, stat) {
	  if (it && !has(it = stat ? it : it.prototype, TAG)) def(it, TAG, { configurable: true, value: tag });
	};


/***/ }),
/* 27 */
/***/ (function(module, exports, __webpack_require__) {

	var store = __webpack_require__(21)('wks');
	var uid = __webpack_require__(19);
	var Symbol = __webpack_require__(4).Symbol;
	var USE_SYMBOL = typeof Symbol == 'function';

	var $exports = module.exports = function (name) {
	  return store[name] || (store[name] =
	    USE_SYMBOL && Symbol[name] || (USE_SYMBOL ? Symbol : uid)('Symbol.' + name));
	};

	$exports.store = store;


/***/ }),
/* 28 */
/***/ (function(module, exports, __webpack_require__) {

	exports.f = __webpack_require__(27);


/***/ }),
/* 29 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var core = __webpack_require__(9);
	var LIBRARY = __webpack_require__(22);
	var wksExt = __webpack_require__(28);
	var defineProperty = __webpack_require__(11).f;
	module.exports = function (name) {
	  var $Symbol = core.Symbol || (core.Symbol = LIBRARY ? {} : global.Symbol || {});
	  if (name.charAt(0) != '_' && !(name in $Symbol)) defineProperty($Symbol, name, { value: wksExt.f(name) });
	};


/***/ }),
/* 30 */
/***/ (function(module, exports, __webpack_require__) {

	// all enumerable object keys, includes symbols
	var getKeys = __webpack_require__(31);
	var gOPS = __webpack_require__(43);
	var pIE = __webpack_require__(44);
	module.exports = function (it) {
	  var result = getKeys(it);
	  var getSymbols = gOPS.f;
	  if (getSymbols) {
	    var symbols = getSymbols(it);
	    var isEnum = pIE.f;
	    var i = 0;
	    var key;
	    while (symbols.length > i) if (isEnum.call(it, key = symbols[i++])) result.push(key);
	  } return result;
	};


/***/ }),
/* 31 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.14 / 15.2.3.14 Object.keys(O)
	var $keys = __webpack_require__(32);
	var enumBugKeys = __webpack_require__(42);

	module.exports = Object.keys || function keys(O) {
	  return $keys(O, enumBugKeys);
	};


/***/ }),
/* 32 */
/***/ (function(module, exports, __webpack_require__) {

	var has = __webpack_require__(5);
	var toIObject = __webpack_require__(33);
	var arrayIndexOf = __webpack_require__(37)(false);
	var IE_PROTO = __webpack_require__(41)('IE_PROTO');

	module.exports = function (object, names) {
	  var O = toIObject(object);
	  var i = 0;
	  var result = [];
	  var key;
	  for (key in O) if (key != IE_PROTO) has(O, key) && result.push(key);
	  // Don't enum bug & hidden keys
	  while (names.length > i) if (has(O, key = names[i++])) {
	    ~arrayIndexOf(result, key) || result.push(key);
	  }
	  return result;
	};


/***/ }),
/* 33 */
/***/ (function(module, exports, __webpack_require__) {

	// to indexed object, toObject with fallback for non-array-like ES3 strings
	var IObject = __webpack_require__(34);
	var defined = __webpack_require__(36);
	module.exports = function (it) {
	  return IObject(defined(it));
	};


/***/ }),
/* 34 */
/***/ (function(module, exports, __webpack_require__) {

	// fallback for non-array-like ES3 and non-enumerable old V8 strings
	var cof = __webpack_require__(35);
	// eslint-disable-next-line no-prototype-builtins
	module.exports = Object('z').propertyIsEnumerable(0) ? Object : function (it) {
	  return cof(it) == 'String' ? it.split('') : Object(it);
	};


/***/ }),
/* 35 */
/***/ (function(module, exports) {

	var toString = {}.toString;

	module.exports = function (it) {
	  return toString.call(it).slice(8, -1);
	};


/***/ }),
/* 36 */
/***/ (function(module, exports) {

	// 7.2.1 RequireObjectCoercible(argument)
	module.exports = function (it) {
	  if (it == undefined) throw TypeError("Can't call method on  " + it);
	  return it;
	};


/***/ }),
/* 37 */
/***/ (function(module, exports, __webpack_require__) {

	// false -> Array#indexOf
	// true  -> Array#includes
	var toIObject = __webpack_require__(33);
	var toLength = __webpack_require__(38);
	var toAbsoluteIndex = __webpack_require__(40);
	module.exports = function (IS_INCLUDES) {
	  return function ($this, el, fromIndex) {
	    var O = toIObject($this);
	    var length = toLength(O.length);
	    var index = toAbsoluteIndex(fromIndex, length);
	    var value;
	    // Array#includes uses SameValueZero equality algorithm
	    // eslint-disable-next-line no-self-compare
	    if (IS_INCLUDES && el != el) while (length > index) {
	      value = O[index++];
	      // eslint-disable-next-line no-self-compare
	      if (value != value) return true;
	    // Array#indexOf ignores holes, Array#includes - not
	    } else for (;length > index; index++) if (IS_INCLUDES || index in O) {
	      if (O[index] === el) return IS_INCLUDES || index || 0;
	    } return !IS_INCLUDES && -1;
	  };
	};


/***/ }),
/* 38 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.1.15 ToLength
	var toInteger = __webpack_require__(39);
	var min = Math.min;
	module.exports = function (it) {
	  return it > 0 ? min(toInteger(it), 0x1fffffffffffff) : 0; // pow(2, 53) - 1 == 9007199254740991
	};


/***/ }),
/* 39 */
/***/ (function(module, exports) {

	// 7.1.4 ToInteger
	var ceil = Math.ceil;
	var floor = Math.floor;
	module.exports = function (it) {
	  return isNaN(it = +it) ? 0 : (it > 0 ? floor : ceil)(it);
	};


/***/ }),
/* 40 */
/***/ (function(module, exports, __webpack_require__) {

	var toInteger = __webpack_require__(39);
	var max = Math.max;
	var min = Math.min;
	module.exports = function (index, length) {
	  index = toInteger(index);
	  return index < 0 ? max(index + length, 0) : min(index, length);
	};


/***/ }),
/* 41 */
/***/ (function(module, exports, __webpack_require__) {

	var shared = __webpack_require__(21)('keys');
	var uid = __webpack_require__(19);
	module.exports = function (key) {
	  return shared[key] || (shared[key] = uid(key));
	};


/***/ }),
/* 42 */
/***/ (function(module, exports) {

	// IE 8- don't enum bug keys
	module.exports = (
	  'constructor,hasOwnProperty,isPrototypeOf,propertyIsEnumerable,toLocaleString,toString,valueOf'
	).split(',');


/***/ }),
/* 43 */
/***/ (function(module, exports) {

	exports.f = Object.getOwnPropertySymbols;


/***/ }),
/* 44 */
/***/ (function(module, exports) {

	exports.f = {}.propertyIsEnumerable;


/***/ }),
/* 45 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.2.2 IsArray(argument)
	var cof = __webpack_require__(35);
	module.exports = Array.isArray || function isArray(arg) {
	  return cof(arg) == 'Array';
	};


/***/ }),
/* 46 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.1.13 ToObject(argument)
	var defined = __webpack_require__(36);
	module.exports = function (it) {
	  return Object(defined(it));
	};


/***/ }),
/* 47 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.2 / 15.2.3.5 Object.create(O [, Properties])
	var anObject = __webpack_require__(12);
	var dPs = __webpack_require__(48);
	var enumBugKeys = __webpack_require__(42);
	var IE_PROTO = __webpack_require__(41)('IE_PROTO');
	var Empty = function () { /* empty */ };
	var PROTOTYPE = 'prototype';

	// Create object with fake `null` prototype: use iframe Object with cleared prototype
	var createDict = function () {
	  // Thrash, waste and sodomy: IE GC bug
	  var iframe = __webpack_require__(15)('iframe');
	  var i = enumBugKeys.length;
	  var lt = '<';
	  var gt = '>';
	  var iframeDocument;
	  iframe.style.display = 'none';
	  __webpack_require__(49).appendChild(iframe);
	  iframe.src = 'javascript:'; // eslint-disable-line no-script-url
	  // createDict = iframe.contentWindow.Object;
	  // html.removeChild(iframe);
	  iframeDocument = iframe.contentWindow.document;
	  iframeDocument.open();
	  iframeDocument.write(lt + 'script' + gt + 'document.F=Object' + lt + '/script' + gt);
	  iframeDocument.close();
	  createDict = iframeDocument.F;
	  while (i--) delete createDict[PROTOTYPE][enumBugKeys[i]];
	  return createDict();
	};

	module.exports = Object.create || function create(O, Properties) {
	  var result;
	  if (O !== null) {
	    Empty[PROTOTYPE] = anObject(O);
	    result = new Empty();
	    Empty[PROTOTYPE] = null;
	    // add "__proto__" for Object.getPrototypeOf polyfill
	    result[IE_PROTO] = O;
	  } else result = createDict();
	  return Properties === undefined ? result : dPs(result, Properties);
	};


/***/ }),
/* 48 */
/***/ (function(module, exports, __webpack_require__) {

	var dP = __webpack_require__(11);
	var anObject = __webpack_require__(12);
	var getKeys = __webpack_require__(31);

	module.exports = __webpack_require__(6) ? Object.defineProperties : function defineProperties(O, Properties) {
	  anObject(O);
	  var keys = getKeys(Properties);
	  var length = keys.length;
	  var i = 0;
	  var P;
	  while (length > i) dP.f(O, P = keys[i++], Properties[P]);
	  return O;
	};


/***/ }),
/* 49 */
/***/ (function(module, exports, __webpack_require__) {

	var document = __webpack_require__(4).document;
	module.exports = document && document.documentElement;


/***/ }),
/* 50 */
/***/ (function(module, exports, __webpack_require__) {

	// fallback for IE11 buggy Object.getOwnPropertyNames with iframe and window
	var toIObject = __webpack_require__(33);
	var gOPN = __webpack_require__(51).f;
	var toString = {}.toString;

	var windowNames = typeof window == 'object' && window && Object.getOwnPropertyNames
	  ? Object.getOwnPropertyNames(window) : [];

	var getWindowNames = function (it) {
	  try {
	    return gOPN(it);
	  } catch (e) {
	    return windowNames.slice();
	  }
	};

	module.exports.f = function getOwnPropertyNames(it) {
	  return windowNames && toString.call(it) == '[object Window]' ? getWindowNames(it) : gOPN(toIObject(it));
	};


/***/ }),
/* 51 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.7 / 15.2.3.4 Object.getOwnPropertyNames(O)
	var $keys = __webpack_require__(32);
	var hiddenKeys = __webpack_require__(42).concat('length', 'prototype');

	exports.f = Object.getOwnPropertyNames || function getOwnPropertyNames(O) {
	  return $keys(O, hiddenKeys);
	};


/***/ }),
/* 52 */
/***/ (function(module, exports, __webpack_require__) {

	var pIE = __webpack_require__(44);
	var createDesc = __webpack_require__(17);
	var toIObject = __webpack_require__(33);
	var toPrimitive = __webpack_require__(16);
	var has = __webpack_require__(5);
	var IE8_DOM_DEFINE = __webpack_require__(14);
	var gOPD = Object.getOwnPropertyDescriptor;

	exports.f = __webpack_require__(6) ? gOPD : function getOwnPropertyDescriptor(O, P) {
	  O = toIObject(O);
	  P = toPrimitive(P, true);
	  if (IE8_DOM_DEFINE) try {
	    return gOPD(O, P);
	  } catch (e) { /* empty */ }
	  if (has(O, P)) return createDesc(!pIE.f.call(O, P), O[P]);
	};


/***/ }),
/* 53 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	// 19.1.2.2 / 15.2.3.5 Object.create(O [, Properties])
	$export($export.S, 'Object', { create: __webpack_require__(47) });


/***/ }),
/* 54 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	// 19.1.2.4 / 15.2.3.6 Object.defineProperty(O, P, Attributes)
	$export($export.S + $export.F * !__webpack_require__(6), 'Object', { defineProperty: __webpack_require__(11).f });


/***/ }),
/* 55 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	// 19.1.2.3 / 15.2.3.7 Object.defineProperties(O, Properties)
	$export($export.S + $export.F * !__webpack_require__(6), 'Object', { defineProperties: __webpack_require__(48) });


/***/ }),
/* 56 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.6 Object.getOwnPropertyDescriptor(O, P)
	var toIObject = __webpack_require__(33);
	var $getOwnPropertyDescriptor = __webpack_require__(52).f;

	__webpack_require__(57)('getOwnPropertyDescriptor', function () {
	  return function getOwnPropertyDescriptor(it, key) {
	    return $getOwnPropertyDescriptor(toIObject(it), key);
	  };
	});


/***/ }),
/* 57 */
/***/ (function(module, exports, __webpack_require__) {

	// most Object methods by ES6 should accept primitives
	var $export = __webpack_require__(8);
	var core = __webpack_require__(9);
	var fails = __webpack_require__(7);
	module.exports = function (KEY, exec) {
	  var fn = (core.Object || {})[KEY] || Object[KEY];
	  var exp = {};
	  exp[KEY] = exec(fn);
	  $export($export.S + $export.F * fails(function () { fn(1); }), 'Object', exp);
	};


/***/ }),
/* 58 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.9 Object.getPrototypeOf(O)
	var toObject = __webpack_require__(46);
	var $getPrototypeOf = __webpack_require__(59);

	__webpack_require__(57)('getPrototypeOf', function () {
	  return function getPrototypeOf(it) {
	    return $getPrototypeOf(toObject(it));
	  };
	});


/***/ }),
/* 59 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.9 / 15.2.3.2 Object.getPrototypeOf(O)
	var has = __webpack_require__(5);
	var toObject = __webpack_require__(46);
	var IE_PROTO = __webpack_require__(41)('IE_PROTO');
	var ObjectProto = Object.prototype;

	module.exports = Object.getPrototypeOf || function (O) {
	  O = toObject(O);
	  if (has(O, IE_PROTO)) return O[IE_PROTO];
	  if (typeof O.constructor == 'function' && O instanceof O.constructor) {
	    return O.constructor.prototype;
	  } return O instanceof Object ? ObjectProto : null;
	};


/***/ }),
/* 60 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.14 Object.keys(O)
	var toObject = __webpack_require__(46);
	var $keys = __webpack_require__(31);

	__webpack_require__(57)('keys', function () {
	  return function keys(it) {
	    return $keys(toObject(it));
	  };
	});


/***/ }),
/* 61 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.7 Object.getOwnPropertyNames(O)
	__webpack_require__(57)('getOwnPropertyNames', function () {
	  return __webpack_require__(50).f;
	});


/***/ }),
/* 62 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.5 Object.freeze(O)
	var isObject = __webpack_require__(13);
	var meta = __webpack_require__(25).onFreeze;

	__webpack_require__(57)('freeze', function ($freeze) {
	  return function freeze(it) {
	    return $freeze && isObject(it) ? $freeze(meta(it)) : it;
	  };
	});


/***/ }),
/* 63 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.17 Object.seal(O)
	var isObject = __webpack_require__(13);
	var meta = __webpack_require__(25).onFreeze;

	__webpack_require__(57)('seal', function ($seal) {
	  return function seal(it) {
	    return $seal && isObject(it) ? $seal(meta(it)) : it;
	  };
	});


/***/ }),
/* 64 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.15 Object.preventExtensions(O)
	var isObject = __webpack_require__(13);
	var meta = __webpack_require__(25).onFreeze;

	__webpack_require__(57)('preventExtensions', function ($preventExtensions) {
	  return function preventExtensions(it) {
	    return $preventExtensions && isObject(it) ? $preventExtensions(meta(it)) : it;
	  };
	});


/***/ }),
/* 65 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.12 Object.isFrozen(O)
	var isObject = __webpack_require__(13);

	__webpack_require__(57)('isFrozen', function ($isFrozen) {
	  return function isFrozen(it) {
	    return isObject(it) ? $isFrozen ? $isFrozen(it) : false : true;
	  };
	});


/***/ }),
/* 66 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.13 Object.isSealed(O)
	var isObject = __webpack_require__(13);

	__webpack_require__(57)('isSealed', function ($isSealed) {
	  return function isSealed(it) {
	    return isObject(it) ? $isSealed ? $isSealed(it) : false : true;
	  };
	});


/***/ }),
/* 67 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.2.11 Object.isExtensible(O)
	var isObject = __webpack_require__(13);

	__webpack_require__(57)('isExtensible', function ($isExtensible) {
	  return function isExtensible(it) {
	    return isObject(it) ? $isExtensible ? $isExtensible(it) : true : false;
	  };
	});


/***/ }),
/* 68 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.3.1 Object.assign(target, source)
	var $export = __webpack_require__(8);

	$export($export.S + $export.F, 'Object', { assign: __webpack_require__(69) });


/***/ }),
/* 69 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 19.1.2.1 Object.assign(target, source, ...)
	var DESCRIPTORS = __webpack_require__(6);
	var getKeys = __webpack_require__(31);
	var gOPS = __webpack_require__(43);
	var pIE = __webpack_require__(44);
	var toObject = __webpack_require__(46);
	var IObject = __webpack_require__(34);
	var $assign = Object.assign;

	// should work with symbols and should have deterministic property order (V8 bug)
	module.exports = !$assign || __webpack_require__(7)(function () {
	  var A = {};
	  var B = {};
	  // eslint-disable-next-line no-undef
	  var S = Symbol();
	  var K = 'abcdefghijklmnopqrst';
	  A[S] = 7;
	  K.split('').forEach(function (k) { B[k] = k; });
	  return $assign({}, A)[S] != 7 || Object.keys($assign({}, B)).join('') != K;
	}) ? function assign(target, source) { // eslint-disable-line no-unused-vars
	  var T = toObject(target);
	  var aLen = arguments.length;
	  var index = 1;
	  var getSymbols = gOPS.f;
	  var isEnum = pIE.f;
	  while (aLen > index) {
	    var S = IObject(arguments[index++]);
	    var keys = getSymbols ? getKeys(S).concat(getSymbols(S)) : getKeys(S);
	    var length = keys.length;
	    var j = 0;
	    var key;
	    while (length > j) {
	      key = keys[j++];
	      if (!DESCRIPTORS || isEnum.call(S, key)) T[key] = S[key];
	    }
	  } return T;
	} : $assign;


/***/ }),
/* 70 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.3.10 Object.is(value1, value2)
	var $export = __webpack_require__(8);
	$export($export.S, 'Object', { is: __webpack_require__(71) });


/***/ }),
/* 71 */
/***/ (function(module, exports) {

	// 7.2.9 SameValue(x, y)
	module.exports = Object.is || function is(x, y) {
	  // eslint-disable-next-line no-self-compare
	  return x === y ? x !== 0 || 1 / x === 1 / y : x != x && y != y;
	};


/***/ }),
/* 72 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.1.3.19 Object.setPrototypeOf(O, proto)
	var $export = __webpack_require__(8);
	$export($export.S, 'Object', { setPrototypeOf: __webpack_require__(73).set });


/***/ }),
/* 73 */
/***/ (function(module, exports, __webpack_require__) {

	// Works with __proto__ only. Old v8 can't work with null proto objects.
	/* eslint-disable no-proto */
	var isObject = __webpack_require__(13);
	var anObject = __webpack_require__(12);
	var check = function (O, proto) {
	  anObject(O);
	  if (!isObject(proto) && proto !== null) throw TypeError(proto + ": can't set as prototype!");
	};
	module.exports = {
	  set: Object.setPrototypeOf || ('__proto__' in {} ? // eslint-disable-line
	    function (test, buggy, set) {
	      try {
	        set = __webpack_require__(23)(Function.call, __webpack_require__(52).f(Object.prototype, '__proto__').set, 2);
	        set(test, []);
	        buggy = !(test instanceof Array);
	      } catch (e) { buggy = true; }
	      return function setPrototypeOf(O, proto) {
	        check(O, proto);
	        if (buggy) O.__proto__ = proto;
	        else set(O, proto);
	        return O;
	      };
	    }({}, false) : undefined),
	  check: check
	};


/***/ }),
/* 74 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 19.1.3.6 Object.prototype.toString()
	var classof = __webpack_require__(75);
	var test = {};
	test[__webpack_require__(27)('toStringTag')] = 'z';
	if (test + '' != '[object z]') {
	  __webpack_require__(18)(Object.prototype, 'toString', function toString() {
	    return '[object ' + classof(this) + ']';
	  }, true);
	}


/***/ }),
/* 75 */
/***/ (function(module, exports, __webpack_require__) {

	// getting tag from 19.1.3.6 Object.prototype.toString()
	var cof = __webpack_require__(35);
	var TAG = __webpack_require__(27)('toStringTag');
	// ES3 wrong here
	var ARG = cof(function () { return arguments; }()) == 'Arguments';

	// fallback for IE11 Script Access Denied error
	var tryGet = function (it, key) {
	  try {
	    return it[key];
	  } catch (e) { /* empty */ }
	};

	module.exports = function (it) {
	  var O, T, B;
	  return it === undefined ? 'Undefined' : it === null ? 'Null'
	    // @@toStringTag case
	    : typeof (T = tryGet(O = Object(it), TAG)) == 'string' ? T
	    // builtinTag case
	    : ARG ? cof(O)
	    // ES3 arguments fallback
	    : (B = cof(O)) == 'Object' && typeof O.callee == 'function' ? 'Arguments' : B;
	};


/***/ }),
/* 76 */
/***/ (function(module, exports, __webpack_require__) {

	// 19.2.3.2 / 15.3.4.5 Function.prototype.bind(thisArg, args...)
	var $export = __webpack_require__(8);

	$export($export.P, 'Function', { bind: __webpack_require__(77) });


/***/ }),
/* 77 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var aFunction = __webpack_require__(24);
	var isObject = __webpack_require__(13);
	var invoke = __webpack_require__(78);
	var arraySlice = [].slice;
	var factories = {};

	var construct = function (F, len, args) {
	  if (!(len in factories)) {
	    for (var n = [], i = 0; i < len; i++) n[i] = 'a[' + i + ']';
	    // eslint-disable-next-line no-new-func
	    factories[len] = Function('F,a', 'return new F(' + n.join(',') + ')');
	  } return factories[len](F, args);
	};

	module.exports = Function.bind || function bind(that /* , ...args */) {
	  var fn = aFunction(this);
	  var partArgs = arraySlice.call(arguments, 1);
	  var bound = function (/* args... */) {
	    var args = partArgs.concat(arraySlice.call(arguments));
	    return this instanceof bound ? construct(fn, args.length, args) : invoke(fn, args, that);
	  };
	  if (isObject(fn.prototype)) bound.prototype = fn.prototype;
	  return bound;
	};


/***/ }),
/* 78 */
/***/ (function(module, exports) {

	// fast apply, http://jsperf.lnkit.com/fast-apply/5
	module.exports = function (fn, args, that) {
	  var un = that === undefined;
	  switch (args.length) {
	    case 0: return un ? fn()
	                      : fn.call(that);
	    case 1: return un ? fn(args[0])
	                      : fn.call(that, args[0]);
	    case 2: return un ? fn(args[0], args[1])
	                      : fn.call(that, args[0], args[1]);
	    case 3: return un ? fn(args[0], args[1], args[2])
	                      : fn.call(that, args[0], args[1], args[2]);
	    case 4: return un ? fn(args[0], args[1], args[2], args[3])
	                      : fn.call(that, args[0], args[1], args[2], args[3]);
	  } return fn.apply(that, args);
	};


/***/ }),
/* 79 */
/***/ (function(module, exports, __webpack_require__) {

	var dP = __webpack_require__(11).f;
	var FProto = Function.prototype;
	var nameRE = /^\s*function ([^ (]*)/;
	var NAME = 'name';

	// 19.2.4.2 name
	NAME in FProto || __webpack_require__(6) && dP(FProto, NAME, {
	  configurable: true,
	  get: function () {
	    try {
	      return ('' + this).match(nameRE)[1];
	    } catch (e) {
	      return '';
	    }
	  }
	});


/***/ }),
/* 80 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var isObject = __webpack_require__(13);
	var getPrototypeOf = __webpack_require__(59);
	var HAS_INSTANCE = __webpack_require__(27)('hasInstance');
	var FunctionProto = Function.prototype;
	// 19.2.3.6 Function.prototype[@@hasInstance](V)
	if (!(HAS_INSTANCE in FunctionProto)) __webpack_require__(11).f(FunctionProto, HAS_INSTANCE, { value: function (O) {
	  if (typeof this != 'function' || !isObject(O)) return false;
	  if (!isObject(this.prototype)) return O instanceof this;
	  // for environment w/o native `@@hasInstance` logic enough `instanceof`, but add this:
	  while (O = getPrototypeOf(O)) if (this.prototype === O) return true;
	  return false;
	} });


/***/ }),
/* 81 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var $parseInt = __webpack_require__(82);
	// 18.2.5 parseInt(string, radix)
	$export($export.G + $export.F * (parseInt != $parseInt), { parseInt: $parseInt });


/***/ }),
/* 82 */
/***/ (function(module, exports, __webpack_require__) {

	var $parseInt = __webpack_require__(4).parseInt;
	var $trim = __webpack_require__(83).trim;
	var ws = __webpack_require__(84);
	var hex = /^[-+]?0[xX]/;

	module.exports = $parseInt(ws + '08') !== 8 || $parseInt(ws + '0x16') !== 22 ? function parseInt(str, radix) {
	  var string = $trim(String(str), 3);
	  return $parseInt(string, (radix >>> 0) || (hex.test(string) ? 16 : 10));
	} : $parseInt;


/***/ }),
/* 83 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var defined = __webpack_require__(36);
	var fails = __webpack_require__(7);
	var spaces = __webpack_require__(84);
	var space = '[' + spaces + ']';
	var non = '\u200b\u0085';
	var ltrim = RegExp('^' + space + space + '*');
	var rtrim = RegExp(space + space + '*$');

	var exporter = function (KEY, exec, ALIAS) {
	  var exp = {};
	  var FORCE = fails(function () {
	    return !!spaces[KEY]() || non[KEY]() != non;
	  });
	  var fn = exp[KEY] = FORCE ? exec(trim) : spaces[KEY];
	  if (ALIAS) exp[ALIAS] = fn;
	  $export($export.P + $export.F * FORCE, 'String', exp);
	};

	// 1 -> String#trimLeft
	// 2 -> String#trimRight
	// 3 -> String#trim
	var trim = exporter.trim = function (string, TYPE) {
	  string = String(defined(string));
	  if (TYPE & 1) string = string.replace(ltrim, '');
	  if (TYPE & 2) string = string.replace(rtrim, '');
	  return string;
	};

	module.exports = exporter;


/***/ }),
/* 84 */
/***/ (function(module, exports) {

	module.exports = '\x09\x0A\x0B\x0C\x0D\x20\xA0\u1680\u180E\u2000\u2001\u2002\u2003' +
	  '\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF';


/***/ }),
/* 85 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var $parseFloat = __webpack_require__(86);
	// 18.2.4 parseFloat(string)
	$export($export.G + $export.F * (parseFloat != $parseFloat), { parseFloat: $parseFloat });


/***/ }),
/* 86 */
/***/ (function(module, exports, __webpack_require__) {

	var $parseFloat = __webpack_require__(4).parseFloat;
	var $trim = __webpack_require__(83).trim;

	module.exports = 1 / $parseFloat(__webpack_require__(84) + '-0') !== -Infinity ? function parseFloat(str) {
	  var string = $trim(String(str), 3);
	  var result = $parseFloat(string);
	  return result === 0 && string.charAt(0) == '-' ? -0 : result;
	} : $parseFloat;


/***/ }),
/* 87 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var global = __webpack_require__(4);
	var has = __webpack_require__(5);
	var cof = __webpack_require__(35);
	var inheritIfRequired = __webpack_require__(88);
	var toPrimitive = __webpack_require__(16);
	var fails = __webpack_require__(7);
	var gOPN = __webpack_require__(51).f;
	var gOPD = __webpack_require__(52).f;
	var dP = __webpack_require__(11).f;
	var $trim = __webpack_require__(83).trim;
	var NUMBER = 'Number';
	var $Number = global[NUMBER];
	var Base = $Number;
	var proto = $Number.prototype;
	// Opera ~12 has broken Object#toString
	var BROKEN_COF = cof(__webpack_require__(47)(proto)) == NUMBER;
	var TRIM = 'trim' in String.prototype;

	// 7.1.3 ToNumber(argument)
	var toNumber = function (argument) {
	  var it = toPrimitive(argument, false);
	  if (typeof it == 'string' && it.length > 2) {
	    it = TRIM ? it.trim() : $trim(it, 3);
	    var first = it.charCodeAt(0);
	    var third, radix, maxCode;
	    if (first === 43 || first === 45) {
	      third = it.charCodeAt(2);
	      if (third === 88 || third === 120) return NaN; // Number('+0x1') should be NaN, old V8 fix
	    } else if (first === 48) {
	      switch (it.charCodeAt(1)) {
	        case 66: case 98: radix = 2; maxCode = 49; break; // fast equal /^0b[01]+$/i
	        case 79: case 111: radix = 8; maxCode = 55; break; // fast equal /^0o[0-7]+$/i
	        default: return +it;
	      }
	      for (var digits = it.slice(2), i = 0, l = digits.length, code; i < l; i++) {
	        code = digits.charCodeAt(i);
	        // parseInt parses a string to a first unavailable symbol
	        // but ToNumber should return NaN if a string contains unavailable symbols
	        if (code < 48 || code > maxCode) return NaN;
	      } return parseInt(digits, radix);
	    }
	  } return +it;
	};

	if (!$Number(' 0o1') || !$Number('0b1') || $Number('+0x1')) {
	  $Number = function Number(value) {
	    var it = arguments.length < 1 ? 0 : value;
	    var that = this;
	    return that instanceof $Number
	      // check on 1..constructor(foo) case
	      && (BROKEN_COF ? fails(function () { proto.valueOf.call(that); }) : cof(that) != NUMBER)
	        ? inheritIfRequired(new Base(toNumber(it)), that, $Number) : toNumber(it);
	  };
	  for (var keys = __webpack_require__(6) ? gOPN(Base) : (
	    // ES3:
	    'MAX_VALUE,MIN_VALUE,NaN,NEGATIVE_INFINITY,POSITIVE_INFINITY,' +
	    // ES6 (in case, if modules with ES6 Number statics required before):
	    'EPSILON,isFinite,isInteger,isNaN,isSafeInteger,MAX_SAFE_INTEGER,' +
	    'MIN_SAFE_INTEGER,parseFloat,parseInt,isInteger'
	  ).split(','), j = 0, key; keys.length > j; j++) {
	    if (has(Base, key = keys[j]) && !has($Number, key)) {
	      dP($Number, key, gOPD(Base, key));
	    }
	  }
	  $Number.prototype = proto;
	  proto.constructor = $Number;
	  __webpack_require__(18)(global, NUMBER, $Number);
	}


/***/ }),
/* 88 */
/***/ (function(module, exports, __webpack_require__) {

	var isObject = __webpack_require__(13);
	var setPrototypeOf = __webpack_require__(73).set;
	module.exports = function (that, target, C) {
	  var S = target.constructor;
	  var P;
	  if (S !== C && typeof S == 'function' && (P = S.prototype) !== C.prototype && isObject(P) && setPrototypeOf) {
	    setPrototypeOf(that, P);
	  } return that;
	};


/***/ }),
/* 89 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toInteger = __webpack_require__(39);
	var aNumberValue = __webpack_require__(90);
	var repeat = __webpack_require__(91);
	var $toFixed = 1.0.toFixed;
	var floor = Math.floor;
	var data = [0, 0, 0, 0, 0, 0];
	var ERROR = 'Number.toFixed: incorrect invocation!';
	var ZERO = '0';

	var multiply = function (n, c) {
	  var i = -1;
	  var c2 = c;
	  while (++i < 6) {
	    c2 += n * data[i];
	    data[i] = c2 % 1e7;
	    c2 = floor(c2 / 1e7);
	  }
	};
	var divide = function (n) {
	  var i = 6;
	  var c = 0;
	  while (--i >= 0) {
	    c += data[i];
	    data[i] = floor(c / n);
	    c = (c % n) * 1e7;
	  }
	};
	var numToString = function () {
	  var i = 6;
	  var s = '';
	  while (--i >= 0) {
	    if (s !== '' || i === 0 || data[i] !== 0) {
	      var t = String(data[i]);
	      s = s === '' ? t : s + repeat.call(ZERO, 7 - t.length) + t;
	    }
	  } return s;
	};
	var pow = function (x, n, acc) {
	  return n === 0 ? acc : n % 2 === 1 ? pow(x, n - 1, acc * x) : pow(x * x, n / 2, acc);
	};
	var log = function (x) {
	  var n = 0;
	  var x2 = x;
	  while (x2 >= 4096) {
	    n += 12;
	    x2 /= 4096;
	  }
	  while (x2 >= 2) {
	    n += 1;
	    x2 /= 2;
	  } return n;
	};

	$export($export.P + $export.F * (!!$toFixed && (
	  0.00008.toFixed(3) !== '0.000' ||
	  0.9.toFixed(0) !== '1' ||
	  1.255.toFixed(2) !== '1.25' ||
	  1000000000000000128.0.toFixed(0) !== '1000000000000000128'
	) || !__webpack_require__(7)(function () {
	  // V8 ~ Android 4.3-
	  $toFixed.call({});
	})), 'Number', {
	  toFixed: function toFixed(fractionDigits) {
	    var x = aNumberValue(this, ERROR);
	    var f = toInteger(fractionDigits);
	    var s = '';
	    var m = ZERO;
	    var e, z, j, k;
	    if (f < 0 || f > 20) throw RangeError(ERROR);
	    // eslint-disable-next-line no-self-compare
	    if (x != x) return 'NaN';
	    if (x <= -1e21 || x >= 1e21) return String(x);
	    if (x < 0) {
	      s = '-';
	      x = -x;
	    }
	    if (x > 1e-21) {
	      e = log(x * pow(2, 69, 1)) - 69;
	      z = e < 0 ? x * pow(2, -e, 1) : x / pow(2, e, 1);
	      z *= 0x10000000000000;
	      e = 52 - e;
	      if (e > 0) {
	        multiply(0, z);
	        j = f;
	        while (j >= 7) {
	          multiply(1e7, 0);
	          j -= 7;
	        }
	        multiply(pow(10, j, 1), 0);
	        j = e - 1;
	        while (j >= 23) {
	          divide(1 << 23);
	          j -= 23;
	        }
	        divide(1 << j);
	        multiply(1, 1);
	        divide(2);
	        m = numToString();
	      } else {
	        multiply(0, z);
	        multiply(1 << -e, 0);
	        m = numToString() + repeat.call(ZERO, f);
	      }
	    }
	    if (f > 0) {
	      k = m.length;
	      m = s + (k <= f ? '0.' + repeat.call(ZERO, f - k) + m : m.slice(0, k - f) + '.' + m.slice(k - f));
	    } else {
	      m = s + m;
	    } return m;
	  }
	});


/***/ }),
/* 90 */
/***/ (function(module, exports, __webpack_require__) {

	var cof = __webpack_require__(35);
	module.exports = function (it, msg) {
	  if (typeof it != 'number' && cof(it) != 'Number') throw TypeError(msg);
	  return +it;
	};


/***/ }),
/* 91 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var toInteger = __webpack_require__(39);
	var defined = __webpack_require__(36);

	module.exports = function repeat(count) {
	  var str = String(defined(this));
	  var res = '';
	  var n = toInteger(count);
	  if (n < 0 || n == Infinity) throw RangeError("Count can't be negative");
	  for (;n > 0; (n >>>= 1) && (str += str)) if (n & 1) res += str;
	  return res;
	};


/***/ }),
/* 92 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $fails = __webpack_require__(7);
	var aNumberValue = __webpack_require__(90);
	var $toPrecision = 1.0.toPrecision;

	$export($export.P + $export.F * ($fails(function () {
	  // IE7-
	  return $toPrecision.call(1, undefined) !== '1';
	}) || !$fails(function () {
	  // V8 ~ Android 4.3-
	  $toPrecision.call({});
	})), 'Number', {
	  toPrecision: function toPrecision(precision) {
	    var that = aNumberValue(this, 'Number#toPrecision: incorrect invocation!');
	    return precision === undefined ? $toPrecision.call(that) : $toPrecision.call(that, precision);
	  }
	});


/***/ }),
/* 93 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.1 Number.EPSILON
	var $export = __webpack_require__(8);

	$export($export.S, 'Number', { EPSILON: Math.pow(2, -52) });


/***/ }),
/* 94 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.2 Number.isFinite(number)
	var $export = __webpack_require__(8);
	var _isFinite = __webpack_require__(4).isFinite;

	$export($export.S, 'Number', {
	  isFinite: function isFinite(it) {
	    return typeof it == 'number' && _isFinite(it);
	  }
	});


/***/ }),
/* 95 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.3 Number.isInteger(number)
	var $export = __webpack_require__(8);

	$export($export.S, 'Number', { isInteger: __webpack_require__(96) });


/***/ }),
/* 96 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.3 Number.isInteger(number)
	var isObject = __webpack_require__(13);
	var floor = Math.floor;
	module.exports = function isInteger(it) {
	  return !isObject(it) && isFinite(it) && floor(it) === it;
	};


/***/ }),
/* 97 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.4 Number.isNaN(number)
	var $export = __webpack_require__(8);

	$export($export.S, 'Number', {
	  isNaN: function isNaN(number) {
	    // eslint-disable-next-line no-self-compare
	    return number != number;
	  }
	});


/***/ }),
/* 98 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.5 Number.isSafeInteger(number)
	var $export = __webpack_require__(8);
	var isInteger = __webpack_require__(96);
	var abs = Math.abs;

	$export($export.S, 'Number', {
	  isSafeInteger: function isSafeInteger(number) {
	    return isInteger(number) && abs(number) <= 0x1fffffffffffff;
	  }
	});


/***/ }),
/* 99 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.6 Number.MAX_SAFE_INTEGER
	var $export = __webpack_require__(8);

	$export($export.S, 'Number', { MAX_SAFE_INTEGER: 0x1fffffffffffff });


/***/ }),
/* 100 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.1.2.10 Number.MIN_SAFE_INTEGER
	var $export = __webpack_require__(8);

	$export($export.S, 'Number', { MIN_SAFE_INTEGER: -0x1fffffffffffff });


/***/ }),
/* 101 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var $parseFloat = __webpack_require__(86);
	// 20.1.2.12 Number.parseFloat(string)
	$export($export.S + $export.F * (Number.parseFloat != $parseFloat), 'Number', { parseFloat: $parseFloat });


/***/ }),
/* 102 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var $parseInt = __webpack_require__(82);
	// 20.1.2.13 Number.parseInt(string, radix)
	$export($export.S + $export.F * (Number.parseInt != $parseInt), 'Number', { parseInt: $parseInt });


/***/ }),
/* 103 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.3 Math.acosh(x)
	var $export = __webpack_require__(8);
	var log1p = __webpack_require__(104);
	var sqrt = Math.sqrt;
	var $acosh = Math.acosh;

	$export($export.S + $export.F * !($acosh
	  // V8 bug: https://code.google.com/p/v8/issues/detail?id=3509
	  && Math.floor($acosh(Number.MAX_VALUE)) == 710
	  // Tor Browser bug: Math.acosh(Infinity) -> NaN
	  && $acosh(Infinity) == Infinity
	), 'Math', {
	  acosh: function acosh(x) {
	    return (x = +x) < 1 ? NaN : x > 94906265.62425156
	      ? Math.log(x) + Math.LN2
	      : log1p(x - 1 + sqrt(x - 1) * sqrt(x + 1));
	  }
	});


/***/ }),
/* 104 */
/***/ (function(module, exports) {

	// 20.2.2.20 Math.log1p(x)
	module.exports = Math.log1p || function log1p(x) {
	  return (x = +x) > -1e-8 && x < 1e-8 ? x - x * x / 2 : Math.log(1 + x);
	};


/***/ }),
/* 105 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.5 Math.asinh(x)
	var $export = __webpack_require__(8);
	var $asinh = Math.asinh;

	function asinh(x) {
	  return !isFinite(x = +x) || x == 0 ? x : x < 0 ? -asinh(-x) : Math.log(x + Math.sqrt(x * x + 1));
	}

	// Tor Browser bug: Math.asinh(0) -> -0
	$export($export.S + $export.F * !($asinh && 1 / $asinh(0) > 0), 'Math', { asinh: asinh });


/***/ }),
/* 106 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.7 Math.atanh(x)
	var $export = __webpack_require__(8);
	var $atanh = Math.atanh;

	// Tor Browser bug: Math.atanh(-0) -> 0
	$export($export.S + $export.F * !($atanh && 1 / $atanh(-0) < 0), 'Math', {
	  atanh: function atanh(x) {
	    return (x = +x) == 0 ? x : Math.log((1 + x) / (1 - x)) / 2;
	  }
	});


/***/ }),
/* 107 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.9 Math.cbrt(x)
	var $export = __webpack_require__(8);
	var sign = __webpack_require__(108);

	$export($export.S, 'Math', {
	  cbrt: function cbrt(x) {
	    return sign(x = +x) * Math.pow(Math.abs(x), 1 / 3);
	  }
	});


/***/ }),
/* 108 */
/***/ (function(module, exports) {

	// 20.2.2.28 Math.sign(x)
	module.exports = Math.sign || function sign(x) {
	  // eslint-disable-next-line no-self-compare
	  return (x = +x) == 0 || x != x ? x : x < 0 ? -1 : 1;
	};


/***/ }),
/* 109 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.11 Math.clz32(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  clz32: function clz32(x) {
	    return (x >>>= 0) ? 31 - Math.floor(Math.log(x + 0.5) * Math.LOG2E) : 32;
	  }
	});


/***/ }),
/* 110 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.12 Math.cosh(x)
	var $export = __webpack_require__(8);
	var exp = Math.exp;

	$export($export.S, 'Math', {
	  cosh: function cosh(x) {
	    return (exp(x = +x) + exp(-x)) / 2;
	  }
	});


/***/ }),
/* 111 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.14 Math.expm1(x)
	var $export = __webpack_require__(8);
	var $expm1 = __webpack_require__(112);

	$export($export.S + $export.F * ($expm1 != Math.expm1), 'Math', { expm1: $expm1 });


/***/ }),
/* 112 */
/***/ (function(module, exports) {

	// 20.2.2.14 Math.expm1(x)
	var $expm1 = Math.expm1;
	module.exports = (!$expm1
	  // Old FF bug
	  || $expm1(10) > 22025.465794806719 || $expm1(10) < 22025.4657948067165168
	  // Tor Browser bug
	  || $expm1(-2e-17) != -2e-17
	) ? function expm1(x) {
	  return (x = +x) == 0 ? x : x > -1e-6 && x < 1e-6 ? x + x * x / 2 : Math.exp(x) - 1;
	} : $expm1;


/***/ }),
/* 113 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.16 Math.fround(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { fround: __webpack_require__(114) });


/***/ }),
/* 114 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.16 Math.fround(x)
	var sign = __webpack_require__(108);
	var pow = Math.pow;
	var EPSILON = pow(2, -52);
	var EPSILON32 = pow(2, -23);
	var MAX32 = pow(2, 127) * (2 - EPSILON32);
	var MIN32 = pow(2, -126);

	var roundTiesToEven = function (n) {
	  return n + 1 / EPSILON - 1 / EPSILON;
	};

	module.exports = Math.fround || function fround(x) {
	  var $abs = Math.abs(x);
	  var $sign = sign(x);
	  var a, result;
	  if ($abs < MIN32) return $sign * roundTiesToEven($abs / MIN32 / EPSILON32) * MIN32 * EPSILON32;
	  a = (1 + EPSILON32 / EPSILON) * $abs;
	  result = a - (a - $abs);
	  // eslint-disable-next-line no-self-compare
	  if (result > MAX32 || result != result) return $sign * Infinity;
	  return $sign * result;
	};


/***/ }),
/* 115 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.17 Math.hypot([value1[, value2[, … ]]])
	var $export = __webpack_require__(8);
	var abs = Math.abs;

	$export($export.S, 'Math', {
	  hypot: function hypot(value1, value2) { // eslint-disable-line no-unused-vars
	    var sum = 0;
	    var i = 0;
	    var aLen = arguments.length;
	    var larg = 0;
	    var arg, div;
	    while (i < aLen) {
	      arg = abs(arguments[i++]);
	      if (larg < arg) {
	        div = larg / arg;
	        sum = sum * div * div + 1;
	        larg = arg;
	      } else if (arg > 0) {
	        div = arg / larg;
	        sum += div * div;
	      } else sum += arg;
	    }
	    return larg === Infinity ? Infinity : larg * Math.sqrt(sum);
	  }
	});


/***/ }),
/* 116 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.18 Math.imul(x, y)
	var $export = __webpack_require__(8);
	var $imul = Math.imul;

	// some WebKit versions fails with big numbers, some has wrong arity
	$export($export.S + $export.F * __webpack_require__(7)(function () {
	  return $imul(0xffffffff, 5) != -5 || $imul.length != 2;
	}), 'Math', {
	  imul: function imul(x, y) {
	    var UINT16 = 0xffff;
	    var xn = +x;
	    var yn = +y;
	    var xl = UINT16 & xn;
	    var yl = UINT16 & yn;
	    return 0 | xl * yl + ((UINT16 & xn >>> 16) * yl + xl * (UINT16 & yn >>> 16) << 16 >>> 0);
	  }
	});


/***/ }),
/* 117 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.21 Math.log10(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  log10: function log10(x) {
	    return Math.log(x) * Math.LOG10E;
	  }
	});


/***/ }),
/* 118 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.20 Math.log1p(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { log1p: __webpack_require__(104) });


/***/ }),
/* 119 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.22 Math.log2(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  log2: function log2(x) {
	    return Math.log(x) / Math.LN2;
	  }
	});


/***/ }),
/* 120 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.28 Math.sign(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { sign: __webpack_require__(108) });


/***/ }),
/* 121 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.30 Math.sinh(x)
	var $export = __webpack_require__(8);
	var expm1 = __webpack_require__(112);
	var exp = Math.exp;

	// V8 near Chromium 38 has a problem with very small numbers
	$export($export.S + $export.F * __webpack_require__(7)(function () {
	  return !Math.sinh(-2e-17) != -2e-17;
	}), 'Math', {
	  sinh: function sinh(x) {
	    return Math.abs(x = +x) < 1
	      ? (expm1(x) - expm1(-x)) / 2
	      : (exp(x - 1) - exp(-x - 1)) * (Math.E / 2);
	  }
	});


/***/ }),
/* 122 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.33 Math.tanh(x)
	var $export = __webpack_require__(8);
	var expm1 = __webpack_require__(112);
	var exp = Math.exp;

	$export($export.S, 'Math', {
	  tanh: function tanh(x) {
	    var a = expm1(x = +x);
	    var b = expm1(-x);
	    return a == Infinity ? 1 : b == Infinity ? -1 : (a - b) / (exp(x) + exp(-x));
	  }
	});


/***/ }),
/* 123 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.2.2.34 Math.trunc(x)
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  trunc: function trunc(it) {
	    return (it > 0 ? Math.floor : Math.ceil)(it);
	  }
	});


/***/ }),
/* 124 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var toAbsoluteIndex = __webpack_require__(40);
	var fromCharCode = String.fromCharCode;
	var $fromCodePoint = String.fromCodePoint;

	// length should be 1, old FF problem
	$export($export.S + $export.F * (!!$fromCodePoint && $fromCodePoint.length != 1), 'String', {
	  // 21.1.2.2 String.fromCodePoint(...codePoints)
	  fromCodePoint: function fromCodePoint(x) { // eslint-disable-line no-unused-vars
	    var res = [];
	    var aLen = arguments.length;
	    var i = 0;
	    var code;
	    while (aLen > i) {
	      code = +arguments[i++];
	      if (toAbsoluteIndex(code, 0x10ffff) !== code) throw RangeError(code + ' is not a valid code point');
	      res.push(code < 0x10000
	        ? fromCharCode(code)
	        : fromCharCode(((code -= 0x10000) >> 10) + 0xd800, code % 0x400 + 0xdc00)
	      );
	    } return res.join('');
	  }
	});


/***/ }),
/* 125 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var toIObject = __webpack_require__(33);
	var toLength = __webpack_require__(38);

	$export($export.S, 'String', {
	  // 21.1.2.4 String.raw(callSite, ...substitutions)
	  raw: function raw(callSite) {
	    var tpl = toIObject(callSite.raw);
	    var len = toLength(tpl.length);
	    var aLen = arguments.length;
	    var res = [];
	    var i = 0;
	    while (len > i) {
	      res.push(String(tpl[i++]));
	      if (i < aLen) res.push(String(arguments[i]));
	    } return res.join('');
	  }
	});


/***/ }),
/* 126 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 21.1.3.25 String.prototype.trim()
	__webpack_require__(83)('trim', function ($trim) {
	  return function trim() {
	    return $trim(this, 3);
	  };
	});


/***/ }),
/* 127 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $at = __webpack_require__(128)(true);

	// 21.1.3.27 String.prototype[@@iterator]()
	__webpack_require__(129)(String, 'String', function (iterated) {
	  this._t = String(iterated); // target
	  this._i = 0;                // next index
	// 21.1.5.2.1 %StringIteratorPrototype%.next()
	}, function () {
	  var O = this._t;
	  var index = this._i;
	  var point;
	  if (index >= O.length) return { value: undefined, done: true };
	  point = $at(O, index);
	  this._i += point.length;
	  return { value: point, done: false };
	});


/***/ }),
/* 128 */
/***/ (function(module, exports, __webpack_require__) {

	var toInteger = __webpack_require__(39);
	var defined = __webpack_require__(36);
	// true  -> String#at
	// false -> String#codePointAt
	module.exports = function (TO_STRING) {
	  return function (that, pos) {
	    var s = String(defined(that));
	    var i = toInteger(pos);
	    var l = s.length;
	    var a, b;
	    if (i < 0 || i >= l) return TO_STRING ? '' : undefined;
	    a = s.charCodeAt(i);
	    return a < 0xd800 || a > 0xdbff || i + 1 === l || (b = s.charCodeAt(i + 1)) < 0xdc00 || b > 0xdfff
	      ? TO_STRING ? s.charAt(i) : a
	      : TO_STRING ? s.slice(i, i + 2) : (a - 0xd800 << 10) + (b - 0xdc00) + 0x10000;
	  };
	};


/***/ }),
/* 129 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var LIBRARY = __webpack_require__(22);
	var $export = __webpack_require__(8);
	var redefine = __webpack_require__(18);
	var hide = __webpack_require__(10);
	var Iterators = __webpack_require__(130);
	var $iterCreate = __webpack_require__(131);
	var setToStringTag = __webpack_require__(26);
	var getPrototypeOf = __webpack_require__(59);
	var ITERATOR = __webpack_require__(27)('iterator');
	var BUGGY = !([].keys && 'next' in [].keys()); // Safari has buggy iterators w/o `next`
	var FF_ITERATOR = '@@iterator';
	var KEYS = 'keys';
	var VALUES = 'values';

	var returnThis = function () { return this; };

	module.exports = function (Base, NAME, Constructor, next, DEFAULT, IS_SET, FORCED) {
	  $iterCreate(Constructor, NAME, next);
	  var getMethod = function (kind) {
	    if (!BUGGY && kind in proto) return proto[kind];
	    switch (kind) {
	      case KEYS: return function keys() { return new Constructor(this, kind); };
	      case VALUES: return function values() { return new Constructor(this, kind); };
	    } return function entries() { return new Constructor(this, kind); };
	  };
	  var TAG = NAME + ' Iterator';
	  var DEF_VALUES = DEFAULT == VALUES;
	  var VALUES_BUG = false;
	  var proto = Base.prototype;
	  var $native = proto[ITERATOR] || proto[FF_ITERATOR] || DEFAULT && proto[DEFAULT];
	  var $default = $native || getMethod(DEFAULT);
	  var $entries = DEFAULT ? !DEF_VALUES ? $default : getMethod('entries') : undefined;
	  var $anyNative = NAME == 'Array' ? proto.entries || $native : $native;
	  var methods, key, IteratorPrototype;
	  // Fix native
	  if ($anyNative) {
	    IteratorPrototype = getPrototypeOf($anyNative.call(new Base()));
	    if (IteratorPrototype !== Object.prototype && IteratorPrototype.next) {
	      // Set @@toStringTag to native iterators
	      setToStringTag(IteratorPrototype, TAG, true);
	      // fix for some old engines
	      if (!LIBRARY && typeof IteratorPrototype[ITERATOR] != 'function') hide(IteratorPrototype, ITERATOR, returnThis);
	    }
	  }
	  // fix Array#{values, @@iterator}.name in V8 / FF
	  if (DEF_VALUES && $native && $native.name !== VALUES) {
	    VALUES_BUG = true;
	    $default = function values() { return $native.call(this); };
	  }
	  // Define iterator
	  if ((!LIBRARY || FORCED) && (BUGGY || VALUES_BUG || !proto[ITERATOR])) {
	    hide(proto, ITERATOR, $default);
	  }
	  // Plug for library
	  Iterators[NAME] = $default;
	  Iterators[TAG] = returnThis;
	  if (DEFAULT) {
	    methods = {
	      values: DEF_VALUES ? $default : getMethod(VALUES),
	      keys: IS_SET ? $default : getMethod(KEYS),
	      entries: $entries
	    };
	    if (FORCED) for (key in methods) {
	      if (!(key in proto)) redefine(proto, key, methods[key]);
	    } else $export($export.P + $export.F * (BUGGY || VALUES_BUG), NAME, methods);
	  }
	  return methods;
	};


/***/ }),
/* 130 */
/***/ (function(module, exports) {

	module.exports = {};


/***/ }),
/* 131 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var create = __webpack_require__(47);
	var descriptor = __webpack_require__(17);
	var setToStringTag = __webpack_require__(26);
	var IteratorPrototype = {};

	// 25.1.2.1.1 %IteratorPrototype%[@@iterator]()
	__webpack_require__(10)(IteratorPrototype, __webpack_require__(27)('iterator'), function () { return this; });

	module.exports = function (Constructor, NAME, next) {
	  Constructor.prototype = create(IteratorPrototype, { next: descriptor(1, next) });
	  setToStringTag(Constructor, NAME + ' Iterator');
	};


/***/ }),
/* 132 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $at = __webpack_require__(128)(false);
	$export($export.P, 'String', {
	  // 21.1.3.3 String.prototype.codePointAt(pos)
	  codePointAt: function codePointAt(pos) {
	    return $at(this, pos);
	  }
	});


/***/ }),
/* 133 */
/***/ (function(module, exports, __webpack_require__) {

	// 21.1.3.6 String.prototype.endsWith(searchString [, endPosition])
	'use strict';
	var $export = __webpack_require__(8);
	var toLength = __webpack_require__(38);
	var context = __webpack_require__(134);
	var ENDS_WITH = 'endsWith';
	var $endsWith = ''[ENDS_WITH];

	$export($export.P + $export.F * __webpack_require__(136)(ENDS_WITH), 'String', {
	  endsWith: function endsWith(searchString /* , endPosition = @length */) {
	    var that = context(this, searchString, ENDS_WITH);
	    var endPosition = arguments.length > 1 ? arguments[1] : undefined;
	    var len = toLength(that.length);
	    var end = endPosition === undefined ? len : Math.min(toLength(endPosition), len);
	    var search = String(searchString);
	    return $endsWith
	      ? $endsWith.call(that, search, end)
	      : that.slice(end - search.length, end) === search;
	  }
	});


/***/ }),
/* 134 */
/***/ (function(module, exports, __webpack_require__) {

	// helper for String#{startsWith, endsWith, includes}
	var isRegExp = __webpack_require__(135);
	var defined = __webpack_require__(36);

	module.exports = function (that, searchString, NAME) {
	  if (isRegExp(searchString)) throw TypeError('String#' + NAME + " doesn't accept regex!");
	  return String(defined(that));
	};


/***/ }),
/* 135 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.2.8 IsRegExp(argument)
	var isObject = __webpack_require__(13);
	var cof = __webpack_require__(35);
	var MATCH = __webpack_require__(27)('match');
	module.exports = function (it) {
	  var isRegExp;
	  return isObject(it) && ((isRegExp = it[MATCH]) !== undefined ? !!isRegExp : cof(it) == 'RegExp');
	};


/***/ }),
/* 136 */
/***/ (function(module, exports, __webpack_require__) {

	var MATCH = __webpack_require__(27)('match');
	module.exports = function (KEY) {
	  var re = /./;
	  try {
	    '/./'[KEY](re);
	  } catch (e) {
	    try {
	      re[MATCH] = false;
	      return !'/./'[KEY](re);
	    } catch (f) { /* empty */ }
	  } return true;
	};


/***/ }),
/* 137 */
/***/ (function(module, exports, __webpack_require__) {

	// 21.1.3.7 String.prototype.includes(searchString, position = 0)
	'use strict';
	var $export = __webpack_require__(8);
	var context = __webpack_require__(134);
	var INCLUDES = 'includes';

	$export($export.P + $export.F * __webpack_require__(136)(INCLUDES), 'String', {
	  includes: function includes(searchString /* , position = 0 */) {
	    return !!~context(this, searchString, INCLUDES)
	      .indexOf(searchString, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});


/***/ }),
/* 138 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);

	$export($export.P, 'String', {
	  // 21.1.3.13 String.prototype.repeat(count)
	  repeat: __webpack_require__(91)
	});


/***/ }),
/* 139 */
/***/ (function(module, exports, __webpack_require__) {

	// 21.1.3.18 String.prototype.startsWith(searchString [, position ])
	'use strict';
	var $export = __webpack_require__(8);
	var toLength = __webpack_require__(38);
	var context = __webpack_require__(134);
	var STARTS_WITH = 'startsWith';
	var $startsWith = ''[STARTS_WITH];

	$export($export.P + $export.F * __webpack_require__(136)(STARTS_WITH), 'String', {
	  startsWith: function startsWith(searchString /* , position = 0 */) {
	    var that = context(this, searchString, STARTS_WITH);
	    var index = toLength(Math.min(arguments.length > 1 ? arguments[1] : undefined, that.length));
	    var search = String(searchString);
	    return $startsWith
	      ? $startsWith.call(that, search, index)
	      : that.slice(index, index + search.length) === search;
	  }
	});


/***/ }),
/* 140 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.2 String.prototype.anchor(name)
	__webpack_require__(141)('anchor', function (createHTML) {
	  return function anchor(name) {
	    return createHTML(this, 'a', 'name', name);
	  };
	});


/***/ }),
/* 141 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var fails = __webpack_require__(7);
	var defined = __webpack_require__(36);
	var quot = /"/g;
	// B.2.3.2.1 CreateHTML(string, tag, attribute, value)
	var createHTML = function (string, tag, attribute, value) {
	  var S = String(defined(string));
	  var p1 = '<' + tag;
	  if (attribute !== '') p1 += ' ' + attribute + '="' + String(value).replace(quot, '&quot;') + '"';
	  return p1 + '>' + S + '</' + tag + '>';
	};
	module.exports = function (NAME, exec) {
	  var O = {};
	  O[NAME] = exec(createHTML);
	  $export($export.P + $export.F * fails(function () {
	    var test = ''[NAME]('"');
	    return test !== test.toLowerCase() || test.split('"').length > 3;
	  }), 'String', O);
	};


/***/ }),
/* 142 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.3 String.prototype.big()
	__webpack_require__(141)('big', function (createHTML) {
	  return function big() {
	    return createHTML(this, 'big', '', '');
	  };
	});


/***/ }),
/* 143 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.4 String.prototype.blink()
	__webpack_require__(141)('blink', function (createHTML) {
	  return function blink() {
	    return createHTML(this, 'blink', '', '');
	  };
	});


/***/ }),
/* 144 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.5 String.prototype.bold()
	__webpack_require__(141)('bold', function (createHTML) {
	  return function bold() {
	    return createHTML(this, 'b', '', '');
	  };
	});


/***/ }),
/* 145 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.6 String.prototype.fixed()
	__webpack_require__(141)('fixed', function (createHTML) {
	  return function fixed() {
	    return createHTML(this, 'tt', '', '');
	  };
	});


/***/ }),
/* 146 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.7 String.prototype.fontcolor(color)
	__webpack_require__(141)('fontcolor', function (createHTML) {
	  return function fontcolor(color) {
	    return createHTML(this, 'font', 'color', color);
	  };
	});


/***/ }),
/* 147 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.8 String.prototype.fontsize(size)
	__webpack_require__(141)('fontsize', function (createHTML) {
	  return function fontsize(size) {
	    return createHTML(this, 'font', 'size', size);
	  };
	});


/***/ }),
/* 148 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.9 String.prototype.italics()
	__webpack_require__(141)('italics', function (createHTML) {
	  return function italics() {
	    return createHTML(this, 'i', '', '');
	  };
	});


/***/ }),
/* 149 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.10 String.prototype.link(url)
	__webpack_require__(141)('link', function (createHTML) {
	  return function link(url) {
	    return createHTML(this, 'a', 'href', url);
	  };
	});


/***/ }),
/* 150 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.11 String.prototype.small()
	__webpack_require__(141)('small', function (createHTML) {
	  return function small() {
	    return createHTML(this, 'small', '', '');
	  };
	});


/***/ }),
/* 151 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.12 String.prototype.strike()
	__webpack_require__(141)('strike', function (createHTML) {
	  return function strike() {
	    return createHTML(this, 'strike', '', '');
	  };
	});


/***/ }),
/* 152 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.13 String.prototype.sub()
	__webpack_require__(141)('sub', function (createHTML) {
	  return function sub() {
	    return createHTML(this, 'sub', '', '');
	  };
	});


/***/ }),
/* 153 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// B.2.3.14 String.prototype.sup()
	__webpack_require__(141)('sup', function (createHTML) {
	  return function sup() {
	    return createHTML(this, 'sup', '', '');
	  };
	});


/***/ }),
/* 154 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.3.3.1 / 15.9.4.4 Date.now()
	var $export = __webpack_require__(8);

	$export($export.S, 'Date', { now: function () { return new Date().getTime(); } });


/***/ }),
/* 155 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var toPrimitive = __webpack_require__(16);

	$export($export.P + $export.F * __webpack_require__(7)(function () {
	  return new Date(NaN).toJSON() !== null
	    || Date.prototype.toJSON.call({ toISOString: function () { return 1; } }) !== 1;
	}), 'Date', {
	  // eslint-disable-next-line no-unused-vars
	  toJSON: function toJSON(key) {
	    var O = toObject(this);
	    var pv = toPrimitive(O);
	    return typeof pv == 'number' && !isFinite(pv) ? null : O.toISOString();
	  }
	});


/***/ }),
/* 156 */
/***/ (function(module, exports, __webpack_require__) {

	// 20.3.4.36 / 15.9.5.43 Date.prototype.toISOString()
	var $export = __webpack_require__(8);
	var toISOString = __webpack_require__(157);

	// PhantomJS / old WebKit has a broken implementations
	$export($export.P + $export.F * (Date.prototype.toISOString !== toISOString), 'Date', {
	  toISOString: toISOString
	});


/***/ }),
/* 157 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 20.3.4.36 / 15.9.5.43 Date.prototype.toISOString()
	var fails = __webpack_require__(7);
	var getTime = Date.prototype.getTime;
	var $toISOString = Date.prototype.toISOString;

	var lz = function (num) {
	  return num > 9 ? num : '0' + num;
	};

	// PhantomJS / old WebKit has a broken implementations
	module.exports = (fails(function () {
	  return $toISOString.call(new Date(-5e13 - 1)) != '0385-07-25T07:06:39.999Z';
	}) || !fails(function () {
	  $toISOString.call(new Date(NaN));
	})) ? function toISOString() {
	  if (!isFinite(getTime.call(this))) throw RangeError('Invalid time value');
	  var d = this;
	  var y = d.getUTCFullYear();
	  var m = d.getUTCMilliseconds();
	  var s = y < 0 ? '-' : y > 9999 ? '+' : '';
	  return s + ('00000' + Math.abs(y)).slice(s ? -6 : -4) +
	    '-' + lz(d.getUTCMonth() + 1) + '-' + lz(d.getUTCDate()) +
	    'T' + lz(d.getUTCHours()) + ':' + lz(d.getUTCMinutes()) +
	    ':' + lz(d.getUTCSeconds()) + '.' + (m > 99 ? m : '0' + lz(m)) + 'Z';
	} : $toISOString;


/***/ }),
/* 158 */
/***/ (function(module, exports, __webpack_require__) {

	var DateProto = Date.prototype;
	var INVALID_DATE = 'Invalid Date';
	var TO_STRING = 'toString';
	var $toString = DateProto[TO_STRING];
	var getTime = DateProto.getTime;
	if (new Date(NaN) + '' != INVALID_DATE) {
	  __webpack_require__(18)(DateProto, TO_STRING, function toString() {
	    var value = getTime.call(this);
	    // eslint-disable-next-line no-self-compare
	    return value === value ? $toString.call(this) : INVALID_DATE;
	  });
	}


/***/ }),
/* 159 */
/***/ (function(module, exports, __webpack_require__) {

	var TO_PRIMITIVE = __webpack_require__(27)('toPrimitive');
	var proto = Date.prototype;

	if (!(TO_PRIMITIVE in proto)) __webpack_require__(10)(proto, TO_PRIMITIVE, __webpack_require__(160));


/***/ }),
/* 160 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var anObject = __webpack_require__(12);
	var toPrimitive = __webpack_require__(16);
	var NUMBER = 'number';

	module.exports = function (hint) {
	  if (hint !== 'string' && hint !== NUMBER && hint !== 'default') throw TypeError('Incorrect hint');
	  return toPrimitive(anObject(this), hint != NUMBER);
	};


/***/ }),
/* 161 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.2.2 / 15.4.3.2 Array.isArray(arg)
	var $export = __webpack_require__(8);

	$export($export.S, 'Array', { isArray: __webpack_require__(45) });


/***/ }),
/* 162 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var ctx = __webpack_require__(23);
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var call = __webpack_require__(163);
	var isArrayIter = __webpack_require__(164);
	var toLength = __webpack_require__(38);
	var createProperty = __webpack_require__(165);
	var getIterFn = __webpack_require__(166);

	$export($export.S + $export.F * !__webpack_require__(167)(function (iter) { Array.from(iter); }), 'Array', {
	  // 22.1.2.1 Array.from(arrayLike, mapfn = undefined, thisArg = undefined)
	  from: function from(arrayLike /* , mapfn = undefined, thisArg = undefined */) {
	    var O = toObject(arrayLike);
	    var C = typeof this == 'function' ? this : Array;
	    var aLen = arguments.length;
	    var mapfn = aLen > 1 ? arguments[1] : undefined;
	    var mapping = mapfn !== undefined;
	    var index = 0;
	    var iterFn = getIterFn(O);
	    var length, result, step, iterator;
	    if (mapping) mapfn = ctx(mapfn, aLen > 2 ? arguments[2] : undefined, 2);
	    // if object isn't iterable or it's array with default iterator - use simple case
	    if (iterFn != undefined && !(C == Array && isArrayIter(iterFn))) {
	      for (iterator = iterFn.call(O), result = new C(); !(step = iterator.next()).done; index++) {
	        createProperty(result, index, mapping ? call(iterator, mapfn, [step.value, index], true) : step.value);
	      }
	    } else {
	      length = toLength(O.length);
	      for (result = new C(length); length > index; index++) {
	        createProperty(result, index, mapping ? mapfn(O[index], index) : O[index]);
	      }
	    }
	    result.length = index;
	    return result;
	  }
	});


/***/ }),
/* 163 */
/***/ (function(module, exports, __webpack_require__) {

	// call something on iterator step with safe closing on error
	var anObject = __webpack_require__(12);
	module.exports = function (iterator, fn, value, entries) {
	  try {
	    return entries ? fn(anObject(value)[0], value[1]) : fn(value);
	  // 7.4.6 IteratorClose(iterator, completion)
	  } catch (e) {
	    var ret = iterator['return'];
	    if (ret !== undefined) anObject(ret.call(iterator));
	    throw e;
	  }
	};


/***/ }),
/* 164 */
/***/ (function(module, exports, __webpack_require__) {

	// check on default Array iterator
	var Iterators = __webpack_require__(130);
	var ITERATOR = __webpack_require__(27)('iterator');
	var ArrayProto = Array.prototype;

	module.exports = function (it) {
	  return it !== undefined && (Iterators.Array === it || ArrayProto[ITERATOR] === it);
	};


/***/ }),
/* 165 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $defineProperty = __webpack_require__(11);
	var createDesc = __webpack_require__(17);

	module.exports = function (object, index, value) {
	  if (index in object) $defineProperty.f(object, index, createDesc(0, value));
	  else object[index] = value;
	};


/***/ }),
/* 166 */
/***/ (function(module, exports, __webpack_require__) {

	var classof = __webpack_require__(75);
	var ITERATOR = __webpack_require__(27)('iterator');
	var Iterators = __webpack_require__(130);
	module.exports = __webpack_require__(9).getIteratorMethod = function (it) {
	  if (it != undefined) return it[ITERATOR]
	    || it['@@iterator']
	    || Iterators[classof(it)];
	};


/***/ }),
/* 167 */
/***/ (function(module, exports, __webpack_require__) {

	var ITERATOR = __webpack_require__(27)('iterator');
	var SAFE_CLOSING = false;

	try {
	  var riter = [7][ITERATOR]();
	  riter['return'] = function () { SAFE_CLOSING = true; };
	  // eslint-disable-next-line no-throw-literal
	  Array.from(riter, function () { throw 2; });
	} catch (e) { /* empty */ }

	module.exports = function (exec, skipClosing) {
	  if (!skipClosing && !SAFE_CLOSING) return false;
	  var safe = false;
	  try {
	    var arr = [7];
	    var iter = arr[ITERATOR]();
	    iter.next = function () { return { done: safe = true }; };
	    arr[ITERATOR] = function () { return iter; };
	    exec(arr);
	  } catch (e) { /* empty */ }
	  return safe;
	};


/***/ }),
/* 168 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var createProperty = __webpack_require__(165);

	// WebKit Array.of isn't generic
	$export($export.S + $export.F * __webpack_require__(7)(function () {
	  function F() { /* empty */ }
	  return !(Array.of.call(F) instanceof F);
	}), 'Array', {
	  // 22.1.2.3 Array.of( ...items)
	  of: function of(/* ...args */) {
	    var index = 0;
	    var aLen = arguments.length;
	    var result = new (typeof this == 'function' ? this : Array)(aLen);
	    while (aLen > index) createProperty(result, index, arguments[index++]);
	    result.length = aLen;
	    return result;
	  }
	});


/***/ }),
/* 169 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 22.1.3.13 Array.prototype.join(separator)
	var $export = __webpack_require__(8);
	var toIObject = __webpack_require__(33);
	var arrayJoin = [].join;

	// fallback for not array-like strings
	$export($export.P + $export.F * (__webpack_require__(34) != Object || !__webpack_require__(170)(arrayJoin)), 'Array', {
	  join: function join(separator) {
	    return arrayJoin.call(toIObject(this), separator === undefined ? ',' : separator);
	  }
	});


/***/ }),
/* 170 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var fails = __webpack_require__(7);

	module.exports = function (method, arg) {
	  return !!method && fails(function () {
	    // eslint-disable-next-line no-useless-call
	    arg ? method.call(null, function () { /* empty */ }, 1) : method.call(null);
	  });
	};


/***/ }),
/* 171 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var html = __webpack_require__(49);
	var cof = __webpack_require__(35);
	var toAbsoluteIndex = __webpack_require__(40);
	var toLength = __webpack_require__(38);
	var arraySlice = [].slice;

	// fallback for not array-like ES3 strings and DOM objects
	$export($export.P + $export.F * __webpack_require__(7)(function () {
	  if (html) arraySlice.call(html);
	}), 'Array', {
	  slice: function slice(begin, end) {
	    var len = toLength(this.length);
	    var klass = cof(this);
	    end = end === undefined ? len : end;
	    if (klass == 'Array') return arraySlice.call(this, begin, end);
	    var start = toAbsoluteIndex(begin, len);
	    var upTo = toAbsoluteIndex(end, len);
	    var size = toLength(upTo - start);
	    var cloned = new Array(size);
	    var i = 0;
	    for (; i < size; i++) cloned[i] = klass == 'String'
	      ? this.charAt(start + i)
	      : this[start + i];
	    return cloned;
	  }
	});


/***/ }),
/* 172 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var aFunction = __webpack_require__(24);
	var toObject = __webpack_require__(46);
	var fails = __webpack_require__(7);
	var $sort = [].sort;
	var test = [1, 2, 3];

	$export($export.P + $export.F * (fails(function () {
	  // IE8-
	  test.sort(undefined);
	}) || !fails(function () {
	  // V8 bug
	  test.sort(null);
	  // Old WebKit
	}) || !__webpack_require__(170)($sort)), 'Array', {
	  // 22.1.3.25 Array.prototype.sort(comparefn)
	  sort: function sort(comparefn) {
	    return comparefn === undefined
	      ? $sort.call(toObject(this))
	      : $sort.call(toObject(this), aFunction(comparefn));
	  }
	});


/***/ }),
/* 173 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $forEach = __webpack_require__(174)(0);
	var STRICT = __webpack_require__(170)([].forEach, true);

	$export($export.P + $export.F * !STRICT, 'Array', {
	  // 22.1.3.10 / 15.4.4.18 Array.prototype.forEach(callbackfn [, thisArg])
	  forEach: function forEach(callbackfn /* , thisArg */) {
	    return $forEach(this, callbackfn, arguments[1]);
	  }
	});


/***/ }),
/* 174 */
/***/ (function(module, exports, __webpack_require__) {

	// 0 -> Array#forEach
	// 1 -> Array#map
	// 2 -> Array#filter
	// 3 -> Array#some
	// 4 -> Array#every
	// 5 -> Array#find
	// 6 -> Array#findIndex
	var ctx = __webpack_require__(23);
	var IObject = __webpack_require__(34);
	var toObject = __webpack_require__(46);
	var toLength = __webpack_require__(38);
	var asc = __webpack_require__(175);
	module.exports = function (TYPE, $create) {
	  var IS_MAP = TYPE == 1;
	  var IS_FILTER = TYPE == 2;
	  var IS_SOME = TYPE == 3;
	  var IS_EVERY = TYPE == 4;
	  var IS_FIND_INDEX = TYPE == 6;
	  var NO_HOLES = TYPE == 5 || IS_FIND_INDEX;
	  var create = $create || asc;
	  return function ($this, callbackfn, that) {
	    var O = toObject($this);
	    var self = IObject(O);
	    var f = ctx(callbackfn, that, 3);
	    var length = toLength(self.length);
	    var index = 0;
	    var result = IS_MAP ? create($this, length) : IS_FILTER ? create($this, 0) : undefined;
	    var val, res;
	    for (;length > index; index++) if (NO_HOLES || index in self) {
	      val = self[index];
	      res = f(val, index, O);
	      if (TYPE) {
	        if (IS_MAP) result[index] = res;   // map
	        else if (res) switch (TYPE) {
	          case 3: return true;             // some
	          case 5: return val;              // find
	          case 6: return index;            // findIndex
	          case 2: result.push(val);        // filter
	        } else if (IS_EVERY) return false; // every
	      }
	    }
	    return IS_FIND_INDEX ? -1 : IS_SOME || IS_EVERY ? IS_EVERY : result;
	  };
	};


/***/ }),
/* 175 */
/***/ (function(module, exports, __webpack_require__) {

	// 9.4.2.3 ArraySpeciesCreate(originalArray, length)
	var speciesConstructor = __webpack_require__(176);

	module.exports = function (original, length) {
	  return new (speciesConstructor(original))(length);
	};


/***/ }),
/* 176 */
/***/ (function(module, exports, __webpack_require__) {

	var isObject = __webpack_require__(13);
	var isArray = __webpack_require__(45);
	var SPECIES = __webpack_require__(27)('species');

	module.exports = function (original) {
	  var C;
	  if (isArray(original)) {
	    C = original.constructor;
	    // cross-realm fallback
	    if (typeof C == 'function' && (C === Array || isArray(C.prototype))) C = undefined;
	    if (isObject(C)) {
	      C = C[SPECIES];
	      if (C === null) C = undefined;
	    }
	  } return C === undefined ? Array : C;
	};


/***/ }),
/* 177 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $map = __webpack_require__(174)(1);

	$export($export.P + $export.F * !__webpack_require__(170)([].map, true), 'Array', {
	  // 22.1.3.15 / 15.4.4.19 Array.prototype.map(callbackfn [, thisArg])
	  map: function map(callbackfn /* , thisArg */) {
	    return $map(this, callbackfn, arguments[1]);
	  }
	});


/***/ }),
/* 178 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $filter = __webpack_require__(174)(2);

	$export($export.P + $export.F * !__webpack_require__(170)([].filter, true), 'Array', {
	  // 22.1.3.7 / 15.4.4.20 Array.prototype.filter(callbackfn [, thisArg])
	  filter: function filter(callbackfn /* , thisArg */) {
	    return $filter(this, callbackfn, arguments[1]);
	  }
	});


/***/ }),
/* 179 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $some = __webpack_require__(174)(3);

	$export($export.P + $export.F * !__webpack_require__(170)([].some, true), 'Array', {
	  // 22.1.3.23 / 15.4.4.17 Array.prototype.some(callbackfn [, thisArg])
	  some: function some(callbackfn /* , thisArg */) {
	    return $some(this, callbackfn, arguments[1]);
	  }
	});


/***/ }),
/* 180 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $every = __webpack_require__(174)(4);

	$export($export.P + $export.F * !__webpack_require__(170)([].every, true), 'Array', {
	  // 22.1.3.5 / 15.4.4.16 Array.prototype.every(callbackfn [, thisArg])
	  every: function every(callbackfn /* , thisArg */) {
	    return $every(this, callbackfn, arguments[1]);
	  }
	});


/***/ }),
/* 181 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $reduce = __webpack_require__(182);

	$export($export.P + $export.F * !__webpack_require__(170)([].reduce, true), 'Array', {
	  // 22.1.3.18 / 15.4.4.21 Array.prototype.reduce(callbackfn [, initialValue])
	  reduce: function reduce(callbackfn /* , initialValue */) {
	    return $reduce(this, callbackfn, arguments.length, arguments[1], false);
	  }
	});


/***/ }),
/* 182 */
/***/ (function(module, exports, __webpack_require__) {

	var aFunction = __webpack_require__(24);
	var toObject = __webpack_require__(46);
	var IObject = __webpack_require__(34);
	var toLength = __webpack_require__(38);

	module.exports = function (that, callbackfn, aLen, memo, isRight) {
	  aFunction(callbackfn);
	  var O = toObject(that);
	  var self = IObject(O);
	  var length = toLength(O.length);
	  var index = isRight ? length - 1 : 0;
	  var i = isRight ? -1 : 1;
	  if (aLen < 2) for (;;) {
	    if (index in self) {
	      memo = self[index];
	      index += i;
	      break;
	    }
	    index += i;
	    if (isRight ? index < 0 : length <= index) {
	      throw TypeError('Reduce of empty array with no initial value');
	    }
	  }
	  for (;isRight ? index >= 0 : length > index; index += i) if (index in self) {
	    memo = callbackfn(memo, self[index], index, O);
	  }
	  return memo;
	};


/***/ }),
/* 183 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $reduce = __webpack_require__(182);

	$export($export.P + $export.F * !__webpack_require__(170)([].reduceRight, true), 'Array', {
	  // 22.1.3.19 / 15.4.4.22 Array.prototype.reduceRight(callbackfn [, initialValue])
	  reduceRight: function reduceRight(callbackfn /* , initialValue */) {
	    return $reduce(this, callbackfn, arguments.length, arguments[1], true);
	  }
	});


/***/ }),
/* 184 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $indexOf = __webpack_require__(37)(false);
	var $native = [].indexOf;
	var NEGATIVE_ZERO = !!$native && 1 / [1].indexOf(1, -0) < 0;

	$export($export.P + $export.F * (NEGATIVE_ZERO || !__webpack_require__(170)($native)), 'Array', {
	  // 22.1.3.11 / 15.4.4.14 Array.prototype.indexOf(searchElement [, fromIndex])
	  indexOf: function indexOf(searchElement /* , fromIndex = 0 */) {
	    return NEGATIVE_ZERO
	      // convert -0 to +0
	      ? $native.apply(this, arguments) || 0
	      : $indexOf(this, searchElement, arguments[1]);
	  }
	});


/***/ }),
/* 185 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toIObject = __webpack_require__(33);
	var toInteger = __webpack_require__(39);
	var toLength = __webpack_require__(38);
	var $native = [].lastIndexOf;
	var NEGATIVE_ZERO = !!$native && 1 / [1].lastIndexOf(1, -0) < 0;

	$export($export.P + $export.F * (NEGATIVE_ZERO || !__webpack_require__(170)($native)), 'Array', {
	  // 22.1.3.14 / 15.4.4.15 Array.prototype.lastIndexOf(searchElement [, fromIndex])
	  lastIndexOf: function lastIndexOf(searchElement /* , fromIndex = @[*-1] */) {
	    // convert -0 to +0
	    if (NEGATIVE_ZERO) return $native.apply(this, arguments) || 0;
	    var O = toIObject(this);
	    var length = toLength(O.length);
	    var index = length - 1;
	    if (arguments.length > 1) index = Math.min(index, toInteger(arguments[1]));
	    if (index < 0) index = length + index;
	    for (;index >= 0; index--) if (index in O) if (O[index] === searchElement) return index || 0;
	    return -1;
	  }
	});


/***/ }),
/* 186 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.3.3 Array.prototype.copyWithin(target, start, end = this.length)
	var $export = __webpack_require__(8);

	$export($export.P, 'Array', { copyWithin: __webpack_require__(187) });

	__webpack_require__(188)('copyWithin');


/***/ }),
/* 187 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.3.3 Array.prototype.copyWithin(target, start, end = this.length)
	'use strict';
	var toObject = __webpack_require__(46);
	var toAbsoluteIndex = __webpack_require__(40);
	var toLength = __webpack_require__(38);

	module.exports = [].copyWithin || function copyWithin(target /* = 0 */, start /* = 0, end = @length */) {
	  var O = toObject(this);
	  var len = toLength(O.length);
	  var to = toAbsoluteIndex(target, len);
	  var from = toAbsoluteIndex(start, len);
	  var end = arguments.length > 2 ? arguments[2] : undefined;
	  var count = Math.min((end === undefined ? len : toAbsoluteIndex(end, len)) - from, len - to);
	  var inc = 1;
	  if (from < to && to < from + count) {
	    inc = -1;
	    from += count - 1;
	    to += count - 1;
	  }
	  while (count-- > 0) {
	    if (from in O) O[to] = O[from];
	    else delete O[to];
	    to += inc;
	    from += inc;
	  } return O;
	};


/***/ }),
/* 188 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.3.31 Array.prototype[@@unscopables]
	var UNSCOPABLES = __webpack_require__(27)('unscopables');
	var ArrayProto = Array.prototype;
	if (ArrayProto[UNSCOPABLES] == undefined) __webpack_require__(10)(ArrayProto, UNSCOPABLES, {});
	module.exports = function (key) {
	  ArrayProto[UNSCOPABLES][key] = true;
	};


/***/ }),
/* 189 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.3.6 Array.prototype.fill(value, start = 0, end = this.length)
	var $export = __webpack_require__(8);

	$export($export.P, 'Array', { fill: __webpack_require__(190) });

	__webpack_require__(188)('fill');


/***/ }),
/* 190 */
/***/ (function(module, exports, __webpack_require__) {

	// 22.1.3.6 Array.prototype.fill(value, start = 0, end = this.length)
	'use strict';
	var toObject = __webpack_require__(46);
	var toAbsoluteIndex = __webpack_require__(40);
	var toLength = __webpack_require__(38);
	module.exports = function fill(value /* , start = 0, end = @length */) {
	  var O = toObject(this);
	  var length = toLength(O.length);
	  var aLen = arguments.length;
	  var index = toAbsoluteIndex(aLen > 1 ? arguments[1] : undefined, length);
	  var end = aLen > 2 ? arguments[2] : undefined;
	  var endPos = end === undefined ? length : toAbsoluteIndex(end, length);
	  while (endPos > index) O[index++] = value;
	  return O;
	};


/***/ }),
/* 191 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 22.1.3.8 Array.prototype.find(predicate, thisArg = undefined)
	var $export = __webpack_require__(8);
	var $find = __webpack_require__(174)(5);
	var KEY = 'find';
	var forced = true;
	// Shouldn't skip holes
	if (KEY in []) Array(1)[KEY](function () { forced = false; });
	$export($export.P + $export.F * forced, 'Array', {
	  find: function find(callbackfn /* , that = undefined */) {
	    return $find(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});
	__webpack_require__(188)(KEY);


/***/ }),
/* 192 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 22.1.3.9 Array.prototype.findIndex(predicate, thisArg = undefined)
	var $export = __webpack_require__(8);
	var $find = __webpack_require__(174)(6);
	var KEY = 'findIndex';
	var forced = true;
	// Shouldn't skip holes
	if (KEY in []) Array(1)[KEY](function () { forced = false; });
	$export($export.P + $export.F * forced, 'Array', {
	  findIndex: function findIndex(callbackfn /* , that = undefined */) {
	    return $find(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});
	__webpack_require__(188)(KEY);


/***/ }),
/* 193 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(194)('Array');


/***/ }),
/* 194 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var global = __webpack_require__(4);
	var dP = __webpack_require__(11);
	var DESCRIPTORS = __webpack_require__(6);
	var SPECIES = __webpack_require__(27)('species');

	module.exports = function (KEY) {
	  var C = global[KEY];
	  if (DESCRIPTORS && C && !C[SPECIES]) dP.f(C, SPECIES, {
	    configurable: true,
	    get: function () { return this; }
	  });
	};


/***/ }),
/* 195 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var addToUnscopables = __webpack_require__(188);
	var step = __webpack_require__(196);
	var Iterators = __webpack_require__(130);
	var toIObject = __webpack_require__(33);

	// 22.1.3.4 Array.prototype.entries()
	// 22.1.3.13 Array.prototype.keys()
	// 22.1.3.29 Array.prototype.values()
	// 22.1.3.30 Array.prototype[@@iterator]()
	module.exports = __webpack_require__(129)(Array, 'Array', function (iterated, kind) {
	  this._t = toIObject(iterated); // target
	  this._i = 0;                   // next index
	  this._k = kind;                // kind
	// 22.1.5.2.1 %ArrayIteratorPrototype%.next()
	}, function () {
	  var O = this._t;
	  var kind = this._k;
	  var index = this._i++;
	  if (!O || index >= O.length) {
	    this._t = undefined;
	    return step(1);
	  }
	  if (kind == 'keys') return step(0, index);
	  if (kind == 'values') return step(0, O[index]);
	  return step(0, [index, O[index]]);
	}, 'values');

	// argumentsList[@@iterator] is %ArrayProto_values% (9.4.4.6, 9.4.4.7)
	Iterators.Arguments = Iterators.Array;

	addToUnscopables('keys');
	addToUnscopables('values');
	addToUnscopables('entries');


/***/ }),
/* 196 */
/***/ (function(module, exports) {

	module.exports = function (done, value) {
	  return { value: value, done: !!done };
	};


/***/ }),
/* 197 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var inheritIfRequired = __webpack_require__(88);
	var dP = __webpack_require__(11).f;
	var gOPN = __webpack_require__(51).f;
	var isRegExp = __webpack_require__(135);
	var $flags = __webpack_require__(198);
	var $RegExp = global.RegExp;
	var Base = $RegExp;
	var proto = $RegExp.prototype;
	var re1 = /a/g;
	var re2 = /a/g;
	// "new" creates a new object, old webkit buggy here
	var CORRECT_NEW = new $RegExp(re1) !== re1;

	if (__webpack_require__(6) && (!CORRECT_NEW || __webpack_require__(7)(function () {
	  re2[__webpack_require__(27)('match')] = false;
	  // RegExp constructor can alter flags and IsRegExp works correct with @@match
	  return $RegExp(re1) != re1 || $RegExp(re2) == re2 || $RegExp(re1, 'i') != '/a/i';
	}))) {
	  $RegExp = function RegExp(p, f) {
	    var tiRE = this instanceof $RegExp;
	    var piRE = isRegExp(p);
	    var fiU = f === undefined;
	    return !tiRE && piRE && p.constructor === $RegExp && fiU ? p
	      : inheritIfRequired(CORRECT_NEW
	        ? new Base(piRE && !fiU ? p.source : p, f)
	        : Base((piRE = p instanceof $RegExp) ? p.source : p, piRE && fiU ? $flags.call(p) : f)
	      , tiRE ? this : proto, $RegExp);
	  };
	  var proxy = function (key) {
	    key in $RegExp || dP($RegExp, key, {
	      configurable: true,
	      get: function () { return Base[key]; },
	      set: function (it) { Base[key] = it; }
	    });
	  };
	  for (var keys = gOPN(Base), i = 0; keys.length > i;) proxy(keys[i++]);
	  proto.constructor = $RegExp;
	  $RegExp.prototype = proto;
	  __webpack_require__(18)(global, 'RegExp', $RegExp);
	}

	__webpack_require__(194)('RegExp');


/***/ }),
/* 198 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 21.2.5.3 get RegExp.prototype.flags
	var anObject = __webpack_require__(12);
	module.exports = function () {
	  var that = anObject(this);
	  var result = '';
	  if (that.global) result += 'g';
	  if (that.ignoreCase) result += 'i';
	  if (that.multiline) result += 'm';
	  if (that.unicode) result += 'u';
	  if (that.sticky) result += 'y';
	  return result;
	};


/***/ }),
/* 199 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var regexpExec = __webpack_require__(200);
	__webpack_require__(8)({
	  target: 'RegExp',
	  proto: true,
	  forced: regexpExec !== /./.exec
	}, {
	  exec: regexpExec
	});


/***/ }),
/* 200 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var regexpFlags = __webpack_require__(198);

	var nativeExec = RegExp.prototype.exec;
	// This always refers to the native implementation, because the
	// String#replace polyfill uses ./fix-regexp-well-known-symbol-logic.js,
	// which loads this file before patching the method.
	var nativeReplace = String.prototype.replace;

	var patchedExec = nativeExec;

	var LAST_INDEX = 'lastIndex';

	var UPDATES_LAST_INDEX_WRONG = (function () {
	  var re1 = /a/,
	      re2 = /b*/g;
	  nativeExec.call(re1, 'a');
	  nativeExec.call(re2, 'a');
	  return re1[LAST_INDEX] !== 0 || re2[LAST_INDEX] !== 0;
	})();

	// nonparticipating capturing group, copied from es5-shim's String#split patch.
	var NPCG_INCLUDED = /()??/.exec('')[1] !== undefined;

	var PATCH = UPDATES_LAST_INDEX_WRONG || NPCG_INCLUDED;

	if (PATCH) {
	  patchedExec = function exec(str) {
	    var re = this;
	    var lastIndex, reCopy, match, i;

	    if (NPCG_INCLUDED) {
	      reCopy = new RegExp('^' + re.source + '$(?!\\s)', regexpFlags.call(re));
	    }
	    if (UPDATES_LAST_INDEX_WRONG) lastIndex = re[LAST_INDEX];

	    match = nativeExec.call(re, str);

	    if (UPDATES_LAST_INDEX_WRONG && match) {
	      re[LAST_INDEX] = re.global ? match.index + match[0].length : lastIndex;
	    }
	    if (NPCG_INCLUDED && match && match.length > 1) {
	      // Fix browsers whose `exec` methods don't consistently return `undefined`
	      // for NPCG, like IE8. NOTE: This doesn' work for /(.?)?/
	      // eslint-disable-next-line no-loop-func
	      nativeReplace.call(match[0], reCopy, function () {
	        for (i = 1; i < arguments.length - 2; i++) {
	          if (arguments[i] === undefined) match[i] = undefined;
	        }
	      });
	    }

	    return match;
	  };
	}

	module.exports = patchedExec;


/***/ }),
/* 201 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	__webpack_require__(202);
	var anObject = __webpack_require__(12);
	var $flags = __webpack_require__(198);
	var DESCRIPTORS = __webpack_require__(6);
	var TO_STRING = 'toString';
	var $toString = /./[TO_STRING];

	var define = function (fn) {
	  __webpack_require__(18)(RegExp.prototype, TO_STRING, fn, true);
	};

	// 21.2.5.14 RegExp.prototype.toString()
	if (__webpack_require__(7)(function () { return $toString.call({ source: 'a', flags: 'b' }) != '/a/b'; })) {
	  define(function toString() {
	    var R = anObject(this);
	    return '/'.concat(R.source, '/',
	      'flags' in R ? R.flags : !DESCRIPTORS && R instanceof RegExp ? $flags.call(R) : undefined);
	  });
	// FF44- RegExp#toString has a wrong name
	} else if ($toString.name != TO_STRING) {
	  define(function toString() {
	    return $toString.call(this);
	  });
	}


/***/ }),
/* 202 */
/***/ (function(module, exports, __webpack_require__) {

	// 21.2.5.3 get RegExp.prototype.flags()
	if (__webpack_require__(6) && /./g.flags != 'g') __webpack_require__(11).f(RegExp.prototype, 'flags', {
	  configurable: true,
	  get: __webpack_require__(198)
	});


/***/ }),
/* 203 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var anObject = __webpack_require__(12);
	var toLength = __webpack_require__(38);
	var advanceStringIndex = __webpack_require__(204);
	var regExpExec = __webpack_require__(205);

	// @@match logic
	__webpack_require__(206)('match', 1, function (defined, MATCH, $match, maybeCallNative) {
	  return [
	    // `String.prototype.match` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.match
	    function match(regexp) {
	      var O = defined(this);
	      var fn = regexp == undefined ? undefined : regexp[MATCH];
	      return fn !== undefined ? fn.call(regexp, O) : new RegExp(regexp)[MATCH](String(O));
	    },
	    // `RegExp.prototype[@@match]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@match
	    function (regexp) {
	      var res = maybeCallNative($match, regexp, this);
	      if (res.done) return res.value;
	      var rx = anObject(regexp);
	      var S = String(this);
	      if (!rx.global) return regExpExec(rx, S);
	      var fullUnicode = rx.unicode;
	      rx.lastIndex = 0;
	      var A = [];
	      var n = 0;
	      var result;
	      while ((result = regExpExec(rx, S)) !== null) {
	        var matchStr = String(result[0]);
	        A[n] = matchStr;
	        if (matchStr === '') rx.lastIndex = advanceStringIndex(S, toLength(rx.lastIndex), fullUnicode);
	        n++;
	      }
	      return n === 0 ? null : A;
	    }
	  ];
	});


/***/ }),
/* 204 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var at = __webpack_require__(128)(true);

	 // `AdvanceStringIndex` abstract operation
	// https://tc39.github.io/ecma262/#sec-advancestringindex
	module.exports = function (S, index, unicode) {
	  return index + (unicode ? at(S, index).length : 1);
	};


/***/ }),
/* 205 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var classof = __webpack_require__(75);
	var builtinExec = RegExp.prototype.exec;

	 // `RegExpExec` abstract operation
	// https://tc39.github.io/ecma262/#sec-regexpexec
	module.exports = function (R, S) {
	  var exec = R.exec;
	  if (typeof exec === 'function') {
	    var result = exec.call(R, S);
	    if (typeof result !== 'object') {
	      throw new TypeError('RegExp exec method returned something other than an Object or null');
	    }
	    return result;
	  }
	  if (classof(R) !== 'RegExp') {
	    throw new TypeError('RegExp#exec called on incompatible receiver');
	  }
	  return builtinExec.call(R, S);
	};


/***/ }),
/* 206 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	__webpack_require__(199);
	var redefine = __webpack_require__(18);
	var hide = __webpack_require__(10);
	var fails = __webpack_require__(7);
	var defined = __webpack_require__(36);
	var wks = __webpack_require__(27);
	var regexpExec = __webpack_require__(200);

	var SPECIES = wks('species');

	var REPLACE_SUPPORTS_NAMED_GROUPS = !fails(function () {
	  // #replace needs built-in support for named groups.
	  // #match works fine because it just return the exec results, even if it has
	  // a "grops" property.
	  var re = /./;
	  re.exec = function () {
	    var result = [];
	    result.groups = { a: '7' };
	    return result;
	  };
	  return ''.replace(re, '$<a>') !== '7';
	});

	var SPLIT_WORKS_WITH_OVERWRITTEN_EXEC = (function () {
	  // Chrome 51 has a buggy "split" implementation when RegExp#exec !== nativeExec
	  var re = /(?:)/;
	  var originalExec = re.exec;
	  re.exec = function () { return originalExec.apply(this, arguments); };
	  var result = 'ab'.split(re);
	  return result.length === 2 && result[0] === 'a' && result[1] === 'b';
	})();

	module.exports = function (KEY, length, exec) {
	  var SYMBOL = wks(KEY);

	  var DELEGATES_TO_SYMBOL = !fails(function () {
	    // String methods call symbol-named RegEp methods
	    var O = {};
	    O[SYMBOL] = function () { return 7; };
	    return ''[KEY](O) != 7;
	  });

	  var DELEGATES_TO_EXEC = DELEGATES_TO_SYMBOL ? !fails(function () {
	    // Symbol-named RegExp methods call .exec
	    var execCalled = false;
	    var re = /a/;
	    re.exec = function () { execCalled = true; return null; };
	    if (KEY === 'split') {
	      // RegExp[@@split] doesn't call the regex's exec method, but first creates
	      // a new one. We need to return the patched regex when creating the new one.
	      re.constructor = {};
	      re.constructor[SPECIES] = function () { return re; };
	    }
	    re[SYMBOL]('');
	    return !execCalled;
	  }) : undefined;

	  if (
	    !DELEGATES_TO_SYMBOL ||
	    !DELEGATES_TO_EXEC ||
	    (KEY === 'replace' && !REPLACE_SUPPORTS_NAMED_GROUPS) ||
	    (KEY === 'split' && !SPLIT_WORKS_WITH_OVERWRITTEN_EXEC)
	  ) {
	    var nativeRegExpMethod = /./[SYMBOL];
	    var fns = exec(
	      defined,
	      SYMBOL,
	      ''[KEY],
	      function maybeCallNative(nativeMethod, regexp, str, arg2, forceStringMethod) {
	        if (regexp.exec === regexpExec) {
	          if (DELEGATES_TO_SYMBOL && !forceStringMethod) {
	            // The native String method already delegates to @@method (this
	            // polyfilled function), leasing to infinite recursion.
	            // We avoid it by directly calling the native @@method method.
	            return { done: true, value: nativeRegExpMethod.call(regexp, str, arg2) };
	          }
	          return { done: true, value: nativeMethod.call(str, regexp, arg2) };
	        }
	        return { done: false };
	      }
	    );
	    var strfn = fns[0];
	    var rxfn = fns[1];

	    redefine(String.prototype, KEY, strfn);
	    hide(RegExp.prototype, SYMBOL, length == 2
	      // 21.2.5.8 RegExp.prototype[@@replace](string, replaceValue)
	      // 21.2.5.11 RegExp.prototype[@@split](string, limit)
	      ? function (string, arg) { return rxfn.call(string, this, arg); }
	      // 21.2.5.6 RegExp.prototype[@@match](string)
	      // 21.2.5.9 RegExp.prototype[@@search](string)
	      : function (string) { return rxfn.call(string, this); }
	    );
	  }
	};


/***/ }),
/* 207 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var anObject = __webpack_require__(12);
	var toObject = __webpack_require__(46);
	var toLength = __webpack_require__(38);
	var toInteger = __webpack_require__(39);
	var advanceStringIndex = __webpack_require__(204);
	var regExpExec = __webpack_require__(205);
	var max = Math.max;
	var min = Math.min;
	var floor = Math.floor;
	var SUBSTITUTION_SYMBOLS = /\$([$&`']|\d\d?|<[^>]*>)/g;
	var SUBSTITUTION_SYMBOLS_NO_NAMED = /\$([$&`']|\d\d?)/g;

	var maybeToString = function (it) {
	  return it === undefined ? it : String(it);
	};

	// @@replace logic
	__webpack_require__(206)('replace', 2, function (defined, REPLACE, $replace, maybeCallNative) {
	  return [
	    // `String.prototype.replace` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.replace
	    function replace(searchValue, replaceValue) {
	      var O = defined(this);
	      var fn = searchValue == undefined ? undefined : searchValue[REPLACE];
	      return fn !== undefined
	        ? fn.call(searchValue, O, replaceValue)
	        : $replace.call(String(O), searchValue, replaceValue);
	    },
	    // `RegExp.prototype[@@replace]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@replace
	    function (regexp, replaceValue) {
	      var res = maybeCallNative($replace, regexp, this, replaceValue);
	      if (res.done) return res.value;

	      var rx = anObject(regexp);
	      var S = String(this);
	      var functionalReplace = typeof replaceValue === 'function';
	      if (!functionalReplace) replaceValue = String(replaceValue);
	      var global = rx.global;
	      if (global) {
	        var fullUnicode = rx.unicode;
	        rx.lastIndex = 0;
	      }
	      var results = [];
	      while (true) {
	        var result = regExpExec(rx, S);
	        if (result === null) break;
	        results.push(result);
	        if (!global) break;
	        var matchStr = String(result[0]);
	        if (matchStr === '') rx.lastIndex = advanceStringIndex(S, toLength(rx.lastIndex), fullUnicode);
	      }
	      var accumulatedResult = '';
	      var nextSourcePosition = 0;
	      for (var i = 0; i < results.length; i++) {
	        result = results[i];
	        var matched = String(result[0]);
	        var position = max(min(toInteger(result.index), S.length), 0);
	        var captures = [];
	        // NOTE: This is equivalent to
	        //   captures = result.slice(1).map(maybeToString)
	        // but for some reason `nativeSlice.call(result, 1, result.length)` (called in
	        // the slice polyfill when slicing native arrays) "doesn't work" in safari 9 and
	        // causes a crash (https://pastebin.com/N21QzeQA) when trying to debug it.
	        for (var j = 1; j < result.length; j++) captures.push(maybeToString(result[j]));
	        var namedCaptures = result.groups;
	        if (functionalReplace) {
	          var replacerArgs = [matched].concat(captures, position, S);
	          if (namedCaptures !== undefined) replacerArgs.push(namedCaptures);
	          var replacement = String(replaceValue.apply(undefined, replacerArgs));
	        } else {
	          replacement = getSubstitution(matched, S, position, captures, namedCaptures, replaceValue);
	        }
	        if (position >= nextSourcePosition) {
	          accumulatedResult += S.slice(nextSourcePosition, position) + replacement;
	          nextSourcePosition = position + matched.length;
	        }
	      }
	      return accumulatedResult + S.slice(nextSourcePosition);
	    }
	  ];

	    // https://tc39.github.io/ecma262/#sec-getsubstitution
	  function getSubstitution(matched, str, position, captures, namedCaptures, replacement) {
	    var tailPos = position + matched.length;
	    var m = captures.length;
	    var symbols = SUBSTITUTION_SYMBOLS_NO_NAMED;
	    if (namedCaptures !== undefined) {
	      namedCaptures = toObject(namedCaptures);
	      symbols = SUBSTITUTION_SYMBOLS;
	    }
	    return $replace.call(replacement, symbols, function (match, ch) {
	      var capture;
	      switch (ch.charAt(0)) {
	        case '$': return '$';
	        case '&': return matched;
	        case '`': return str.slice(0, position);
	        case "'": return str.slice(tailPos);
	        case '<':
	          capture = namedCaptures[ch.slice(1, -1)];
	          break;
	        default: // \d\d?
	          var n = +ch;
	          if (n === 0) return match;
	          if (n > m) {
	            var f = floor(n / 10);
	            if (f === 0) return match;
	            if (f <= m) return captures[f - 1] === undefined ? ch.charAt(1) : captures[f - 1] + ch.charAt(1);
	            return match;
	          }
	          capture = captures[n - 1];
	      }
	      return capture === undefined ? '' : capture;
	    });
	  }
	});


/***/ }),
/* 208 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var anObject = __webpack_require__(12);
	var sameValue = __webpack_require__(71);
	var regExpExec = __webpack_require__(205);

	// @@search logic
	__webpack_require__(206)('search', 1, function (defined, SEARCH, $search, maybeCallNative) {
	  return [
	    // `String.prototype.search` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.search
	    function search(regexp) {
	      var O = defined(this);
	      var fn = regexp == undefined ? undefined : regexp[SEARCH];
	      return fn !== undefined ? fn.call(regexp, O) : new RegExp(regexp)[SEARCH](String(O));
	    },
	    // `RegExp.prototype[@@search]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@search
	    function (regexp) {
	      var res = maybeCallNative($search, regexp, this);
	      if (res.done) return res.value;
	      var rx = anObject(regexp);
	      var S = String(this);
	      var previousLastIndex = rx.lastIndex;
	      if (!sameValue(previousLastIndex, 0)) rx.lastIndex = 0;
	      var result = regExpExec(rx, S);
	      if (!sameValue(rx.lastIndex, previousLastIndex)) rx.lastIndex = previousLastIndex;
	      return result === null ? -1 : result.index;
	    }
	  ];
	});


/***/ }),
/* 209 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var isRegExp = __webpack_require__(135);
	var anObject = __webpack_require__(12);
	var speciesConstructor = __webpack_require__(210);
	var advanceStringIndex = __webpack_require__(204);
	var toLength = __webpack_require__(38);
	var callRegExpExec = __webpack_require__(205);
	var regexpExec = __webpack_require__(200);
	var fails = __webpack_require__(7);
	var $min = Math.min;
	var $push = [].push;
	var $SPLIT = 'split';
	var LENGTH = 'length';
	var LAST_INDEX = 'lastIndex';
	var MAX_UINT32 = 0xffffffff;

	// babel-minify transpiles RegExp('x', 'y') -> /x/y and it causes SyntaxError
	var SUPPORTS_Y = !fails(function () { RegExp(MAX_UINT32, 'y'); });

	// @@split logic
	__webpack_require__(206)('split', 2, function (defined, SPLIT, $split, maybeCallNative) {
	  var internalSplit;
	  if (
	    'abbc'[$SPLIT](/(b)*/)[1] == 'c' ||
	    'test'[$SPLIT](/(?:)/, -1)[LENGTH] != 4 ||
	    'ab'[$SPLIT](/(?:ab)*/)[LENGTH] != 2 ||
	    '.'[$SPLIT](/(.?)(.?)/)[LENGTH] != 4 ||
	    '.'[$SPLIT](/()()/)[LENGTH] > 1 ||
	    ''[$SPLIT](/.?/)[LENGTH]
	  ) {
	    // based on es5-shim implementation, need to rework it
	    internalSplit = function (separator, limit) {
	      var string = String(this);
	      if (separator === undefined && limit === 0) return [];
	      // If `separator` is not a regex, use native split
	      if (!isRegExp(separator)) return $split.call(string, separator, limit);
	      var output = [];
	      var flags = (separator.ignoreCase ? 'i' : '') +
	                  (separator.multiline ? 'm' : '') +
	                  (separator.unicode ? 'u' : '') +
	                  (separator.sticky ? 'y' : '');
	      var lastLastIndex = 0;
	      var splitLimit = limit === undefined ? MAX_UINT32 : limit >>> 0;
	      // Make `global` and avoid `lastIndex` issues by working with a copy
	      var separatorCopy = new RegExp(separator.source, flags + 'g');
	      var match, lastIndex, lastLength;
	      while (match = regexpExec.call(separatorCopy, string)) {
	        lastIndex = separatorCopy[LAST_INDEX];
	        if (lastIndex > lastLastIndex) {
	          output.push(string.slice(lastLastIndex, match.index));
	          if (match[LENGTH] > 1 && match.index < string[LENGTH]) $push.apply(output, match.slice(1));
	          lastLength = match[0][LENGTH];
	          lastLastIndex = lastIndex;
	          if (output[LENGTH] >= splitLimit) break;
	        }
	        if (separatorCopy[LAST_INDEX] === match.index) separatorCopy[LAST_INDEX]++; // Avoid an infinite loop
	      }
	      if (lastLastIndex === string[LENGTH]) {
	        if (lastLength || !separatorCopy.test('')) output.push('');
	      } else output.push(string.slice(lastLastIndex));
	      return output[LENGTH] > splitLimit ? output.slice(0, splitLimit) : output;
	    };
	  // Chakra, V8
	  } else if ('0'[$SPLIT](undefined, 0)[LENGTH]) {
	    internalSplit = function (separator, limit) {
	      return separator === undefined && limit === 0 ? [] : $split.call(this, separator, limit);
	    };
	  } else {
	    internalSplit = $split;
	  }

	  return [
	    // `String.prototype.split` method
	    // https://tc39.github.io/ecma262/#sec-string.prototype.split
	    function split(separator, limit) {
	      var O = defined(this);
	      var splitter = separator == undefined ? undefined : separator[SPLIT];
	      return splitter !== undefined
	        ? splitter.call(separator, O, limit)
	        : internalSplit.call(String(O), separator, limit);
	    },
	    // `RegExp.prototype[@@split]` method
	    // https://tc39.github.io/ecma262/#sec-regexp.prototype-@@split
	    //
	    // NOTE: This cannot be properly polyfilled in engines that don't support
	    // the 'y' flag.
	    function (regexp, limit) {
	      var res = maybeCallNative(internalSplit, regexp, this, limit, internalSplit !== $split);
	      if (res.done) return res.value;

	      var rx = anObject(regexp);
	      var S = String(this);
	      var C = speciesConstructor(rx, RegExp);

	      var unicodeMatching = rx.unicode;
	      var flags = (rx.ignoreCase ? 'i' : '') +
	                  (rx.multiline ? 'm' : '') +
	                  (rx.unicode ? 'u' : '') +
	                  (SUPPORTS_Y ? 'y' : 'g');

	      // ^(? + rx + ) is needed, in combination with some S slicing, to
	      // simulate the 'y' flag.
	      var splitter = new C(SUPPORTS_Y ? rx : '^(?:' + rx.source + ')', flags);
	      var lim = limit === undefined ? MAX_UINT32 : limit >>> 0;
	      if (lim === 0) return [];
	      if (S.length === 0) return callRegExpExec(splitter, S) === null ? [S] : [];
	      var p = 0;
	      var q = 0;
	      var A = [];
	      while (q < S.length) {
	        splitter.lastIndex = SUPPORTS_Y ? q : 0;
	        var z = callRegExpExec(splitter, SUPPORTS_Y ? S : S.slice(q));
	        var e;
	        if (
	          z === null ||
	          (e = $min(toLength(splitter.lastIndex + (SUPPORTS_Y ? 0 : q)), S.length)) === p
	        ) {
	          q = advanceStringIndex(S, q, unicodeMatching);
	        } else {
	          A.push(S.slice(p, q));
	          if (A.length === lim) return A;
	          for (var i = 1; i <= z.length - 1; i++) {
	            A.push(z[i]);
	            if (A.length === lim) return A;
	          }
	          q = p = e;
	        }
	      }
	      A.push(S.slice(p));
	      return A;
	    }
	  ];
	});


/***/ }),
/* 210 */
/***/ (function(module, exports, __webpack_require__) {

	// 7.3.20 SpeciesConstructor(O, defaultConstructor)
	var anObject = __webpack_require__(12);
	var aFunction = __webpack_require__(24);
	var SPECIES = __webpack_require__(27)('species');
	module.exports = function (O, D) {
	  var C = anObject(O).constructor;
	  var S;
	  return C === undefined || (S = anObject(C)[SPECIES]) == undefined ? D : aFunction(S);
	};


/***/ }),
/* 211 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var LIBRARY = __webpack_require__(22);
	var global = __webpack_require__(4);
	var ctx = __webpack_require__(23);
	var classof = __webpack_require__(75);
	var $export = __webpack_require__(8);
	var isObject = __webpack_require__(13);
	var aFunction = __webpack_require__(24);
	var anInstance = __webpack_require__(212);
	var forOf = __webpack_require__(213);
	var speciesConstructor = __webpack_require__(210);
	var task = __webpack_require__(214).set;
	var microtask = __webpack_require__(215)();
	var newPromiseCapabilityModule = __webpack_require__(216);
	var perform = __webpack_require__(217);
	var userAgent = __webpack_require__(218);
	var promiseResolve = __webpack_require__(219);
	var PROMISE = 'Promise';
	var TypeError = global.TypeError;
	var process = global.process;
	var versions = process && process.versions;
	var v8 = versions && versions.v8 || '';
	var $Promise = global[PROMISE];
	var isNode = classof(process) == 'process';
	var empty = function () { /* empty */ };
	var Internal, newGenericPromiseCapability, OwnPromiseCapability, Wrapper;
	var newPromiseCapability = newGenericPromiseCapability = newPromiseCapabilityModule.f;

	var USE_NATIVE = !!function () {
	  try {
	    // correct subclassing with @@species support
	    var promise = $Promise.resolve(1);
	    var FakePromise = (promise.constructor = {})[__webpack_require__(27)('species')] = function (exec) {
	      exec(empty, empty);
	    };
	    // unhandled rejections tracking support, NodeJS Promise without it fails @@species test
	    return (isNode || typeof PromiseRejectionEvent == 'function')
	      && promise.then(empty) instanceof FakePromise
	      // v8 6.6 (Node 10 and Chrome 66) have a bug with resolving custom thenables
	      // https://bugs.chromium.org/p/chromium/issues/detail?id=830565
	      // we can't detect it synchronously, so just check versions
	      && v8.indexOf('6.6') !== 0
	      && userAgent.indexOf('Chrome/66') === -1;
	  } catch (e) { /* empty */ }
	}();

	// helpers
	var isThenable = function (it) {
	  var then;
	  return isObject(it) && typeof (then = it.then) == 'function' ? then : false;
	};
	var notify = function (promise, isReject) {
	  if (promise._n) return;
	  promise._n = true;
	  var chain = promise._c;
	  microtask(function () {
	    var value = promise._v;
	    var ok = promise._s == 1;
	    var i = 0;
	    var run = function (reaction) {
	      var handler = ok ? reaction.ok : reaction.fail;
	      var resolve = reaction.resolve;
	      var reject = reaction.reject;
	      var domain = reaction.domain;
	      var result, then, exited;
	      try {
	        if (handler) {
	          if (!ok) {
	            if (promise._h == 2) onHandleUnhandled(promise);
	            promise._h = 1;
	          }
	          if (handler === true) result = value;
	          else {
	            if (domain) domain.enter();
	            result = handler(value); // may throw
	            if (domain) {
	              domain.exit();
	              exited = true;
	            }
	          }
	          if (result === reaction.promise) {
	            reject(TypeError('Promise-chain cycle'));
	          } else if (then = isThenable(result)) {
	            then.call(result, resolve, reject);
	          } else resolve(result);
	        } else reject(value);
	      } catch (e) {
	        if (domain && !exited) domain.exit();
	        reject(e);
	      }
	    };
	    while (chain.length > i) run(chain[i++]); // variable length - can't use forEach
	    promise._c = [];
	    promise._n = false;
	    if (isReject && !promise._h) onUnhandled(promise);
	  });
	};
	var onUnhandled = function (promise) {
	  task.call(global, function () {
	    var value = promise._v;
	    var unhandled = isUnhandled(promise);
	    var result, handler, console;
	    if (unhandled) {
	      result = perform(function () {
	        if (isNode) {
	          process.emit('unhandledRejection', value, promise);
	        } else if (handler = global.onunhandledrejection) {
	          handler({ promise: promise, reason: value });
	        } else if ((console = global.console) && console.error) {
	          console.error('Unhandled promise rejection', value);
	        }
	      });
	      // Browsers should not trigger `rejectionHandled` event if it was handled here, NodeJS - should
	      promise._h = isNode || isUnhandled(promise) ? 2 : 1;
	    } promise._a = undefined;
	    if (unhandled && result.e) throw result.v;
	  });
	};
	var isUnhandled = function (promise) {
	  return promise._h !== 1 && (promise._a || promise._c).length === 0;
	};
	var onHandleUnhandled = function (promise) {
	  task.call(global, function () {
	    var handler;
	    if (isNode) {
	      process.emit('rejectionHandled', promise);
	    } else if (handler = global.onrejectionhandled) {
	      handler({ promise: promise, reason: promise._v });
	    }
	  });
	};
	var $reject = function (value) {
	  var promise = this;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap
	  promise._v = value;
	  promise._s = 2;
	  if (!promise._a) promise._a = promise._c.slice();
	  notify(promise, true);
	};
	var $resolve = function (value) {
	  var promise = this;
	  var then;
	  if (promise._d) return;
	  promise._d = true;
	  promise = promise._w || promise; // unwrap
	  try {
	    if (promise === value) throw TypeError("Promise can't be resolved itself");
	    if (then = isThenable(value)) {
	      microtask(function () {
	        var wrapper = { _w: promise, _d: false }; // wrap
	        try {
	          then.call(value, ctx($resolve, wrapper, 1), ctx($reject, wrapper, 1));
	        } catch (e) {
	          $reject.call(wrapper, e);
	        }
	      });
	    } else {
	      promise._v = value;
	      promise._s = 1;
	      notify(promise, false);
	    }
	  } catch (e) {
	    $reject.call({ _w: promise, _d: false }, e); // wrap
	  }
	};

	// constructor polyfill
	if (!USE_NATIVE) {
	  // 25.4.3.1 Promise(executor)
	  $Promise = function Promise(executor) {
	    anInstance(this, $Promise, PROMISE, '_h');
	    aFunction(executor);
	    Internal.call(this);
	    try {
	      executor(ctx($resolve, this, 1), ctx($reject, this, 1));
	    } catch (err) {
	      $reject.call(this, err);
	    }
	  };
	  // eslint-disable-next-line no-unused-vars
	  Internal = function Promise(executor) {
	    this._c = [];             // <- awaiting reactions
	    this._a = undefined;      // <- checked in isUnhandled reactions
	    this._s = 0;              // <- state
	    this._d = false;          // <- done
	    this._v = undefined;      // <- value
	    this._h = 0;              // <- rejection state, 0 - default, 1 - handled, 2 - unhandled
	    this._n = false;          // <- notify
	  };
	  Internal.prototype = __webpack_require__(220)($Promise.prototype, {
	    // 25.4.5.3 Promise.prototype.then(onFulfilled, onRejected)
	    then: function then(onFulfilled, onRejected) {
	      var reaction = newPromiseCapability(speciesConstructor(this, $Promise));
	      reaction.ok = typeof onFulfilled == 'function' ? onFulfilled : true;
	      reaction.fail = typeof onRejected == 'function' && onRejected;
	      reaction.domain = isNode ? process.domain : undefined;
	      this._c.push(reaction);
	      if (this._a) this._a.push(reaction);
	      if (this._s) notify(this, false);
	      return reaction.promise;
	    },
	    // 25.4.5.1 Promise.prototype.catch(onRejected)
	    'catch': function (onRejected) {
	      return this.then(undefined, onRejected);
	    }
	  });
	  OwnPromiseCapability = function () {
	    var promise = new Internal();
	    this.promise = promise;
	    this.resolve = ctx($resolve, promise, 1);
	    this.reject = ctx($reject, promise, 1);
	  };
	  newPromiseCapabilityModule.f = newPromiseCapability = function (C) {
	    return C === $Promise || C === Wrapper
	      ? new OwnPromiseCapability(C)
	      : newGenericPromiseCapability(C);
	  };
	}

	$export($export.G + $export.W + $export.F * !USE_NATIVE, { Promise: $Promise });
	__webpack_require__(26)($Promise, PROMISE);
	__webpack_require__(194)(PROMISE);
	Wrapper = __webpack_require__(9)[PROMISE];

	// statics
	$export($export.S + $export.F * !USE_NATIVE, PROMISE, {
	  // 25.4.4.5 Promise.reject(r)
	  reject: function reject(r) {
	    var capability = newPromiseCapability(this);
	    var $$reject = capability.reject;
	    $$reject(r);
	    return capability.promise;
	  }
	});
	$export($export.S + $export.F * (LIBRARY || !USE_NATIVE), PROMISE, {
	  // 25.4.4.6 Promise.resolve(x)
	  resolve: function resolve(x) {
	    return promiseResolve(LIBRARY && this === Wrapper ? $Promise : this, x);
	  }
	});
	$export($export.S + $export.F * !(USE_NATIVE && __webpack_require__(167)(function (iter) {
	  $Promise.all(iter)['catch'](empty);
	})), PROMISE, {
	  // 25.4.4.1 Promise.all(iterable)
	  all: function all(iterable) {
	    var C = this;
	    var capability = newPromiseCapability(C);
	    var resolve = capability.resolve;
	    var reject = capability.reject;
	    var result = perform(function () {
	      var values = [];
	      var index = 0;
	      var remaining = 1;
	      forOf(iterable, false, function (promise) {
	        var $index = index++;
	        var alreadyCalled = false;
	        values.push(undefined);
	        remaining++;
	        C.resolve(promise).then(function (value) {
	          if (alreadyCalled) return;
	          alreadyCalled = true;
	          values[$index] = value;
	          --remaining || resolve(values);
	        }, reject);
	      });
	      --remaining || resolve(values);
	    });
	    if (result.e) reject(result.v);
	    return capability.promise;
	  },
	  // 25.4.4.4 Promise.race(iterable)
	  race: function race(iterable) {
	    var C = this;
	    var capability = newPromiseCapability(C);
	    var reject = capability.reject;
	    var result = perform(function () {
	      forOf(iterable, false, function (promise) {
	        C.resolve(promise).then(capability.resolve, reject);
	      });
	    });
	    if (result.e) reject(result.v);
	    return capability.promise;
	  }
	});


/***/ }),
/* 212 */
/***/ (function(module, exports) {

	module.exports = function (it, Constructor, name, forbiddenField) {
	  if (!(it instanceof Constructor) || (forbiddenField !== undefined && forbiddenField in it)) {
	    throw TypeError(name + ': incorrect invocation!');
	  } return it;
	};


/***/ }),
/* 213 */
/***/ (function(module, exports, __webpack_require__) {

	var ctx = __webpack_require__(23);
	var call = __webpack_require__(163);
	var isArrayIter = __webpack_require__(164);
	var anObject = __webpack_require__(12);
	var toLength = __webpack_require__(38);
	var getIterFn = __webpack_require__(166);
	var BREAK = {};
	var RETURN = {};
	var exports = module.exports = function (iterable, entries, fn, that, ITERATOR) {
	  var iterFn = ITERATOR ? function () { return iterable; } : getIterFn(iterable);
	  var f = ctx(fn, that, entries ? 2 : 1);
	  var index = 0;
	  var length, step, iterator, result;
	  if (typeof iterFn != 'function') throw TypeError(iterable + ' is not iterable!');
	  // fast case for arrays with default iterator
	  if (isArrayIter(iterFn)) for (length = toLength(iterable.length); length > index; index++) {
	    result = entries ? f(anObject(step = iterable[index])[0], step[1]) : f(iterable[index]);
	    if (result === BREAK || result === RETURN) return result;
	  } else for (iterator = iterFn.call(iterable); !(step = iterator.next()).done;) {
	    result = call(iterator, f, step.value, entries);
	    if (result === BREAK || result === RETURN) return result;
	  }
	};
	exports.BREAK = BREAK;
	exports.RETURN = RETURN;


/***/ }),
/* 214 */
/***/ (function(module, exports, __webpack_require__) {

	var ctx = __webpack_require__(23);
	var invoke = __webpack_require__(78);
	var html = __webpack_require__(49);
	var cel = __webpack_require__(15);
	var global = __webpack_require__(4);
	var process = global.process;
	var setTask = global.setImmediate;
	var clearTask = global.clearImmediate;
	var MessageChannel = global.MessageChannel;
	var Dispatch = global.Dispatch;
	var counter = 0;
	var queue = {};
	var ONREADYSTATECHANGE = 'onreadystatechange';
	var defer, channel, port;
	var run = function () {
	  var id = +this;
	  // eslint-disable-next-line no-prototype-builtins
	  if (queue.hasOwnProperty(id)) {
	    var fn = queue[id];
	    delete queue[id];
	    fn();
	  }
	};
	var listener = function (event) {
	  run.call(event.data);
	};
	// Node.js 0.9+ & IE10+ has setImmediate, otherwise:
	if (!setTask || !clearTask) {
	  setTask = function setImmediate(fn) {
	    var args = [];
	    var i = 1;
	    while (arguments.length > i) args.push(arguments[i++]);
	    queue[++counter] = function () {
	      // eslint-disable-next-line no-new-func
	      invoke(typeof fn == 'function' ? fn : Function(fn), args);
	    };
	    defer(counter);
	    return counter;
	  };
	  clearTask = function clearImmediate(id) {
	    delete queue[id];
	  };
	  // Node.js 0.8-
	  if (__webpack_require__(35)(process) == 'process') {
	    defer = function (id) {
	      process.nextTick(ctx(run, id, 1));
	    };
	  // Sphere (JS game engine) Dispatch API
	  } else if (Dispatch && Dispatch.now) {
	    defer = function (id) {
	      Dispatch.now(ctx(run, id, 1));
	    };
	  // Browsers with MessageChannel, includes WebWorkers
	  } else if (MessageChannel) {
	    channel = new MessageChannel();
	    port = channel.port2;
	    channel.port1.onmessage = listener;
	    defer = ctx(port.postMessage, port, 1);
	  // Browsers with postMessage, skip WebWorkers
	  // IE8 has postMessage, but it's sync & typeof its postMessage is 'object'
	  } else if (global.addEventListener && typeof postMessage == 'function' && !global.importScripts) {
	    defer = function (id) {
	      global.postMessage(id + '', '*');
	    };
	    global.addEventListener('message', listener, false);
	  // IE8-
	  } else if (ONREADYSTATECHANGE in cel('script')) {
	    defer = function (id) {
	      html.appendChild(cel('script'))[ONREADYSTATECHANGE] = function () {
	        html.removeChild(this);
	        run.call(id);
	      };
	    };
	  // Rest old browsers
	  } else {
	    defer = function (id) {
	      setTimeout(ctx(run, id, 1), 0);
	    };
	  }
	}
	module.exports = {
	  set: setTask,
	  clear: clearTask
	};


/***/ }),
/* 215 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var macrotask = __webpack_require__(214).set;
	var Observer = global.MutationObserver || global.WebKitMutationObserver;
	var process = global.process;
	var Promise = global.Promise;
	var isNode = __webpack_require__(35)(process) == 'process';

	module.exports = function () {
	  var head, last, notify;

	  var flush = function () {
	    var parent, fn;
	    if (isNode && (parent = process.domain)) parent.exit();
	    while (head) {
	      fn = head.fn;
	      head = head.next;
	      try {
	        fn();
	      } catch (e) {
	        if (head) notify();
	        else last = undefined;
	        throw e;
	      }
	    } last = undefined;
	    if (parent) parent.enter();
	  };

	  // Node.js
	  if (isNode) {
	    notify = function () {
	      process.nextTick(flush);
	    };
	  // browsers with MutationObserver, except iOS Safari - https://github.com/zloirock/core-js/issues/339
	  } else if (Observer && !(global.navigator && global.navigator.standalone)) {
	    var toggle = true;
	    var node = document.createTextNode('');
	    new Observer(flush).observe(node, { characterData: true }); // eslint-disable-line no-new
	    notify = function () {
	      node.data = toggle = !toggle;
	    };
	  // environments with maybe non-completely correct, but existent Promise
	  } else if (Promise && Promise.resolve) {
	    // Promise.resolve without an argument throws an error in LG WebOS 2
	    var promise = Promise.resolve(undefined);
	    notify = function () {
	      promise.then(flush);
	    };
	  // for other environments - macrotask based on:
	  // - setImmediate
	  // - MessageChannel
	  // - window.postMessag
	  // - onreadystatechange
	  // - setTimeout
	  } else {
	    notify = function () {
	      // strange IE + webpack dev server bug - use .call(global)
	      macrotask.call(global, flush);
	    };
	  }

	  return function (fn) {
	    var task = { fn: fn, next: undefined };
	    if (last) last.next = task;
	    if (!head) {
	      head = task;
	      notify();
	    } last = task;
	  };
	};


/***/ }),
/* 216 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 25.4.1.5 NewPromiseCapability(C)
	var aFunction = __webpack_require__(24);

	function PromiseCapability(C) {
	  var resolve, reject;
	  this.promise = new C(function ($$resolve, $$reject) {
	    if (resolve !== undefined || reject !== undefined) throw TypeError('Bad Promise constructor');
	    resolve = $$resolve;
	    reject = $$reject;
	  });
	  this.resolve = aFunction(resolve);
	  this.reject = aFunction(reject);
	}

	module.exports.f = function (C) {
	  return new PromiseCapability(C);
	};


/***/ }),
/* 217 */
/***/ (function(module, exports) {

	module.exports = function (exec) {
	  try {
	    return { e: false, v: exec() };
	  } catch (e) {
	    return { e: true, v: e };
	  }
	};


/***/ }),
/* 218 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var navigator = global.navigator;

	module.exports = navigator && navigator.userAgent || '';


/***/ }),
/* 219 */
/***/ (function(module, exports, __webpack_require__) {

	var anObject = __webpack_require__(12);
	var isObject = __webpack_require__(13);
	var newPromiseCapability = __webpack_require__(216);

	module.exports = function (C, x) {
	  anObject(C);
	  if (isObject(x) && x.constructor === C) return x;
	  var promiseCapability = newPromiseCapability.f(C);
	  var resolve = promiseCapability.resolve;
	  resolve(x);
	  return promiseCapability.promise;
	};


/***/ }),
/* 220 */
/***/ (function(module, exports, __webpack_require__) {

	var redefine = __webpack_require__(18);
	module.exports = function (target, src, safe) {
	  for (var key in src) redefine(target, key, src[key], safe);
	  return target;
	};


/***/ }),
/* 221 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var strong = __webpack_require__(222);
	var validate = __webpack_require__(223);
	var MAP = 'Map';

	// 23.1 Map Objects
	module.exports = __webpack_require__(224)(MAP, function (get) {
	  return function Map() { return get(this, arguments.length > 0 ? arguments[0] : undefined); };
	}, {
	  // 23.1.3.6 Map.prototype.get(key)
	  get: function get(key) {
	    var entry = strong.getEntry(validate(this, MAP), key);
	    return entry && entry.v;
	  },
	  // 23.1.3.9 Map.prototype.set(key, value)
	  set: function set(key, value) {
	    return strong.def(validate(this, MAP), key === 0 ? 0 : key, value);
	  }
	}, strong, true);


/***/ }),
/* 222 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var dP = __webpack_require__(11).f;
	var create = __webpack_require__(47);
	var redefineAll = __webpack_require__(220);
	var ctx = __webpack_require__(23);
	var anInstance = __webpack_require__(212);
	var forOf = __webpack_require__(213);
	var $iterDefine = __webpack_require__(129);
	var step = __webpack_require__(196);
	var setSpecies = __webpack_require__(194);
	var DESCRIPTORS = __webpack_require__(6);
	var fastKey = __webpack_require__(25).fastKey;
	var validate = __webpack_require__(223);
	var SIZE = DESCRIPTORS ? '_s' : 'size';

	var getEntry = function (that, key) {
	  // fast case
	  var index = fastKey(key);
	  var entry;
	  if (index !== 'F') return that._i[index];
	  // frozen object case
	  for (entry = that._f; entry; entry = entry.n) {
	    if (entry.k == key) return entry;
	  }
	};

	module.exports = {
	  getConstructor: function (wrapper, NAME, IS_MAP, ADDER) {
	    var C = wrapper(function (that, iterable) {
	      anInstance(that, C, NAME, '_i');
	      that._t = NAME;         // collection type
	      that._i = create(null); // index
	      that._f = undefined;    // first entry
	      that._l = undefined;    // last entry
	      that[SIZE] = 0;         // size
	      if (iterable != undefined) forOf(iterable, IS_MAP, that[ADDER], that);
	    });
	    redefineAll(C.prototype, {
	      // 23.1.3.1 Map.prototype.clear()
	      // 23.2.3.2 Set.prototype.clear()
	      clear: function clear() {
	        for (var that = validate(this, NAME), data = that._i, entry = that._f; entry; entry = entry.n) {
	          entry.r = true;
	          if (entry.p) entry.p = entry.p.n = undefined;
	          delete data[entry.i];
	        }
	        that._f = that._l = undefined;
	        that[SIZE] = 0;
	      },
	      // 23.1.3.3 Map.prototype.delete(key)
	      // 23.2.3.4 Set.prototype.delete(value)
	      'delete': function (key) {
	        var that = validate(this, NAME);
	        var entry = getEntry(that, key);
	        if (entry) {
	          var next = entry.n;
	          var prev = entry.p;
	          delete that._i[entry.i];
	          entry.r = true;
	          if (prev) prev.n = next;
	          if (next) next.p = prev;
	          if (that._f == entry) that._f = next;
	          if (that._l == entry) that._l = prev;
	          that[SIZE]--;
	        } return !!entry;
	      },
	      // 23.2.3.6 Set.prototype.forEach(callbackfn, thisArg = undefined)
	      // 23.1.3.5 Map.prototype.forEach(callbackfn, thisArg = undefined)
	      forEach: function forEach(callbackfn /* , that = undefined */) {
	        validate(this, NAME);
	        var f = ctx(callbackfn, arguments.length > 1 ? arguments[1] : undefined, 3);
	        var entry;
	        while (entry = entry ? entry.n : this._f) {
	          f(entry.v, entry.k, this);
	          // revert to the last existing entry
	          while (entry && entry.r) entry = entry.p;
	        }
	      },
	      // 23.1.3.7 Map.prototype.has(key)
	      // 23.2.3.7 Set.prototype.has(value)
	      has: function has(key) {
	        return !!getEntry(validate(this, NAME), key);
	      }
	    });
	    if (DESCRIPTORS) dP(C.prototype, 'size', {
	      get: function () {
	        return validate(this, NAME)[SIZE];
	      }
	    });
	    return C;
	  },
	  def: function (that, key, value) {
	    var entry = getEntry(that, key);
	    var prev, index;
	    // change existing entry
	    if (entry) {
	      entry.v = value;
	    // create new entry
	    } else {
	      that._l = entry = {
	        i: index = fastKey(key, true), // <- index
	        k: key,                        // <- key
	        v: value,                      // <- value
	        p: prev = that._l,             // <- previous entry
	        n: undefined,                  // <- next entry
	        r: false                       // <- removed
	      };
	      if (!that._f) that._f = entry;
	      if (prev) prev.n = entry;
	      that[SIZE]++;
	      // add to index
	      if (index !== 'F') that._i[index] = entry;
	    } return that;
	  },
	  getEntry: getEntry,
	  setStrong: function (C, NAME, IS_MAP) {
	    // add .keys, .values, .entries, [@@iterator]
	    // 23.1.3.4, 23.1.3.8, 23.1.3.11, 23.1.3.12, 23.2.3.5, 23.2.3.8, 23.2.3.10, 23.2.3.11
	    $iterDefine(C, NAME, function (iterated, kind) {
	      this._t = validate(iterated, NAME); // target
	      this._k = kind;                     // kind
	      this._l = undefined;                // previous
	    }, function () {
	      var that = this;
	      var kind = that._k;
	      var entry = that._l;
	      // revert to the last existing entry
	      while (entry && entry.r) entry = entry.p;
	      // get next entry
	      if (!that._t || !(that._l = entry = entry ? entry.n : that._t._f)) {
	        // or finish the iteration
	        that._t = undefined;
	        return step(1);
	      }
	      // return step by kind
	      if (kind == 'keys') return step(0, entry.k);
	      if (kind == 'values') return step(0, entry.v);
	      return step(0, [entry.k, entry.v]);
	    }, IS_MAP ? 'entries' : 'values', !IS_MAP, true);

	    // add [@@species], 23.1.2.2, 23.2.2.2
	    setSpecies(NAME);
	  }
	};


/***/ }),
/* 223 */
/***/ (function(module, exports, __webpack_require__) {

	var isObject = __webpack_require__(13);
	module.exports = function (it, TYPE) {
	  if (!isObject(it) || it._t !== TYPE) throw TypeError('Incompatible receiver, ' + TYPE + ' required!');
	  return it;
	};


/***/ }),
/* 224 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var global = __webpack_require__(4);
	var $export = __webpack_require__(8);
	var redefine = __webpack_require__(18);
	var redefineAll = __webpack_require__(220);
	var meta = __webpack_require__(25);
	var forOf = __webpack_require__(213);
	var anInstance = __webpack_require__(212);
	var isObject = __webpack_require__(13);
	var fails = __webpack_require__(7);
	var $iterDetect = __webpack_require__(167);
	var setToStringTag = __webpack_require__(26);
	var inheritIfRequired = __webpack_require__(88);

	module.exports = function (NAME, wrapper, methods, common, IS_MAP, IS_WEAK) {
	  var Base = global[NAME];
	  var C = Base;
	  var ADDER = IS_MAP ? 'set' : 'add';
	  var proto = C && C.prototype;
	  var O = {};
	  var fixMethod = function (KEY) {
	    var fn = proto[KEY];
	    redefine(proto, KEY,
	      KEY == 'delete' ? function (a) {
	        return IS_WEAK && !isObject(a) ? false : fn.call(this, a === 0 ? 0 : a);
	      } : KEY == 'has' ? function has(a) {
	        return IS_WEAK && !isObject(a) ? false : fn.call(this, a === 0 ? 0 : a);
	      } : KEY == 'get' ? function get(a) {
	        return IS_WEAK && !isObject(a) ? undefined : fn.call(this, a === 0 ? 0 : a);
	      } : KEY == 'add' ? function add(a) { fn.call(this, a === 0 ? 0 : a); return this; }
	        : function set(a, b) { fn.call(this, a === 0 ? 0 : a, b); return this; }
	    );
	  };
	  if (typeof C != 'function' || !(IS_WEAK || proto.forEach && !fails(function () {
	    new C().entries().next();
	  }))) {
	    // create collection constructor
	    C = common.getConstructor(wrapper, NAME, IS_MAP, ADDER);
	    redefineAll(C.prototype, methods);
	    meta.NEED = true;
	  } else {
	    var instance = new C();
	    // early implementations not supports chaining
	    var HASNT_CHAINING = instance[ADDER](IS_WEAK ? {} : -0, 1) != instance;
	    // V8 ~  Chromium 40- weak-collections throws on primitives, but should return false
	    var THROWS_ON_PRIMITIVES = fails(function () { instance.has(1); });
	    // most early implementations doesn't supports iterables, most modern - not close it correctly
	    var ACCEPT_ITERABLES = $iterDetect(function (iter) { new C(iter); }); // eslint-disable-line no-new
	    // for early implementations -0 and +0 not the same
	    var BUGGY_ZERO = !IS_WEAK && fails(function () {
	      // V8 ~ Chromium 42- fails only with 5+ elements
	      var $instance = new C();
	      var index = 5;
	      while (index--) $instance[ADDER](index, index);
	      return !$instance.has(-0);
	    });
	    if (!ACCEPT_ITERABLES) {
	      C = wrapper(function (target, iterable) {
	        anInstance(target, C, NAME);
	        var that = inheritIfRequired(new Base(), target, C);
	        if (iterable != undefined) forOf(iterable, IS_MAP, that[ADDER], that);
	        return that;
	      });
	      C.prototype = proto;
	      proto.constructor = C;
	    }
	    if (THROWS_ON_PRIMITIVES || BUGGY_ZERO) {
	      fixMethod('delete');
	      fixMethod('has');
	      IS_MAP && fixMethod('get');
	    }
	    if (BUGGY_ZERO || HASNT_CHAINING) fixMethod(ADDER);
	    // weak collections should not contains .clear method
	    if (IS_WEAK && proto.clear) delete proto.clear;
	  }

	  setToStringTag(C, NAME);

	  O[NAME] = C;
	  $export($export.G + $export.W + $export.F * (C != Base), O);

	  if (!IS_WEAK) common.setStrong(C, NAME, IS_MAP);

	  return C;
	};


/***/ }),
/* 225 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var strong = __webpack_require__(222);
	var validate = __webpack_require__(223);
	var SET = 'Set';

	// 23.2 Set Objects
	module.exports = __webpack_require__(224)(SET, function (get) {
	  return function Set() { return get(this, arguments.length > 0 ? arguments[0] : undefined); };
	}, {
	  // 23.2.3.1 Set.prototype.add(value)
	  add: function add(value) {
	    return strong.def(validate(this, SET), value = value === 0 ? 0 : value, value);
	  }
	}, strong);


/***/ }),
/* 226 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var global = __webpack_require__(4);
	var each = __webpack_require__(174)(0);
	var redefine = __webpack_require__(18);
	var meta = __webpack_require__(25);
	var assign = __webpack_require__(69);
	var weak = __webpack_require__(227);
	var isObject = __webpack_require__(13);
	var validate = __webpack_require__(223);
	var NATIVE_WEAK_MAP = __webpack_require__(223);
	var IS_IE11 = !global.ActiveXObject && 'ActiveXObject' in global;
	var WEAK_MAP = 'WeakMap';
	var getWeak = meta.getWeak;
	var isExtensible = Object.isExtensible;
	var uncaughtFrozenStore = weak.ufstore;
	var InternalMap;

	var wrapper = function (get) {
	  return function WeakMap() {
	    return get(this, arguments.length > 0 ? arguments[0] : undefined);
	  };
	};

	var methods = {
	  // 23.3.3.3 WeakMap.prototype.get(key)
	  get: function get(key) {
	    if (isObject(key)) {
	      var data = getWeak(key);
	      if (data === true) return uncaughtFrozenStore(validate(this, WEAK_MAP)).get(key);
	      return data ? data[this._i] : undefined;
	    }
	  },
	  // 23.3.3.5 WeakMap.prototype.set(key, value)
	  set: function set(key, value) {
	    return weak.def(validate(this, WEAK_MAP), key, value);
	  }
	};

	// 23.3 WeakMap Objects
	var $WeakMap = module.exports = __webpack_require__(224)(WEAK_MAP, wrapper, methods, weak, true, true);

	// IE11 WeakMap frozen keys fix
	if (NATIVE_WEAK_MAP && IS_IE11) {
	  InternalMap = weak.getConstructor(wrapper, WEAK_MAP);
	  assign(InternalMap.prototype, methods);
	  meta.NEED = true;
	  each(['delete', 'has', 'get', 'set'], function (key) {
	    var proto = $WeakMap.prototype;
	    var method = proto[key];
	    redefine(proto, key, function (a, b) {
	      // store frozen objects on internal weakmap shim
	      if (isObject(a) && !isExtensible(a)) {
	        if (!this._f) this._f = new InternalMap();
	        var result = this._f[key](a, b);
	        return key == 'set' ? this : result;
	      // store all the rest on native weakmap
	      } return method.call(this, a, b);
	    });
	  });
	}


/***/ }),
/* 227 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var redefineAll = __webpack_require__(220);
	var getWeak = __webpack_require__(25).getWeak;
	var anObject = __webpack_require__(12);
	var isObject = __webpack_require__(13);
	var anInstance = __webpack_require__(212);
	var forOf = __webpack_require__(213);
	var createArrayMethod = __webpack_require__(174);
	var $has = __webpack_require__(5);
	var validate = __webpack_require__(223);
	var arrayFind = createArrayMethod(5);
	var arrayFindIndex = createArrayMethod(6);
	var id = 0;

	// fallback for uncaught frozen keys
	var uncaughtFrozenStore = function (that) {
	  return that._l || (that._l = new UncaughtFrozenStore());
	};
	var UncaughtFrozenStore = function () {
	  this.a = [];
	};
	var findUncaughtFrozen = function (store, key) {
	  return arrayFind(store.a, function (it) {
	    return it[0] === key;
	  });
	};
	UncaughtFrozenStore.prototype = {
	  get: function (key) {
	    var entry = findUncaughtFrozen(this, key);
	    if (entry) return entry[1];
	  },
	  has: function (key) {
	    return !!findUncaughtFrozen(this, key);
	  },
	  set: function (key, value) {
	    var entry = findUncaughtFrozen(this, key);
	    if (entry) entry[1] = value;
	    else this.a.push([key, value]);
	  },
	  'delete': function (key) {
	    var index = arrayFindIndex(this.a, function (it) {
	      return it[0] === key;
	    });
	    if (~index) this.a.splice(index, 1);
	    return !!~index;
	  }
	};

	module.exports = {
	  getConstructor: function (wrapper, NAME, IS_MAP, ADDER) {
	    var C = wrapper(function (that, iterable) {
	      anInstance(that, C, NAME, '_i');
	      that._t = NAME;      // collection type
	      that._i = id++;      // collection id
	      that._l = undefined; // leak store for uncaught frozen objects
	      if (iterable != undefined) forOf(iterable, IS_MAP, that[ADDER], that);
	    });
	    redefineAll(C.prototype, {
	      // 23.3.3.2 WeakMap.prototype.delete(key)
	      // 23.4.3.3 WeakSet.prototype.delete(value)
	      'delete': function (key) {
	        if (!isObject(key)) return false;
	        var data = getWeak(key);
	        if (data === true) return uncaughtFrozenStore(validate(this, NAME))['delete'](key);
	        return data && $has(data, this._i) && delete data[this._i];
	      },
	      // 23.3.3.4 WeakMap.prototype.has(key)
	      // 23.4.3.4 WeakSet.prototype.has(value)
	      has: function has(key) {
	        if (!isObject(key)) return false;
	        var data = getWeak(key);
	        if (data === true) return uncaughtFrozenStore(validate(this, NAME)).has(key);
	        return data && $has(data, this._i);
	      }
	    });
	    return C;
	  },
	  def: function (that, key, value) {
	    var data = getWeak(anObject(key), true);
	    if (data === true) uncaughtFrozenStore(that).set(key, value);
	    else data[that._i] = value;
	    return that;
	  },
	  ufstore: uncaughtFrozenStore
	};


/***/ }),
/* 228 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var weak = __webpack_require__(227);
	var validate = __webpack_require__(223);
	var WEAK_SET = 'WeakSet';

	// 23.4 WeakSet Objects
	__webpack_require__(224)(WEAK_SET, function (get) {
	  return function WeakSet() { return get(this, arguments.length > 0 ? arguments[0] : undefined); };
	}, {
	  // 23.4.3.1 WeakSet.prototype.add(value)
	  add: function add(value) {
	    return weak.def(validate(this, WEAK_SET), value, true);
	  }
	}, weak, false, true);


/***/ }),
/* 229 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var $typed = __webpack_require__(230);
	var buffer = __webpack_require__(231);
	var anObject = __webpack_require__(12);
	var toAbsoluteIndex = __webpack_require__(40);
	var toLength = __webpack_require__(38);
	var isObject = __webpack_require__(13);
	var ArrayBuffer = __webpack_require__(4).ArrayBuffer;
	var speciesConstructor = __webpack_require__(210);
	var $ArrayBuffer = buffer.ArrayBuffer;
	var $DataView = buffer.DataView;
	var $isView = $typed.ABV && ArrayBuffer.isView;
	var $slice = $ArrayBuffer.prototype.slice;
	var VIEW = $typed.VIEW;
	var ARRAY_BUFFER = 'ArrayBuffer';

	$export($export.G + $export.W + $export.F * (ArrayBuffer !== $ArrayBuffer), { ArrayBuffer: $ArrayBuffer });

	$export($export.S + $export.F * !$typed.CONSTR, ARRAY_BUFFER, {
	  // 24.1.3.1 ArrayBuffer.isView(arg)
	  isView: function isView(it) {
	    return $isView && $isView(it) || isObject(it) && VIEW in it;
	  }
	});

	$export($export.P + $export.U + $export.F * __webpack_require__(7)(function () {
	  return !new $ArrayBuffer(2).slice(1, undefined).byteLength;
	}), ARRAY_BUFFER, {
	  // 24.1.4.3 ArrayBuffer.prototype.slice(start, end)
	  slice: function slice(start, end) {
	    if ($slice !== undefined && end === undefined) return $slice.call(anObject(this), start); // FF fix
	    var len = anObject(this).byteLength;
	    var first = toAbsoluteIndex(start, len);
	    var fin = toAbsoluteIndex(end === undefined ? len : end, len);
	    var result = new (speciesConstructor(this, $ArrayBuffer))(toLength(fin - first));
	    var viewS = new $DataView(this);
	    var viewT = new $DataView(result);
	    var index = 0;
	    while (first < fin) {
	      viewT.setUint8(index++, viewS.getUint8(first++));
	    } return result;
	  }
	});

	__webpack_require__(194)(ARRAY_BUFFER);


/***/ }),
/* 230 */
/***/ (function(module, exports, __webpack_require__) {

	var global = __webpack_require__(4);
	var hide = __webpack_require__(10);
	var uid = __webpack_require__(19);
	var TYPED = uid('typed_array');
	var VIEW = uid('view');
	var ABV = !!(global.ArrayBuffer && global.DataView);
	var CONSTR = ABV;
	var i = 0;
	var l = 9;
	var Typed;

	var TypedArrayConstructors = (
	  'Int8Array,Uint8Array,Uint8ClampedArray,Int16Array,Uint16Array,Int32Array,Uint32Array,Float32Array,Float64Array'
	).split(',');

	while (i < l) {
	  if (Typed = global[TypedArrayConstructors[i++]]) {
	    hide(Typed.prototype, TYPED, true);
	    hide(Typed.prototype, VIEW, true);
	  } else CONSTR = false;
	}

	module.exports = {
	  ABV: ABV,
	  CONSTR: CONSTR,
	  TYPED: TYPED,
	  VIEW: VIEW
	};


/***/ }),
/* 231 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var global = __webpack_require__(4);
	var DESCRIPTORS = __webpack_require__(6);
	var LIBRARY = __webpack_require__(22);
	var $typed = __webpack_require__(230);
	var hide = __webpack_require__(10);
	var redefineAll = __webpack_require__(220);
	var fails = __webpack_require__(7);
	var anInstance = __webpack_require__(212);
	var toInteger = __webpack_require__(39);
	var toLength = __webpack_require__(38);
	var toIndex = __webpack_require__(232);
	var gOPN = __webpack_require__(51).f;
	var dP = __webpack_require__(11).f;
	var arrayFill = __webpack_require__(190);
	var setToStringTag = __webpack_require__(26);
	var ARRAY_BUFFER = 'ArrayBuffer';
	var DATA_VIEW = 'DataView';
	var PROTOTYPE = 'prototype';
	var WRONG_LENGTH = 'Wrong length!';
	var WRONG_INDEX = 'Wrong index!';
	var $ArrayBuffer = global[ARRAY_BUFFER];
	var $DataView = global[DATA_VIEW];
	var Math = global.Math;
	var RangeError = global.RangeError;
	// eslint-disable-next-line no-shadow-restricted-names
	var Infinity = global.Infinity;
	var BaseBuffer = $ArrayBuffer;
	var abs = Math.abs;
	var pow = Math.pow;
	var floor = Math.floor;
	var log = Math.log;
	var LN2 = Math.LN2;
	var BUFFER = 'buffer';
	var BYTE_LENGTH = 'byteLength';
	var BYTE_OFFSET = 'byteOffset';
	var $BUFFER = DESCRIPTORS ? '_b' : BUFFER;
	var $LENGTH = DESCRIPTORS ? '_l' : BYTE_LENGTH;
	var $OFFSET = DESCRIPTORS ? '_o' : BYTE_OFFSET;

	// IEEE754 conversions based on https://github.com/feross/ieee754
	function packIEEE754(value, mLen, nBytes) {
	  var buffer = new Array(nBytes);
	  var eLen = nBytes * 8 - mLen - 1;
	  var eMax = (1 << eLen) - 1;
	  var eBias = eMax >> 1;
	  var rt = mLen === 23 ? pow(2, -24) - pow(2, -77) : 0;
	  var i = 0;
	  var s = value < 0 || value === 0 && 1 / value < 0 ? 1 : 0;
	  var e, m, c;
	  value = abs(value);
	  // eslint-disable-next-line no-self-compare
	  if (value != value || value === Infinity) {
	    // eslint-disable-next-line no-self-compare
	    m = value != value ? 1 : 0;
	    e = eMax;
	  } else {
	    e = floor(log(value) / LN2);
	    if (value * (c = pow(2, -e)) < 1) {
	      e--;
	      c *= 2;
	    }
	    if (e + eBias >= 1) {
	      value += rt / c;
	    } else {
	      value += rt * pow(2, 1 - eBias);
	    }
	    if (value * c >= 2) {
	      e++;
	      c /= 2;
	    }
	    if (e + eBias >= eMax) {
	      m = 0;
	      e = eMax;
	    } else if (e + eBias >= 1) {
	      m = (value * c - 1) * pow(2, mLen);
	      e = e + eBias;
	    } else {
	      m = value * pow(2, eBias - 1) * pow(2, mLen);
	      e = 0;
	    }
	  }
	  for (; mLen >= 8; buffer[i++] = m & 255, m /= 256, mLen -= 8);
	  e = e << mLen | m;
	  eLen += mLen;
	  for (; eLen > 0; buffer[i++] = e & 255, e /= 256, eLen -= 8);
	  buffer[--i] |= s * 128;
	  return buffer;
	}
	function unpackIEEE754(buffer, mLen, nBytes) {
	  var eLen = nBytes * 8 - mLen - 1;
	  var eMax = (1 << eLen) - 1;
	  var eBias = eMax >> 1;
	  var nBits = eLen - 7;
	  var i = nBytes - 1;
	  var s = buffer[i--];
	  var e = s & 127;
	  var m;
	  s >>= 7;
	  for (; nBits > 0; e = e * 256 + buffer[i], i--, nBits -= 8);
	  m = e & (1 << -nBits) - 1;
	  e >>= -nBits;
	  nBits += mLen;
	  for (; nBits > 0; m = m * 256 + buffer[i], i--, nBits -= 8);
	  if (e === 0) {
	    e = 1 - eBias;
	  } else if (e === eMax) {
	    return m ? NaN : s ? -Infinity : Infinity;
	  } else {
	    m = m + pow(2, mLen);
	    e = e - eBias;
	  } return (s ? -1 : 1) * m * pow(2, e - mLen);
	}

	function unpackI32(bytes) {
	  return bytes[3] << 24 | bytes[2] << 16 | bytes[1] << 8 | bytes[0];
	}
	function packI8(it) {
	  return [it & 0xff];
	}
	function packI16(it) {
	  return [it & 0xff, it >> 8 & 0xff];
	}
	function packI32(it) {
	  return [it & 0xff, it >> 8 & 0xff, it >> 16 & 0xff, it >> 24 & 0xff];
	}
	function packF64(it) {
	  return packIEEE754(it, 52, 8);
	}
	function packF32(it) {
	  return packIEEE754(it, 23, 4);
	}

	function addGetter(C, key, internal) {
	  dP(C[PROTOTYPE], key, { get: function () { return this[internal]; } });
	}

	function get(view, bytes, index, isLittleEndian) {
	  var numIndex = +index;
	  var intIndex = toIndex(numIndex);
	  if (intIndex + bytes > view[$LENGTH]) throw RangeError(WRONG_INDEX);
	  var store = view[$BUFFER]._b;
	  var start = intIndex + view[$OFFSET];
	  var pack = store.slice(start, start + bytes);
	  return isLittleEndian ? pack : pack.reverse();
	}
	function set(view, bytes, index, conversion, value, isLittleEndian) {
	  var numIndex = +index;
	  var intIndex = toIndex(numIndex);
	  if (intIndex + bytes > view[$LENGTH]) throw RangeError(WRONG_INDEX);
	  var store = view[$BUFFER]._b;
	  var start = intIndex + view[$OFFSET];
	  var pack = conversion(+value);
	  for (var i = 0; i < bytes; i++) store[start + i] = pack[isLittleEndian ? i : bytes - i - 1];
	}

	if (!$typed.ABV) {
	  $ArrayBuffer = function ArrayBuffer(length) {
	    anInstance(this, $ArrayBuffer, ARRAY_BUFFER);
	    var byteLength = toIndex(length);
	    this._b = arrayFill.call(new Array(byteLength), 0);
	    this[$LENGTH] = byteLength;
	  };

	  $DataView = function DataView(buffer, byteOffset, byteLength) {
	    anInstance(this, $DataView, DATA_VIEW);
	    anInstance(buffer, $ArrayBuffer, DATA_VIEW);
	    var bufferLength = buffer[$LENGTH];
	    var offset = toInteger(byteOffset);
	    if (offset < 0 || offset > bufferLength) throw RangeError('Wrong offset!');
	    byteLength = byteLength === undefined ? bufferLength - offset : toLength(byteLength);
	    if (offset + byteLength > bufferLength) throw RangeError(WRONG_LENGTH);
	    this[$BUFFER] = buffer;
	    this[$OFFSET] = offset;
	    this[$LENGTH] = byteLength;
	  };

	  if (DESCRIPTORS) {
	    addGetter($ArrayBuffer, BYTE_LENGTH, '_l');
	    addGetter($DataView, BUFFER, '_b');
	    addGetter($DataView, BYTE_LENGTH, '_l');
	    addGetter($DataView, BYTE_OFFSET, '_o');
	  }

	  redefineAll($DataView[PROTOTYPE], {
	    getInt8: function getInt8(byteOffset) {
	      return get(this, 1, byteOffset)[0] << 24 >> 24;
	    },
	    getUint8: function getUint8(byteOffset) {
	      return get(this, 1, byteOffset)[0];
	    },
	    getInt16: function getInt16(byteOffset /* , littleEndian */) {
	      var bytes = get(this, 2, byteOffset, arguments[1]);
	      return (bytes[1] << 8 | bytes[0]) << 16 >> 16;
	    },
	    getUint16: function getUint16(byteOffset /* , littleEndian */) {
	      var bytes = get(this, 2, byteOffset, arguments[1]);
	      return bytes[1] << 8 | bytes[0];
	    },
	    getInt32: function getInt32(byteOffset /* , littleEndian */) {
	      return unpackI32(get(this, 4, byteOffset, arguments[1]));
	    },
	    getUint32: function getUint32(byteOffset /* , littleEndian */) {
	      return unpackI32(get(this, 4, byteOffset, arguments[1])) >>> 0;
	    },
	    getFloat32: function getFloat32(byteOffset /* , littleEndian */) {
	      return unpackIEEE754(get(this, 4, byteOffset, arguments[1]), 23, 4);
	    },
	    getFloat64: function getFloat64(byteOffset /* , littleEndian */) {
	      return unpackIEEE754(get(this, 8, byteOffset, arguments[1]), 52, 8);
	    },
	    setInt8: function setInt8(byteOffset, value) {
	      set(this, 1, byteOffset, packI8, value);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      set(this, 1, byteOffset, packI8, value);
	    },
	    setInt16: function setInt16(byteOffset, value /* , littleEndian */) {
	      set(this, 2, byteOffset, packI16, value, arguments[2]);
	    },
	    setUint16: function setUint16(byteOffset, value /* , littleEndian */) {
	      set(this, 2, byteOffset, packI16, value, arguments[2]);
	    },
	    setInt32: function setInt32(byteOffset, value /* , littleEndian */) {
	      set(this, 4, byteOffset, packI32, value, arguments[2]);
	    },
	    setUint32: function setUint32(byteOffset, value /* , littleEndian */) {
	      set(this, 4, byteOffset, packI32, value, arguments[2]);
	    },
	    setFloat32: function setFloat32(byteOffset, value /* , littleEndian */) {
	      set(this, 4, byteOffset, packF32, value, arguments[2]);
	    },
	    setFloat64: function setFloat64(byteOffset, value /* , littleEndian */) {
	      set(this, 8, byteOffset, packF64, value, arguments[2]);
	    }
	  });
	} else {
	  if (!fails(function () {
	    $ArrayBuffer(1);
	  }) || !fails(function () {
	    new $ArrayBuffer(-1); // eslint-disable-line no-new
	  }) || fails(function () {
	    new $ArrayBuffer(); // eslint-disable-line no-new
	    new $ArrayBuffer(1.5); // eslint-disable-line no-new
	    new $ArrayBuffer(NaN); // eslint-disable-line no-new
	    return $ArrayBuffer.name != ARRAY_BUFFER;
	  })) {
	    $ArrayBuffer = function ArrayBuffer(length) {
	      anInstance(this, $ArrayBuffer);
	      return new BaseBuffer(toIndex(length));
	    };
	    var ArrayBufferProto = $ArrayBuffer[PROTOTYPE] = BaseBuffer[PROTOTYPE];
	    for (var keys = gOPN(BaseBuffer), j = 0, key; keys.length > j;) {
	      if (!((key = keys[j++]) in $ArrayBuffer)) hide($ArrayBuffer, key, BaseBuffer[key]);
	    }
	    if (!LIBRARY) ArrayBufferProto.constructor = $ArrayBuffer;
	  }
	  // iOS Safari 7.x bug
	  var view = new $DataView(new $ArrayBuffer(2));
	  var $setInt8 = $DataView[PROTOTYPE].setInt8;
	  view.setInt8(0, 2147483648);
	  view.setInt8(1, 2147483649);
	  if (view.getInt8(0) || !view.getInt8(1)) redefineAll($DataView[PROTOTYPE], {
	    setInt8: function setInt8(byteOffset, value) {
	      $setInt8.call(this, byteOffset, value << 24 >> 24);
	    },
	    setUint8: function setUint8(byteOffset, value) {
	      $setInt8.call(this, byteOffset, value << 24 >> 24);
	    }
	  }, true);
	}
	setToStringTag($ArrayBuffer, ARRAY_BUFFER);
	setToStringTag($DataView, DATA_VIEW);
	hide($DataView[PROTOTYPE], $typed.VIEW, true);
	exports[ARRAY_BUFFER] = $ArrayBuffer;
	exports[DATA_VIEW] = $DataView;


/***/ }),
/* 232 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/ecma262/#sec-toindex
	var toInteger = __webpack_require__(39);
	var toLength = __webpack_require__(38);
	module.exports = function (it) {
	  if (it === undefined) return 0;
	  var number = toInteger(it);
	  var length = toLength(number);
	  if (number !== length) throw RangeError('Wrong length!');
	  return length;
	};


/***/ }),
/* 233 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	$export($export.G + $export.W + $export.F * !__webpack_require__(230).ABV, {
	  DataView: __webpack_require__(231).DataView
	});


/***/ }),
/* 234 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Int8', 1, function (init) {
	  return function Int8Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 235 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	if (__webpack_require__(6)) {
	  var LIBRARY = __webpack_require__(22);
	  var global = __webpack_require__(4);
	  var fails = __webpack_require__(7);
	  var $export = __webpack_require__(8);
	  var $typed = __webpack_require__(230);
	  var $buffer = __webpack_require__(231);
	  var ctx = __webpack_require__(23);
	  var anInstance = __webpack_require__(212);
	  var propertyDesc = __webpack_require__(17);
	  var hide = __webpack_require__(10);
	  var redefineAll = __webpack_require__(220);
	  var toInteger = __webpack_require__(39);
	  var toLength = __webpack_require__(38);
	  var toIndex = __webpack_require__(232);
	  var toAbsoluteIndex = __webpack_require__(40);
	  var toPrimitive = __webpack_require__(16);
	  var has = __webpack_require__(5);
	  var classof = __webpack_require__(75);
	  var isObject = __webpack_require__(13);
	  var toObject = __webpack_require__(46);
	  var isArrayIter = __webpack_require__(164);
	  var create = __webpack_require__(47);
	  var getPrototypeOf = __webpack_require__(59);
	  var gOPN = __webpack_require__(51).f;
	  var getIterFn = __webpack_require__(166);
	  var uid = __webpack_require__(19);
	  var wks = __webpack_require__(27);
	  var createArrayMethod = __webpack_require__(174);
	  var createArrayIncludes = __webpack_require__(37);
	  var speciesConstructor = __webpack_require__(210);
	  var ArrayIterators = __webpack_require__(195);
	  var Iterators = __webpack_require__(130);
	  var $iterDetect = __webpack_require__(167);
	  var setSpecies = __webpack_require__(194);
	  var arrayFill = __webpack_require__(190);
	  var arrayCopyWithin = __webpack_require__(187);
	  var $DP = __webpack_require__(11);
	  var $GOPD = __webpack_require__(52);
	  var dP = $DP.f;
	  var gOPD = $GOPD.f;
	  var RangeError = global.RangeError;
	  var TypeError = global.TypeError;
	  var Uint8Array = global.Uint8Array;
	  var ARRAY_BUFFER = 'ArrayBuffer';
	  var SHARED_BUFFER = 'Shared' + ARRAY_BUFFER;
	  var BYTES_PER_ELEMENT = 'BYTES_PER_ELEMENT';
	  var PROTOTYPE = 'prototype';
	  var ArrayProto = Array[PROTOTYPE];
	  var $ArrayBuffer = $buffer.ArrayBuffer;
	  var $DataView = $buffer.DataView;
	  var arrayForEach = createArrayMethod(0);
	  var arrayFilter = createArrayMethod(2);
	  var arraySome = createArrayMethod(3);
	  var arrayEvery = createArrayMethod(4);
	  var arrayFind = createArrayMethod(5);
	  var arrayFindIndex = createArrayMethod(6);
	  var arrayIncludes = createArrayIncludes(true);
	  var arrayIndexOf = createArrayIncludes(false);
	  var arrayValues = ArrayIterators.values;
	  var arrayKeys = ArrayIterators.keys;
	  var arrayEntries = ArrayIterators.entries;
	  var arrayLastIndexOf = ArrayProto.lastIndexOf;
	  var arrayReduce = ArrayProto.reduce;
	  var arrayReduceRight = ArrayProto.reduceRight;
	  var arrayJoin = ArrayProto.join;
	  var arraySort = ArrayProto.sort;
	  var arraySlice = ArrayProto.slice;
	  var arrayToString = ArrayProto.toString;
	  var arrayToLocaleString = ArrayProto.toLocaleString;
	  var ITERATOR = wks('iterator');
	  var TAG = wks('toStringTag');
	  var TYPED_CONSTRUCTOR = uid('typed_constructor');
	  var DEF_CONSTRUCTOR = uid('def_constructor');
	  var ALL_CONSTRUCTORS = $typed.CONSTR;
	  var TYPED_ARRAY = $typed.TYPED;
	  var VIEW = $typed.VIEW;
	  var WRONG_LENGTH = 'Wrong length!';

	  var $map = createArrayMethod(1, function (O, length) {
	    return allocate(speciesConstructor(O, O[DEF_CONSTRUCTOR]), length);
	  });

	  var LITTLE_ENDIAN = fails(function () {
	    // eslint-disable-next-line no-undef
	    return new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
	  });

	  var FORCED_SET = !!Uint8Array && !!Uint8Array[PROTOTYPE].set && fails(function () {
	    new Uint8Array(1).set({});
	  });

	  var toOffset = function (it, BYTES) {
	    var offset = toInteger(it);
	    if (offset < 0 || offset % BYTES) throw RangeError('Wrong offset!');
	    return offset;
	  };

	  var validate = function (it) {
	    if (isObject(it) && TYPED_ARRAY in it) return it;
	    throw TypeError(it + ' is not a typed array!');
	  };

	  var allocate = function (C, length) {
	    if (!(isObject(C) && TYPED_CONSTRUCTOR in C)) {
	      throw TypeError('It is not a typed array constructor!');
	    } return new C(length);
	  };

	  var speciesFromList = function (O, list) {
	    return fromList(speciesConstructor(O, O[DEF_CONSTRUCTOR]), list);
	  };

	  var fromList = function (C, list) {
	    var index = 0;
	    var length = list.length;
	    var result = allocate(C, length);
	    while (length > index) result[index] = list[index++];
	    return result;
	  };

	  var addGetter = function (it, key, internal) {
	    dP(it, key, { get: function () { return this._d[internal]; } });
	  };

	  var $from = function from(source /* , mapfn, thisArg */) {
	    var O = toObject(source);
	    var aLen = arguments.length;
	    var mapfn = aLen > 1 ? arguments[1] : undefined;
	    var mapping = mapfn !== undefined;
	    var iterFn = getIterFn(O);
	    var i, length, values, result, step, iterator;
	    if (iterFn != undefined && !isArrayIter(iterFn)) {
	      for (iterator = iterFn.call(O), values = [], i = 0; !(step = iterator.next()).done; i++) {
	        values.push(step.value);
	      } O = values;
	    }
	    if (mapping && aLen > 2) mapfn = ctx(mapfn, arguments[2], 2);
	    for (i = 0, length = toLength(O.length), result = allocate(this, length); length > i; i++) {
	      result[i] = mapping ? mapfn(O[i], i) : O[i];
	    }
	    return result;
	  };

	  var $of = function of(/* ...items */) {
	    var index = 0;
	    var length = arguments.length;
	    var result = allocate(this, length);
	    while (length > index) result[index] = arguments[index++];
	    return result;
	  };

	  // iOS Safari 6.x fails here
	  var TO_LOCALE_BUG = !!Uint8Array && fails(function () { arrayToLocaleString.call(new Uint8Array(1)); });

	  var $toLocaleString = function toLocaleString() {
	    return arrayToLocaleString.apply(TO_LOCALE_BUG ? arraySlice.call(validate(this)) : validate(this), arguments);
	  };

	  var proto = {
	    copyWithin: function copyWithin(target, start /* , end */) {
	      return arrayCopyWithin.call(validate(this), target, start, arguments.length > 2 ? arguments[2] : undefined);
	    },
	    every: function every(callbackfn /* , thisArg */) {
	      return arrayEvery(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    fill: function fill(value /* , start, end */) { // eslint-disable-line no-unused-vars
	      return arrayFill.apply(validate(this), arguments);
	    },
	    filter: function filter(callbackfn /* , thisArg */) {
	      return speciesFromList(this, arrayFilter(validate(this), callbackfn,
	        arguments.length > 1 ? arguments[1] : undefined));
	    },
	    find: function find(predicate /* , thisArg */) {
	      return arrayFind(validate(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    findIndex: function findIndex(predicate /* , thisArg */) {
	      return arrayFindIndex(validate(this), predicate, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    forEach: function forEach(callbackfn /* , thisArg */) {
	      arrayForEach(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    indexOf: function indexOf(searchElement /* , fromIndex */) {
	      return arrayIndexOf(validate(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    includes: function includes(searchElement /* , fromIndex */) {
	      return arrayIncludes(validate(this), searchElement, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    join: function join(separator) { // eslint-disable-line no-unused-vars
	      return arrayJoin.apply(validate(this), arguments);
	    },
	    lastIndexOf: function lastIndexOf(searchElement /* , fromIndex */) { // eslint-disable-line no-unused-vars
	      return arrayLastIndexOf.apply(validate(this), arguments);
	    },
	    map: function map(mapfn /* , thisArg */) {
	      return $map(validate(this), mapfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    reduce: function reduce(callbackfn /* , initialValue */) { // eslint-disable-line no-unused-vars
	      return arrayReduce.apply(validate(this), arguments);
	    },
	    reduceRight: function reduceRight(callbackfn /* , initialValue */) { // eslint-disable-line no-unused-vars
	      return arrayReduceRight.apply(validate(this), arguments);
	    },
	    reverse: function reverse() {
	      var that = this;
	      var length = validate(that).length;
	      var middle = Math.floor(length / 2);
	      var index = 0;
	      var value;
	      while (index < middle) {
	        value = that[index];
	        that[index++] = that[--length];
	        that[length] = value;
	      } return that;
	    },
	    some: function some(callbackfn /* , thisArg */) {
	      return arraySome(validate(this), callbackfn, arguments.length > 1 ? arguments[1] : undefined);
	    },
	    sort: function sort(comparefn) {
	      return arraySort.call(validate(this), comparefn);
	    },
	    subarray: function subarray(begin, end) {
	      var O = validate(this);
	      var length = O.length;
	      var $begin = toAbsoluteIndex(begin, length);
	      return new (speciesConstructor(O, O[DEF_CONSTRUCTOR]))(
	        O.buffer,
	        O.byteOffset + $begin * O.BYTES_PER_ELEMENT,
	        toLength((end === undefined ? length : toAbsoluteIndex(end, length)) - $begin)
	      );
	    }
	  };

	  var $slice = function slice(start, end) {
	    return speciesFromList(this, arraySlice.call(validate(this), start, end));
	  };

	  var $set = function set(arrayLike /* , offset */) {
	    validate(this);
	    var offset = toOffset(arguments[1], 1);
	    var length = this.length;
	    var src = toObject(arrayLike);
	    var len = toLength(src.length);
	    var index = 0;
	    if (len + offset > length) throw RangeError(WRONG_LENGTH);
	    while (index < len) this[offset + index] = src[index++];
	  };

	  var $iterators = {
	    entries: function entries() {
	      return arrayEntries.call(validate(this));
	    },
	    keys: function keys() {
	      return arrayKeys.call(validate(this));
	    },
	    values: function values() {
	      return arrayValues.call(validate(this));
	    }
	  };

	  var isTAIndex = function (target, key) {
	    return isObject(target)
	      && target[TYPED_ARRAY]
	      && typeof key != 'symbol'
	      && key in target
	      && String(+key) == String(key);
	  };
	  var $getDesc = function getOwnPropertyDescriptor(target, key) {
	    return isTAIndex(target, key = toPrimitive(key, true))
	      ? propertyDesc(2, target[key])
	      : gOPD(target, key);
	  };
	  var $setDesc = function defineProperty(target, key, desc) {
	    if (isTAIndex(target, key = toPrimitive(key, true))
	      && isObject(desc)
	      && has(desc, 'value')
	      && !has(desc, 'get')
	      && !has(desc, 'set')
	      // TODO: add validation descriptor w/o calling accessors
	      && !desc.configurable
	      && (!has(desc, 'writable') || desc.writable)
	      && (!has(desc, 'enumerable') || desc.enumerable)
	    ) {
	      target[key] = desc.value;
	      return target;
	    } return dP(target, key, desc);
	  };

	  if (!ALL_CONSTRUCTORS) {
	    $GOPD.f = $getDesc;
	    $DP.f = $setDesc;
	  }

	  $export($export.S + $export.F * !ALL_CONSTRUCTORS, 'Object', {
	    getOwnPropertyDescriptor: $getDesc,
	    defineProperty: $setDesc
	  });

	  if (fails(function () { arrayToString.call({}); })) {
	    arrayToString = arrayToLocaleString = function toString() {
	      return arrayJoin.call(this);
	    };
	  }

	  var $TypedArrayPrototype$ = redefineAll({}, proto);
	  redefineAll($TypedArrayPrototype$, $iterators);
	  hide($TypedArrayPrototype$, ITERATOR, $iterators.values);
	  redefineAll($TypedArrayPrototype$, {
	    slice: $slice,
	    set: $set,
	    constructor: function () { /* noop */ },
	    toString: arrayToString,
	    toLocaleString: $toLocaleString
	  });
	  addGetter($TypedArrayPrototype$, 'buffer', 'b');
	  addGetter($TypedArrayPrototype$, 'byteOffset', 'o');
	  addGetter($TypedArrayPrototype$, 'byteLength', 'l');
	  addGetter($TypedArrayPrototype$, 'length', 'e');
	  dP($TypedArrayPrototype$, TAG, {
	    get: function () { return this[TYPED_ARRAY]; }
	  });

	  // eslint-disable-next-line max-statements
	  module.exports = function (KEY, BYTES, wrapper, CLAMPED) {
	    CLAMPED = !!CLAMPED;
	    var NAME = KEY + (CLAMPED ? 'Clamped' : '') + 'Array';
	    var GETTER = 'get' + KEY;
	    var SETTER = 'set' + KEY;
	    var TypedArray = global[NAME];
	    var Base = TypedArray || {};
	    var TAC = TypedArray && getPrototypeOf(TypedArray);
	    var FORCED = !TypedArray || !$typed.ABV;
	    var O = {};
	    var TypedArrayPrototype = TypedArray && TypedArray[PROTOTYPE];
	    var getter = function (that, index) {
	      var data = that._d;
	      return data.v[GETTER](index * BYTES + data.o, LITTLE_ENDIAN);
	    };
	    var setter = function (that, index, value) {
	      var data = that._d;
	      if (CLAMPED) value = (value = Math.round(value)) < 0 ? 0 : value > 0xff ? 0xff : value & 0xff;
	      data.v[SETTER](index * BYTES + data.o, value, LITTLE_ENDIAN);
	    };
	    var addElement = function (that, index) {
	      dP(that, index, {
	        get: function () {
	          return getter(this, index);
	        },
	        set: function (value) {
	          return setter(this, index, value);
	        },
	        enumerable: true
	      });
	    };
	    if (FORCED) {
	      TypedArray = wrapper(function (that, data, $offset, $length) {
	        anInstance(that, TypedArray, NAME, '_d');
	        var index = 0;
	        var offset = 0;
	        var buffer, byteLength, length, klass;
	        if (!isObject(data)) {
	          length = toIndex(data);
	          byteLength = length * BYTES;
	          buffer = new $ArrayBuffer(byteLength);
	        } else if (data instanceof $ArrayBuffer || (klass = classof(data)) == ARRAY_BUFFER || klass == SHARED_BUFFER) {
	          buffer = data;
	          offset = toOffset($offset, BYTES);
	          var $len = data.byteLength;
	          if ($length === undefined) {
	            if ($len % BYTES) throw RangeError(WRONG_LENGTH);
	            byteLength = $len - offset;
	            if (byteLength < 0) throw RangeError(WRONG_LENGTH);
	          } else {
	            byteLength = toLength($length) * BYTES;
	            if (byteLength + offset > $len) throw RangeError(WRONG_LENGTH);
	          }
	          length = byteLength / BYTES;
	        } else if (TYPED_ARRAY in data) {
	          return fromList(TypedArray, data);
	        } else {
	          return $from.call(TypedArray, data);
	        }
	        hide(that, '_d', {
	          b: buffer,
	          o: offset,
	          l: byteLength,
	          e: length,
	          v: new $DataView(buffer)
	        });
	        while (index < length) addElement(that, index++);
	      });
	      TypedArrayPrototype = TypedArray[PROTOTYPE] = create($TypedArrayPrototype$);
	      hide(TypedArrayPrototype, 'constructor', TypedArray);
	    } else if (!fails(function () {
	      TypedArray(1);
	    }) || !fails(function () {
	      new TypedArray(-1); // eslint-disable-line no-new
	    }) || !$iterDetect(function (iter) {
	      new TypedArray(); // eslint-disable-line no-new
	      new TypedArray(null); // eslint-disable-line no-new
	      new TypedArray(1.5); // eslint-disable-line no-new
	      new TypedArray(iter); // eslint-disable-line no-new
	    }, true)) {
	      TypedArray = wrapper(function (that, data, $offset, $length) {
	        anInstance(that, TypedArray, NAME);
	        var klass;
	        // `ws` module bug, temporarily remove validation length for Uint8Array
	        // https://github.com/websockets/ws/pull/645
	        if (!isObject(data)) return new Base(toIndex(data));
	        if (data instanceof $ArrayBuffer || (klass = classof(data)) == ARRAY_BUFFER || klass == SHARED_BUFFER) {
	          return $length !== undefined
	            ? new Base(data, toOffset($offset, BYTES), $length)
	            : $offset !== undefined
	              ? new Base(data, toOffset($offset, BYTES))
	              : new Base(data);
	        }
	        if (TYPED_ARRAY in data) return fromList(TypedArray, data);
	        return $from.call(TypedArray, data);
	      });
	      arrayForEach(TAC !== Function.prototype ? gOPN(Base).concat(gOPN(TAC)) : gOPN(Base), function (key) {
	        if (!(key in TypedArray)) hide(TypedArray, key, Base[key]);
	      });
	      TypedArray[PROTOTYPE] = TypedArrayPrototype;
	      if (!LIBRARY) TypedArrayPrototype.constructor = TypedArray;
	    }
	    var $nativeIterator = TypedArrayPrototype[ITERATOR];
	    var CORRECT_ITER_NAME = !!$nativeIterator
	      && ($nativeIterator.name == 'values' || $nativeIterator.name == undefined);
	    var $iterator = $iterators.values;
	    hide(TypedArray, TYPED_CONSTRUCTOR, true);
	    hide(TypedArrayPrototype, TYPED_ARRAY, NAME);
	    hide(TypedArrayPrototype, VIEW, true);
	    hide(TypedArrayPrototype, DEF_CONSTRUCTOR, TypedArray);

	    if (CLAMPED ? new TypedArray(1)[TAG] != NAME : !(TAG in TypedArrayPrototype)) {
	      dP(TypedArrayPrototype, TAG, {
	        get: function () { return NAME; }
	      });
	    }

	    O[NAME] = TypedArray;

	    $export($export.G + $export.W + $export.F * (TypedArray != Base), O);

	    $export($export.S, NAME, {
	      BYTES_PER_ELEMENT: BYTES
	    });

	    $export($export.S + $export.F * fails(function () { Base.of.call(TypedArray, 1); }), NAME, {
	      from: $from,
	      of: $of
	    });

	    if (!(BYTES_PER_ELEMENT in TypedArrayPrototype)) hide(TypedArrayPrototype, BYTES_PER_ELEMENT, BYTES);

	    $export($export.P, NAME, proto);

	    setSpecies(NAME);

	    $export($export.P + $export.F * FORCED_SET, NAME, { set: $set });

	    $export($export.P + $export.F * !CORRECT_ITER_NAME, NAME, $iterators);

	    if (!LIBRARY && TypedArrayPrototype.toString != arrayToString) TypedArrayPrototype.toString = arrayToString;

	    $export($export.P + $export.F * fails(function () {
	      new TypedArray(1).slice();
	    }), NAME, { slice: $slice });

	    $export($export.P + $export.F * (fails(function () {
	      return [1, 2].toLocaleString() != new TypedArray([1, 2]).toLocaleString();
	    }) || !fails(function () {
	      TypedArrayPrototype.toLocaleString.call([1, 2]);
	    })), NAME, { toLocaleString: $toLocaleString });

	    Iterators[NAME] = CORRECT_ITER_NAME ? $nativeIterator : $iterator;
	    if (!LIBRARY && !CORRECT_ITER_NAME) hide(TypedArrayPrototype, ITERATOR, $iterator);
	  };
	} else module.exports = function () { /* empty */ };


/***/ }),
/* 236 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Uint8', 1, function (init) {
	  return function Uint8Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 237 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Uint8', 1, function (init) {
	  return function Uint8ClampedArray(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	}, true);


/***/ }),
/* 238 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Int16', 2, function (init) {
	  return function Int16Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 239 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Uint16', 2, function (init) {
	  return function Uint16Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 240 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Int32', 4, function (init) {
	  return function Int32Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 241 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Uint32', 4, function (init) {
	  return function Uint32Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 242 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Float32', 4, function (init) {
	  return function Float32Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 243 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(235)('Float64', 8, function (init) {
	  return function Float64Array(data, byteOffset, length) {
	    return init(this, data, byteOffset, length);
	  };
	});


/***/ }),
/* 244 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.1 Reflect.apply(target, thisArgument, argumentsList)
	var $export = __webpack_require__(8);
	var aFunction = __webpack_require__(24);
	var anObject = __webpack_require__(12);
	var rApply = (__webpack_require__(4).Reflect || {}).apply;
	var fApply = Function.apply;
	// MS Edge argumentsList argument is optional
	$export($export.S + $export.F * !__webpack_require__(7)(function () {
	  rApply(function () { /* empty */ });
	}), 'Reflect', {
	  apply: function apply(target, thisArgument, argumentsList) {
	    var T = aFunction(target);
	    var L = anObject(argumentsList);
	    return rApply ? rApply(T, thisArgument, L) : fApply.call(T, thisArgument, L);
	  }
	});


/***/ }),
/* 245 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.2 Reflect.construct(target, argumentsList [, newTarget])
	var $export = __webpack_require__(8);
	var create = __webpack_require__(47);
	var aFunction = __webpack_require__(24);
	var anObject = __webpack_require__(12);
	var isObject = __webpack_require__(13);
	var fails = __webpack_require__(7);
	var bind = __webpack_require__(77);
	var rConstruct = (__webpack_require__(4).Reflect || {}).construct;

	// MS Edge supports only 2 arguments and argumentsList argument is optional
	// FF Nightly sets third argument as `new.target`, but does not create `this` from it
	var NEW_TARGET_BUG = fails(function () {
	  function F() { /* empty */ }
	  return !(rConstruct(function () { /* empty */ }, [], F) instanceof F);
	});
	var ARGS_BUG = !fails(function () {
	  rConstruct(function () { /* empty */ });
	});

	$export($export.S + $export.F * (NEW_TARGET_BUG || ARGS_BUG), 'Reflect', {
	  construct: function construct(Target, args /* , newTarget */) {
	    aFunction(Target);
	    anObject(args);
	    var newTarget = arguments.length < 3 ? Target : aFunction(arguments[2]);
	    if (ARGS_BUG && !NEW_TARGET_BUG) return rConstruct(Target, args, newTarget);
	    if (Target == newTarget) {
	      // w/o altered newTarget, optimization for 0-4 arguments
	      switch (args.length) {
	        case 0: return new Target();
	        case 1: return new Target(args[0]);
	        case 2: return new Target(args[0], args[1]);
	        case 3: return new Target(args[0], args[1], args[2]);
	        case 4: return new Target(args[0], args[1], args[2], args[3]);
	      }
	      // w/o altered newTarget, lot of arguments case
	      var $args = [null];
	      $args.push.apply($args, args);
	      return new (bind.apply(Target, $args))();
	    }
	    // with altered newTarget, not support built-in constructors
	    var proto = newTarget.prototype;
	    var instance = create(isObject(proto) ? proto : Object.prototype);
	    var result = Function.apply.call(Target, instance, args);
	    return isObject(result) ? result : instance;
	  }
	});


/***/ }),
/* 246 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.3 Reflect.defineProperty(target, propertyKey, attributes)
	var dP = __webpack_require__(11);
	var $export = __webpack_require__(8);
	var anObject = __webpack_require__(12);
	var toPrimitive = __webpack_require__(16);

	// MS Edge has broken Reflect.defineProperty - throwing instead of returning false
	$export($export.S + $export.F * __webpack_require__(7)(function () {
	  // eslint-disable-next-line no-undef
	  Reflect.defineProperty(dP.f({}, 1, { value: 1 }), 1, { value: 2 });
	}), 'Reflect', {
	  defineProperty: function defineProperty(target, propertyKey, attributes) {
	    anObject(target);
	    propertyKey = toPrimitive(propertyKey, true);
	    anObject(attributes);
	    try {
	      dP.f(target, propertyKey, attributes);
	      return true;
	    } catch (e) {
	      return false;
	    }
	  }
	});


/***/ }),
/* 247 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.4 Reflect.deleteProperty(target, propertyKey)
	var $export = __webpack_require__(8);
	var gOPD = __webpack_require__(52).f;
	var anObject = __webpack_require__(12);

	$export($export.S, 'Reflect', {
	  deleteProperty: function deleteProperty(target, propertyKey) {
	    var desc = gOPD(anObject(target), propertyKey);
	    return desc && !desc.configurable ? false : delete target[propertyKey];
	  }
	});


/***/ }),
/* 248 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// 26.1.5 Reflect.enumerate(target)
	var $export = __webpack_require__(8);
	var anObject = __webpack_require__(12);
	var Enumerate = function (iterated) {
	  this._t = anObject(iterated); // target
	  this._i = 0;                  // next index
	  var keys = this._k = [];      // keys
	  var key;
	  for (key in iterated) keys.push(key);
	};
	__webpack_require__(131)(Enumerate, 'Object', function () {
	  var that = this;
	  var keys = that._k;
	  var key;
	  do {
	    if (that._i >= keys.length) return { value: undefined, done: true };
	  } while (!((key = keys[that._i++]) in that._t));
	  return { value: key, done: false };
	});

	$export($export.S, 'Reflect', {
	  enumerate: function enumerate(target) {
	    return new Enumerate(target);
	  }
	});


/***/ }),
/* 249 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.6 Reflect.get(target, propertyKey [, receiver])
	var gOPD = __webpack_require__(52);
	var getPrototypeOf = __webpack_require__(59);
	var has = __webpack_require__(5);
	var $export = __webpack_require__(8);
	var isObject = __webpack_require__(13);
	var anObject = __webpack_require__(12);

	function get(target, propertyKey /* , receiver */) {
	  var receiver = arguments.length < 3 ? target : arguments[2];
	  var desc, proto;
	  if (anObject(target) === receiver) return target[propertyKey];
	  if (desc = gOPD.f(target, propertyKey)) return has(desc, 'value')
	    ? desc.value
	    : desc.get !== undefined
	      ? desc.get.call(receiver)
	      : undefined;
	  if (isObject(proto = getPrototypeOf(target))) return get(proto, propertyKey, receiver);
	}

	$export($export.S, 'Reflect', { get: get });


/***/ }),
/* 250 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.7 Reflect.getOwnPropertyDescriptor(target, propertyKey)
	var gOPD = __webpack_require__(52);
	var $export = __webpack_require__(8);
	var anObject = __webpack_require__(12);

	$export($export.S, 'Reflect', {
	  getOwnPropertyDescriptor: function getOwnPropertyDescriptor(target, propertyKey) {
	    return gOPD.f(anObject(target), propertyKey);
	  }
	});


/***/ }),
/* 251 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.8 Reflect.getPrototypeOf(target)
	var $export = __webpack_require__(8);
	var getProto = __webpack_require__(59);
	var anObject = __webpack_require__(12);

	$export($export.S, 'Reflect', {
	  getPrototypeOf: function getPrototypeOf(target) {
	    return getProto(anObject(target));
	  }
	});


/***/ }),
/* 252 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.9 Reflect.has(target, propertyKey)
	var $export = __webpack_require__(8);

	$export($export.S, 'Reflect', {
	  has: function has(target, propertyKey) {
	    return propertyKey in target;
	  }
	});


/***/ }),
/* 253 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.10 Reflect.isExtensible(target)
	var $export = __webpack_require__(8);
	var anObject = __webpack_require__(12);
	var $isExtensible = Object.isExtensible;

	$export($export.S, 'Reflect', {
	  isExtensible: function isExtensible(target) {
	    anObject(target);
	    return $isExtensible ? $isExtensible(target) : true;
	  }
	});


/***/ }),
/* 254 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.11 Reflect.ownKeys(target)
	var $export = __webpack_require__(8);

	$export($export.S, 'Reflect', { ownKeys: __webpack_require__(255) });


/***/ }),
/* 255 */
/***/ (function(module, exports, __webpack_require__) {

	// all object keys, includes non-enumerable and symbols
	var gOPN = __webpack_require__(51);
	var gOPS = __webpack_require__(43);
	var anObject = __webpack_require__(12);
	var Reflect = __webpack_require__(4).Reflect;
	module.exports = Reflect && Reflect.ownKeys || function ownKeys(it) {
	  var keys = gOPN.f(anObject(it));
	  var getSymbols = gOPS.f;
	  return getSymbols ? keys.concat(getSymbols(it)) : keys;
	};


/***/ }),
/* 256 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.12 Reflect.preventExtensions(target)
	var $export = __webpack_require__(8);
	var anObject = __webpack_require__(12);
	var $preventExtensions = Object.preventExtensions;

	$export($export.S, 'Reflect', {
	  preventExtensions: function preventExtensions(target) {
	    anObject(target);
	    try {
	      if ($preventExtensions) $preventExtensions(target);
	      return true;
	    } catch (e) {
	      return false;
	    }
	  }
	});


/***/ }),
/* 257 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.13 Reflect.set(target, propertyKey, V [, receiver])
	var dP = __webpack_require__(11);
	var gOPD = __webpack_require__(52);
	var getPrototypeOf = __webpack_require__(59);
	var has = __webpack_require__(5);
	var $export = __webpack_require__(8);
	var createDesc = __webpack_require__(17);
	var anObject = __webpack_require__(12);
	var isObject = __webpack_require__(13);

	function set(target, propertyKey, V /* , receiver */) {
	  var receiver = arguments.length < 4 ? target : arguments[3];
	  var ownDesc = gOPD.f(anObject(target), propertyKey);
	  var existingDescriptor, proto;
	  if (!ownDesc) {
	    if (isObject(proto = getPrototypeOf(target))) {
	      return set(proto, propertyKey, V, receiver);
	    }
	    ownDesc = createDesc(0);
	  }
	  if (has(ownDesc, 'value')) {
	    if (ownDesc.writable === false || !isObject(receiver)) return false;
	    if (existingDescriptor = gOPD.f(receiver, propertyKey)) {
	      if (existingDescriptor.get || existingDescriptor.set || existingDescriptor.writable === false) return false;
	      existingDescriptor.value = V;
	      dP.f(receiver, propertyKey, existingDescriptor);
	    } else dP.f(receiver, propertyKey, createDesc(0, V));
	    return true;
	  }
	  return ownDesc.set === undefined ? false : (ownDesc.set.call(receiver, V), true);
	}

	$export($export.S, 'Reflect', { set: set });


/***/ }),
/* 258 */
/***/ (function(module, exports, __webpack_require__) {

	// 26.1.14 Reflect.setPrototypeOf(target, proto)
	var $export = __webpack_require__(8);
	var setProto = __webpack_require__(73);

	if (setProto) $export($export.S, 'Reflect', {
	  setPrototypeOf: function setPrototypeOf(target, proto) {
	    setProto.check(target, proto);
	    try {
	      setProto.set(target, proto);
	      return true;
	    } catch (e) {
	      return false;
	    }
	  }
	});


/***/ }),
/* 259 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/tc39/Array.prototype.includes
	var $export = __webpack_require__(8);
	var $includes = __webpack_require__(37)(true);

	$export($export.P, 'Array', {
	  includes: function includes(el /* , fromIndex = 0 */) {
	    return $includes(this, el, arguments.length > 1 ? arguments[1] : undefined);
	  }
	});

	__webpack_require__(188)('includes');


/***/ }),
/* 260 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/proposal-flatMap/#sec-Array.prototype.flatMap
	var $export = __webpack_require__(8);
	var flattenIntoArray = __webpack_require__(261);
	var toObject = __webpack_require__(46);
	var toLength = __webpack_require__(38);
	var aFunction = __webpack_require__(24);
	var arraySpeciesCreate = __webpack_require__(175);

	$export($export.P, 'Array', {
	  flatMap: function flatMap(callbackfn /* , thisArg */) {
	    var O = toObject(this);
	    var sourceLen, A;
	    aFunction(callbackfn);
	    sourceLen = toLength(O.length);
	    A = arraySpeciesCreate(O, 0);
	    flattenIntoArray(A, O, O, sourceLen, 0, 1, callbackfn, arguments[1]);
	    return A;
	  }
	});

	__webpack_require__(188)('flatMap');


/***/ }),
/* 261 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/proposal-flatMap/#sec-FlattenIntoArray
	var isArray = __webpack_require__(45);
	var isObject = __webpack_require__(13);
	var toLength = __webpack_require__(38);
	var ctx = __webpack_require__(23);
	var IS_CONCAT_SPREADABLE = __webpack_require__(27)('isConcatSpreadable');

	function flattenIntoArray(target, original, source, sourceLen, start, depth, mapper, thisArg) {
	  var targetIndex = start;
	  var sourceIndex = 0;
	  var mapFn = mapper ? ctx(mapper, thisArg, 3) : false;
	  var element, spreadable;

	  while (sourceIndex < sourceLen) {
	    if (sourceIndex in source) {
	      element = mapFn ? mapFn(source[sourceIndex], sourceIndex, original) : source[sourceIndex];

	      spreadable = false;
	      if (isObject(element)) {
	        spreadable = element[IS_CONCAT_SPREADABLE];
	        spreadable = spreadable !== undefined ? !!spreadable : isArray(element);
	      }

	      if (spreadable && depth > 0) {
	        targetIndex = flattenIntoArray(target, original, element, toLength(element.length), targetIndex, depth - 1) - 1;
	      } else {
	        if (targetIndex >= 0x1fffffffffffff) throw TypeError();
	        target[targetIndex] = element;
	      }

	      targetIndex++;
	    }
	    sourceIndex++;
	  }
	  return targetIndex;
	}

	module.exports = flattenIntoArray;


/***/ }),
/* 262 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/proposal-flatMap/#sec-Array.prototype.flatten
	var $export = __webpack_require__(8);
	var flattenIntoArray = __webpack_require__(261);
	var toObject = __webpack_require__(46);
	var toLength = __webpack_require__(38);
	var toInteger = __webpack_require__(39);
	var arraySpeciesCreate = __webpack_require__(175);

	$export($export.P, 'Array', {
	  flatten: function flatten(/* depthArg = 1 */) {
	    var depthArg = arguments[0];
	    var O = toObject(this);
	    var sourceLen = toLength(O.length);
	    var A = arraySpeciesCreate(O, 0);
	    flattenIntoArray(A, O, O, sourceLen, 0, depthArg === undefined ? 1 : toInteger(depthArg));
	    return A;
	  }
	});

	__webpack_require__(188)('flatten');


/***/ }),
/* 263 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/mathiasbynens/String.prototype.at
	var $export = __webpack_require__(8);
	var $at = __webpack_require__(128)(true);

	$export($export.P, 'String', {
	  at: function at(pos) {
	    return $at(this, pos);
	  }
	});


/***/ }),
/* 264 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/tc39/proposal-string-pad-start-end
	var $export = __webpack_require__(8);
	var $pad = __webpack_require__(265);
	var userAgent = __webpack_require__(218);

	// https://github.com/zloirock/core-js/issues/280
	var WEBKIT_BUG = /Version\/10\.\d+(\.\d+)?( Mobile\/\w+)? Safari\//.test(userAgent);

	$export($export.P + $export.F * WEBKIT_BUG, 'String', {
	  padStart: function padStart(maxLength /* , fillString = ' ' */) {
	    return $pad(this, maxLength, arguments.length > 1 ? arguments[1] : undefined, true);
	  }
	});


/***/ }),
/* 265 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-string-pad-start-end
	var toLength = __webpack_require__(38);
	var repeat = __webpack_require__(91);
	var defined = __webpack_require__(36);

	module.exports = function (that, maxLength, fillString, left) {
	  var S = String(defined(that));
	  var stringLength = S.length;
	  var fillStr = fillString === undefined ? ' ' : String(fillString);
	  var intMaxLength = toLength(maxLength);
	  if (intMaxLength <= stringLength || fillStr == '') return S;
	  var fillLen = intMaxLength - stringLength;
	  var stringFiller = repeat.call(fillStr, Math.ceil(fillLen / fillStr.length));
	  if (stringFiller.length > fillLen) stringFiller = stringFiller.slice(0, fillLen);
	  return left ? stringFiller + S : S + stringFiller;
	};


/***/ }),
/* 266 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/tc39/proposal-string-pad-start-end
	var $export = __webpack_require__(8);
	var $pad = __webpack_require__(265);
	var userAgent = __webpack_require__(218);

	// https://github.com/zloirock/core-js/issues/280
	var WEBKIT_BUG = /Version\/10\.\d+(\.\d+)?( Mobile\/\w+)? Safari\//.test(userAgent);

	$export($export.P + $export.F * WEBKIT_BUG, 'String', {
	  padEnd: function padEnd(maxLength /* , fillString = ' ' */) {
	    return $pad(this, maxLength, arguments.length > 1 ? arguments[1] : undefined, false);
	  }
	});


/***/ }),
/* 267 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/sebmarkbage/ecmascript-string-left-right-trim
	__webpack_require__(83)('trimLeft', function ($trim) {
	  return function trimLeft() {
	    return $trim(this, 1);
	  };
	}, 'trimStart');


/***/ }),
/* 268 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/sebmarkbage/ecmascript-string-left-right-trim
	__webpack_require__(83)('trimRight', function ($trim) {
	  return function trimRight() {
	    return $trim(this, 2);
	  };
	}, 'trimEnd');


/***/ }),
/* 269 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/String.prototype.matchAll/
	var $export = __webpack_require__(8);
	var defined = __webpack_require__(36);
	var toLength = __webpack_require__(38);
	var isRegExp = __webpack_require__(135);
	var getFlags = __webpack_require__(198);
	var RegExpProto = RegExp.prototype;

	var $RegExpStringIterator = function (regexp, string) {
	  this._r = regexp;
	  this._s = string;
	};

	__webpack_require__(131)($RegExpStringIterator, 'RegExp String', function next() {
	  var match = this._r.exec(this._s);
	  return { value: match, done: match === null };
	});

	$export($export.P, 'String', {
	  matchAll: function matchAll(regexp) {
	    defined(this);
	    if (!isRegExp(regexp)) throw TypeError(regexp + ' is not a regexp!');
	    var S = String(this);
	    var flags = 'flags' in RegExpProto ? String(regexp.flags) : getFlags.call(regexp);
	    var rx = new RegExp(regexp.source, ~flags.indexOf('g') ? flags : 'g' + flags);
	    rx.lastIndex = toLength(regexp.lastIndex);
	    return new $RegExpStringIterator(rx, S);
	  }
	});


/***/ }),
/* 270 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(29)('asyncIterator');


/***/ }),
/* 271 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(29)('observable');


/***/ }),
/* 272 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-object-getownpropertydescriptors
	var $export = __webpack_require__(8);
	var ownKeys = __webpack_require__(255);
	var toIObject = __webpack_require__(33);
	var gOPD = __webpack_require__(52);
	var createProperty = __webpack_require__(165);

	$export($export.S, 'Object', {
	  getOwnPropertyDescriptors: function getOwnPropertyDescriptors(object) {
	    var O = toIObject(object);
	    var getDesc = gOPD.f;
	    var keys = ownKeys(O);
	    var result = {};
	    var i = 0;
	    var key, desc;
	    while (keys.length > i) {
	      desc = getDesc(O, key = keys[i++]);
	      if (desc !== undefined) createProperty(result, key, desc);
	    }
	    return result;
	  }
	});


/***/ }),
/* 273 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-object-values-entries
	var $export = __webpack_require__(8);
	var $values = __webpack_require__(274)(false);

	$export($export.S, 'Object', {
	  values: function values(it) {
	    return $values(it);
	  }
	});


/***/ }),
/* 274 */
/***/ (function(module, exports, __webpack_require__) {

	var DESCRIPTORS = __webpack_require__(6);
	var getKeys = __webpack_require__(31);
	var toIObject = __webpack_require__(33);
	var isEnum = __webpack_require__(44).f;
	module.exports = function (isEntries) {
	  return function (it) {
	    var O = toIObject(it);
	    var keys = getKeys(O);
	    var length = keys.length;
	    var i = 0;
	    var result = [];
	    var key;
	    while (length > i) {
	      key = keys[i++];
	      if (!DESCRIPTORS || isEnum.call(O, key)) {
	        result.push(isEntries ? [key, O[key]] : O[key]);
	      }
	    }
	    return result;
	  };
	};


/***/ }),
/* 275 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-object-values-entries
	var $export = __webpack_require__(8);
	var $entries = __webpack_require__(274)(true);

	$export($export.S, 'Object', {
	  entries: function entries(it) {
	    return $entries(it);
	  }
	});


/***/ }),
/* 276 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var aFunction = __webpack_require__(24);
	var $defineProperty = __webpack_require__(11);

	// B.2.2.2 Object.prototype.__defineGetter__(P, getter)
	__webpack_require__(6) && $export($export.P + __webpack_require__(277), 'Object', {
	  __defineGetter__: function __defineGetter__(P, getter) {
	    $defineProperty.f(toObject(this), P, { get: aFunction(getter), enumerable: true, configurable: true });
	  }
	});


/***/ }),
/* 277 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// Forced replacement prototype accessors methods
	module.exports = __webpack_require__(22) || !__webpack_require__(7)(function () {
	  var K = Math.random();
	  // In FF throws only define methods
	  // eslint-disable-next-line no-undef, no-useless-call
	  __defineSetter__.call(null, K, function () { /* empty */ });
	  delete __webpack_require__(4)[K];
	});


/***/ }),
/* 278 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var aFunction = __webpack_require__(24);
	var $defineProperty = __webpack_require__(11);

	// B.2.2.3 Object.prototype.__defineSetter__(P, setter)
	__webpack_require__(6) && $export($export.P + __webpack_require__(277), 'Object', {
	  __defineSetter__: function __defineSetter__(P, setter) {
	    $defineProperty.f(toObject(this), P, { set: aFunction(setter), enumerable: true, configurable: true });
	  }
	});


/***/ }),
/* 279 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var toPrimitive = __webpack_require__(16);
	var getPrototypeOf = __webpack_require__(59);
	var getOwnPropertyDescriptor = __webpack_require__(52).f;

	// B.2.2.4 Object.prototype.__lookupGetter__(P)
	__webpack_require__(6) && $export($export.P + __webpack_require__(277), 'Object', {
	  __lookupGetter__: function __lookupGetter__(P) {
	    var O = toObject(this);
	    var K = toPrimitive(P, true);
	    var D;
	    do {
	      if (D = getOwnPropertyDescriptor(O, K)) return D.get;
	    } while (O = getPrototypeOf(O));
	  }
	});


/***/ }),
/* 280 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	var $export = __webpack_require__(8);
	var toObject = __webpack_require__(46);
	var toPrimitive = __webpack_require__(16);
	var getPrototypeOf = __webpack_require__(59);
	var getOwnPropertyDescriptor = __webpack_require__(52).f;

	// B.2.2.5 Object.prototype.__lookupSetter__(P)
	__webpack_require__(6) && $export($export.P + __webpack_require__(277), 'Object', {
	  __lookupSetter__: function __lookupSetter__(P) {
	    var O = toObject(this);
	    var K = toPrimitive(P, true);
	    var D;
	    do {
	      if (D = getOwnPropertyDescriptor(O, K)) return D.set;
	    } while (O = getPrototypeOf(O));
	  }
	});


/***/ }),
/* 281 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/DavidBruant/Map-Set.prototype.toJSON
	var $export = __webpack_require__(8);

	$export($export.P + $export.R, 'Map', { toJSON: __webpack_require__(282)('Map') });


/***/ }),
/* 282 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/DavidBruant/Map-Set.prototype.toJSON
	var classof = __webpack_require__(75);
	var from = __webpack_require__(283);
	module.exports = function (NAME) {
	  return function toJSON() {
	    if (classof(this) != NAME) throw TypeError(NAME + "#toJSON isn't generic");
	    return from(this);
	  };
	};


/***/ }),
/* 283 */
/***/ (function(module, exports, __webpack_require__) {

	var forOf = __webpack_require__(213);

	module.exports = function (iter, ITERATOR) {
	  var result = [];
	  forOf(iter, false, result.push, result, ITERATOR);
	  return result;
	};


/***/ }),
/* 284 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/DavidBruant/Map-Set.prototype.toJSON
	var $export = __webpack_require__(8);

	$export($export.P + $export.R, 'Set', { toJSON: __webpack_require__(282)('Set') });


/***/ }),
/* 285 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-map.of
	__webpack_require__(286)('Map');


/***/ }),
/* 286 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/proposal-setmap-offrom/
	var $export = __webpack_require__(8);

	module.exports = function (COLLECTION) {
	  $export($export.S, COLLECTION, { of: function of() {
	    var length = arguments.length;
	    var A = new Array(length);
	    while (length--) A[length] = arguments[length];
	    return new this(A);
	  } });
	};


/***/ }),
/* 287 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-set.of
	__webpack_require__(286)('Set');


/***/ }),
/* 288 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-weakmap.of
	__webpack_require__(286)('WeakMap');


/***/ }),
/* 289 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-weakset.of
	__webpack_require__(286)('WeakSet');


/***/ }),
/* 290 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-map.from
	__webpack_require__(291)('Map');


/***/ }),
/* 291 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://tc39.github.io/proposal-setmap-offrom/
	var $export = __webpack_require__(8);
	var aFunction = __webpack_require__(24);
	var ctx = __webpack_require__(23);
	var forOf = __webpack_require__(213);

	module.exports = function (COLLECTION) {
	  $export($export.S, COLLECTION, { from: function from(source /* , mapFn, thisArg */) {
	    var mapFn = arguments[1];
	    var mapping, A, n, cb;
	    aFunction(this);
	    mapping = mapFn !== undefined;
	    if (mapping) aFunction(mapFn);
	    if (source == undefined) return new this();
	    A = [];
	    if (mapping) {
	      n = 0;
	      cb = ctx(mapFn, arguments[2], 2);
	      forOf(source, false, function (nextItem) {
	        A.push(cb(nextItem, n++));
	      });
	    } else {
	      forOf(source, false, A.push, A);
	    }
	    return new this(A);
	  } });
	};


/***/ }),
/* 292 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-set.from
	__webpack_require__(291)('Set');


/***/ }),
/* 293 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-weakmap.from
	__webpack_require__(291)('WeakMap');


/***/ }),
/* 294 */
/***/ (function(module, exports, __webpack_require__) {

	// https://tc39.github.io/proposal-setmap-offrom/#sec-weakset.from
	__webpack_require__(291)('WeakSet');


/***/ }),
/* 295 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-global
	var $export = __webpack_require__(8);

	$export($export.G, { global: __webpack_require__(4) });


/***/ }),
/* 296 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-global
	var $export = __webpack_require__(8);

	$export($export.S, 'System', { global: __webpack_require__(4) });


/***/ }),
/* 297 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/ljharb/proposal-is-error
	var $export = __webpack_require__(8);
	var cof = __webpack_require__(35);

	$export($export.S, 'Error', {
	  isError: function isError(it) {
	    return cof(it) === 'Error';
	  }
	});


/***/ }),
/* 298 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  clamp: function clamp(x, lower, upper) {
	    return Math.min(upper, Math.max(lower, x));
	  }
	});


/***/ }),
/* 299 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { DEG_PER_RAD: Math.PI / 180 });


/***/ }),
/* 300 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);
	var RAD_PER_DEG = 180 / Math.PI;

	$export($export.S, 'Math', {
	  degrees: function degrees(radians) {
	    return radians * RAD_PER_DEG;
	  }
	});


/***/ }),
/* 301 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);
	var scale = __webpack_require__(302);
	var fround = __webpack_require__(114);

	$export($export.S, 'Math', {
	  fscale: function fscale(x, inLow, inHigh, outLow, outHigh) {
	    return fround(scale(x, inLow, inHigh, outLow, outHigh));
	  }
	});


/***/ }),
/* 302 */
/***/ (function(module, exports) {

	// https://rwaldron.github.io/proposal-math-extensions/
	module.exports = Math.scale || function scale(x, inLow, inHigh, outLow, outHigh) {
	  if (
	    arguments.length === 0
	      // eslint-disable-next-line no-self-compare
	      || x != x
	      // eslint-disable-next-line no-self-compare
	      || inLow != inLow
	      // eslint-disable-next-line no-self-compare
	      || inHigh != inHigh
	      // eslint-disable-next-line no-self-compare
	      || outLow != outLow
	      // eslint-disable-next-line no-self-compare
	      || outHigh != outHigh
	  ) return NaN;
	  if (x === Infinity || x === -Infinity) return x;
	  return (x - inLow) * (outHigh - outLow) / (inHigh - inLow) + outLow;
	};


/***/ }),
/* 303 */
/***/ (function(module, exports, __webpack_require__) {

	// https://gist.github.com/BrendanEich/4294d5c212a6d2254703
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  iaddh: function iaddh(x0, x1, y0, y1) {
	    var $x0 = x0 >>> 0;
	    var $x1 = x1 >>> 0;
	    var $y0 = y0 >>> 0;
	    return $x1 + (y1 >>> 0) + (($x0 & $y0 | ($x0 | $y0) & ~($x0 + $y0 >>> 0)) >>> 31) | 0;
	  }
	});


/***/ }),
/* 304 */
/***/ (function(module, exports, __webpack_require__) {

	// https://gist.github.com/BrendanEich/4294d5c212a6d2254703
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  isubh: function isubh(x0, x1, y0, y1) {
	    var $x0 = x0 >>> 0;
	    var $x1 = x1 >>> 0;
	    var $y0 = y0 >>> 0;
	    return $x1 - (y1 >>> 0) - ((~$x0 & $y0 | ~($x0 ^ $y0) & $x0 - $y0 >>> 0) >>> 31) | 0;
	  }
	});


/***/ }),
/* 305 */
/***/ (function(module, exports, __webpack_require__) {

	// https://gist.github.com/BrendanEich/4294d5c212a6d2254703
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  imulh: function imulh(u, v) {
	    var UINT16 = 0xffff;
	    var $u = +u;
	    var $v = +v;
	    var u0 = $u & UINT16;
	    var v0 = $v & UINT16;
	    var u1 = $u >> 16;
	    var v1 = $v >> 16;
	    var t = (u1 * v0 >>> 0) + (u0 * v0 >>> 16);
	    return u1 * v1 + (t >> 16) + ((u0 * v1 >>> 0) + (t & UINT16) >> 16);
	  }
	});


/***/ }),
/* 306 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { RAD_PER_DEG: 180 / Math.PI });


/***/ }),
/* 307 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);
	var DEG_PER_RAD = Math.PI / 180;

	$export($export.S, 'Math', {
	  radians: function radians(degrees) {
	    return degrees * DEG_PER_RAD;
	  }
	});


/***/ }),
/* 308 */
/***/ (function(module, exports, __webpack_require__) {

	// https://rwaldron.github.io/proposal-math-extensions/
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { scale: __webpack_require__(302) });


/***/ }),
/* 309 */
/***/ (function(module, exports, __webpack_require__) {

	// https://gist.github.com/BrendanEich/4294d5c212a6d2254703
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', {
	  umulh: function umulh(u, v) {
	    var UINT16 = 0xffff;
	    var $u = +u;
	    var $v = +v;
	    var u0 = $u & UINT16;
	    var v0 = $v & UINT16;
	    var u1 = $u >>> 16;
	    var v1 = $v >>> 16;
	    var t = (u1 * v0 >>> 0) + (u0 * v0 >>> 16);
	    return u1 * v1 + (t >>> 16) + ((u0 * v1 >>> 0) + (t & UINT16) >>> 16);
	  }
	});


/***/ }),
/* 310 */
/***/ (function(module, exports, __webpack_require__) {

	// http://jfbastien.github.io/papers/Math.signbit.html
	var $export = __webpack_require__(8);

	$export($export.S, 'Math', { signbit: function signbit(x) {
	  // eslint-disable-next-line no-self-compare
	  return (x = +x) != x ? x : x == 0 ? 1 / x == Infinity : x > 0;
	} });


/***/ }),
/* 311 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/tc39/proposal-promise-finally
	'use strict';
	var $export = __webpack_require__(8);
	var core = __webpack_require__(9);
	var global = __webpack_require__(4);
	var speciesConstructor = __webpack_require__(210);
	var promiseResolve = __webpack_require__(219);

	$export($export.P + $export.R, 'Promise', { 'finally': function (onFinally) {
	  var C = speciesConstructor(this, core.Promise || global.Promise);
	  var isFunction = typeof onFinally == 'function';
	  return this.then(
	    isFunction ? function (x) {
	      return promiseResolve(C, onFinally()).then(function () { return x; });
	    } : onFinally,
	    isFunction ? function (e) {
	      return promiseResolve(C, onFinally()).then(function () { throw e; });
	    } : onFinally
	  );
	} });


/***/ }),
/* 312 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/tc39/proposal-promise-try
	var $export = __webpack_require__(8);
	var newPromiseCapability = __webpack_require__(216);
	var perform = __webpack_require__(217);

	$export($export.S, 'Promise', { 'try': function (callbackfn) {
	  var promiseCapability = newPromiseCapability.f(this);
	  var result = perform(callbackfn);
	  (result.e ? promiseCapability.reject : promiseCapability.resolve)(result.v);
	  return promiseCapability.promise;
	} });


/***/ }),
/* 313 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var toMetaKey = metadata.key;
	var ordinaryDefineOwnMetadata = metadata.set;

	metadata.exp({ defineMetadata: function defineMetadata(metadataKey, metadataValue, target, targetKey) {
	  ordinaryDefineOwnMetadata(metadataKey, metadataValue, anObject(target), toMetaKey(targetKey));
	} });


/***/ }),
/* 314 */
/***/ (function(module, exports, __webpack_require__) {

	var Map = __webpack_require__(221);
	var $export = __webpack_require__(8);
	var shared = __webpack_require__(21)('metadata');
	var store = shared.store || (shared.store = new (__webpack_require__(226))());

	var getOrCreateMetadataMap = function (target, targetKey, create) {
	  var targetMetadata = store.get(target);
	  if (!targetMetadata) {
	    if (!create) return undefined;
	    store.set(target, targetMetadata = new Map());
	  }
	  var keyMetadata = targetMetadata.get(targetKey);
	  if (!keyMetadata) {
	    if (!create) return undefined;
	    targetMetadata.set(targetKey, keyMetadata = new Map());
	  } return keyMetadata;
	};
	var ordinaryHasOwnMetadata = function (MetadataKey, O, P) {
	  var metadataMap = getOrCreateMetadataMap(O, P, false);
	  return metadataMap === undefined ? false : metadataMap.has(MetadataKey);
	};
	var ordinaryGetOwnMetadata = function (MetadataKey, O, P) {
	  var metadataMap = getOrCreateMetadataMap(O, P, false);
	  return metadataMap === undefined ? undefined : metadataMap.get(MetadataKey);
	};
	var ordinaryDefineOwnMetadata = function (MetadataKey, MetadataValue, O, P) {
	  getOrCreateMetadataMap(O, P, true).set(MetadataKey, MetadataValue);
	};
	var ordinaryOwnMetadataKeys = function (target, targetKey) {
	  var metadataMap = getOrCreateMetadataMap(target, targetKey, false);
	  var keys = [];
	  if (metadataMap) metadataMap.forEach(function (_, key) { keys.push(key); });
	  return keys;
	};
	var toMetaKey = function (it) {
	  return it === undefined || typeof it == 'symbol' ? it : String(it);
	};
	var exp = function (O) {
	  $export($export.S, 'Reflect', O);
	};

	module.exports = {
	  store: store,
	  map: getOrCreateMetadataMap,
	  has: ordinaryHasOwnMetadata,
	  get: ordinaryGetOwnMetadata,
	  set: ordinaryDefineOwnMetadata,
	  keys: ordinaryOwnMetadataKeys,
	  key: toMetaKey,
	  exp: exp
	};


/***/ }),
/* 315 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var toMetaKey = metadata.key;
	var getOrCreateMetadataMap = metadata.map;
	var store = metadata.store;

	metadata.exp({ deleteMetadata: function deleteMetadata(metadataKey, target /* , targetKey */) {
	  var targetKey = arguments.length < 3 ? undefined : toMetaKey(arguments[2]);
	  var metadataMap = getOrCreateMetadataMap(anObject(target), targetKey, false);
	  if (metadataMap === undefined || !metadataMap['delete'](metadataKey)) return false;
	  if (metadataMap.size) return true;
	  var targetMetadata = store.get(target);
	  targetMetadata['delete'](targetKey);
	  return !!targetMetadata.size || store['delete'](target);
	} });


/***/ }),
/* 316 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var getPrototypeOf = __webpack_require__(59);
	var ordinaryHasOwnMetadata = metadata.has;
	var ordinaryGetOwnMetadata = metadata.get;
	var toMetaKey = metadata.key;

	var ordinaryGetMetadata = function (MetadataKey, O, P) {
	  var hasOwn = ordinaryHasOwnMetadata(MetadataKey, O, P);
	  if (hasOwn) return ordinaryGetOwnMetadata(MetadataKey, O, P);
	  var parent = getPrototypeOf(O);
	  return parent !== null ? ordinaryGetMetadata(MetadataKey, parent, P) : undefined;
	};

	metadata.exp({ getMetadata: function getMetadata(metadataKey, target /* , targetKey */) {
	  return ordinaryGetMetadata(metadataKey, anObject(target), arguments.length < 3 ? undefined : toMetaKey(arguments[2]));
	} });


/***/ }),
/* 317 */
/***/ (function(module, exports, __webpack_require__) {

	var Set = __webpack_require__(225);
	var from = __webpack_require__(283);
	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var getPrototypeOf = __webpack_require__(59);
	var ordinaryOwnMetadataKeys = metadata.keys;
	var toMetaKey = metadata.key;

	var ordinaryMetadataKeys = function (O, P) {
	  var oKeys = ordinaryOwnMetadataKeys(O, P);
	  var parent = getPrototypeOf(O);
	  if (parent === null) return oKeys;
	  var pKeys = ordinaryMetadataKeys(parent, P);
	  return pKeys.length ? oKeys.length ? from(new Set(oKeys.concat(pKeys))) : pKeys : oKeys;
	};

	metadata.exp({ getMetadataKeys: function getMetadataKeys(target /* , targetKey */) {
	  return ordinaryMetadataKeys(anObject(target), arguments.length < 2 ? undefined : toMetaKey(arguments[1]));
	} });


/***/ }),
/* 318 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var ordinaryGetOwnMetadata = metadata.get;
	var toMetaKey = metadata.key;

	metadata.exp({ getOwnMetadata: function getOwnMetadata(metadataKey, target /* , targetKey */) {
	  return ordinaryGetOwnMetadata(metadataKey, anObject(target)
	    , arguments.length < 3 ? undefined : toMetaKey(arguments[2]));
	} });


/***/ }),
/* 319 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var ordinaryOwnMetadataKeys = metadata.keys;
	var toMetaKey = metadata.key;

	metadata.exp({ getOwnMetadataKeys: function getOwnMetadataKeys(target /* , targetKey */) {
	  return ordinaryOwnMetadataKeys(anObject(target), arguments.length < 2 ? undefined : toMetaKey(arguments[1]));
	} });


/***/ }),
/* 320 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var getPrototypeOf = __webpack_require__(59);
	var ordinaryHasOwnMetadata = metadata.has;
	var toMetaKey = metadata.key;

	var ordinaryHasMetadata = function (MetadataKey, O, P) {
	  var hasOwn = ordinaryHasOwnMetadata(MetadataKey, O, P);
	  if (hasOwn) return true;
	  var parent = getPrototypeOf(O);
	  return parent !== null ? ordinaryHasMetadata(MetadataKey, parent, P) : false;
	};

	metadata.exp({ hasMetadata: function hasMetadata(metadataKey, target /* , targetKey */) {
	  return ordinaryHasMetadata(metadataKey, anObject(target), arguments.length < 3 ? undefined : toMetaKey(arguments[2]));
	} });


/***/ }),
/* 321 */
/***/ (function(module, exports, __webpack_require__) {

	var metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var ordinaryHasOwnMetadata = metadata.has;
	var toMetaKey = metadata.key;

	metadata.exp({ hasOwnMetadata: function hasOwnMetadata(metadataKey, target /* , targetKey */) {
	  return ordinaryHasOwnMetadata(metadataKey, anObject(target)
	    , arguments.length < 3 ? undefined : toMetaKey(arguments[2]));
	} });


/***/ }),
/* 322 */
/***/ (function(module, exports, __webpack_require__) {

	var $metadata = __webpack_require__(314);
	var anObject = __webpack_require__(12);
	var aFunction = __webpack_require__(24);
	var toMetaKey = $metadata.key;
	var ordinaryDefineOwnMetadata = $metadata.set;

	$metadata.exp({ metadata: function metadata(metadataKey, metadataValue) {
	  return function decorator(target, targetKey) {
	    ordinaryDefineOwnMetadata(
	      metadataKey, metadataValue,
	      (targetKey !== undefined ? anObject : aFunction)(target),
	      toMetaKey(targetKey)
	    );
	  };
	} });


/***/ }),
/* 323 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/rwaldron/tc39-notes/blob/master/es6/2014-09/sept-25.md#510-globalasap-for-enqueuing-a-microtask
	var $export = __webpack_require__(8);
	var microtask = __webpack_require__(215)();
	var process = __webpack_require__(4).process;
	var isNode = __webpack_require__(35)(process) == 'process';

	$export($export.G, {
	  asap: function asap(fn) {
	    var domain = isNode && process.domain;
	    microtask(domain ? domain.bind(fn) : fn);
	  }
	});


/***/ }),
/* 324 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';
	// https://github.com/zenparsing/es-observable
	var $export = __webpack_require__(8);
	var global = __webpack_require__(4);
	var core = __webpack_require__(9);
	var microtask = __webpack_require__(215)();
	var OBSERVABLE = __webpack_require__(27)('observable');
	var aFunction = __webpack_require__(24);
	var anObject = __webpack_require__(12);
	var anInstance = __webpack_require__(212);
	var redefineAll = __webpack_require__(220);
	var hide = __webpack_require__(10);
	var forOf = __webpack_require__(213);
	var RETURN = forOf.RETURN;

	var getMethod = function (fn) {
	  return fn == null ? undefined : aFunction(fn);
	};

	var cleanupSubscription = function (subscription) {
	  var cleanup = subscription._c;
	  if (cleanup) {
	    subscription._c = undefined;
	    cleanup();
	  }
	};

	var subscriptionClosed = function (subscription) {
	  return subscription._o === undefined;
	};

	var closeSubscription = function (subscription) {
	  if (!subscriptionClosed(subscription)) {
	    subscription._o = undefined;
	    cleanupSubscription(subscription);
	  }
	};

	var Subscription = function (observer, subscriber) {
	  anObject(observer);
	  this._c = undefined;
	  this._o = observer;
	  observer = new SubscriptionObserver(this);
	  try {
	    var cleanup = subscriber(observer);
	    var subscription = cleanup;
	    if (cleanup != null) {
	      if (typeof cleanup.unsubscribe === 'function') cleanup = function () { subscription.unsubscribe(); };
	      else aFunction(cleanup);
	      this._c = cleanup;
	    }
	  } catch (e) {
	    observer.error(e);
	    return;
	  } if (subscriptionClosed(this)) cleanupSubscription(this);
	};

	Subscription.prototype = redefineAll({}, {
	  unsubscribe: function unsubscribe() { closeSubscription(this); }
	});

	var SubscriptionObserver = function (subscription) {
	  this._s = subscription;
	};

	SubscriptionObserver.prototype = redefineAll({}, {
	  next: function next(value) {
	    var subscription = this._s;
	    if (!subscriptionClosed(subscription)) {
	      var observer = subscription._o;
	      try {
	        var m = getMethod(observer.next);
	        if (m) return m.call(observer, value);
	      } catch (e) {
	        try {
	          closeSubscription(subscription);
	        } finally {
	          throw e;
	        }
	      }
	    }
	  },
	  error: function error(value) {
	    var subscription = this._s;
	    if (subscriptionClosed(subscription)) throw value;
	    var observer = subscription._o;
	    subscription._o = undefined;
	    try {
	      var m = getMethod(observer.error);
	      if (!m) throw value;
	      value = m.call(observer, value);
	    } catch (e) {
	      try {
	        cleanupSubscription(subscription);
	      } finally {
	        throw e;
	      }
	    } cleanupSubscription(subscription);
	    return value;
	  },
	  complete: function complete(value) {
	    var subscription = this._s;
	    if (!subscriptionClosed(subscription)) {
	      var observer = subscription._o;
	      subscription._o = undefined;
	      try {
	        var m = getMethod(observer.complete);
	        value = m ? m.call(observer, value) : undefined;
	      } catch (e) {
	        try {
	          cleanupSubscription(subscription);
	        } finally {
	          throw e;
	        }
	      } cleanupSubscription(subscription);
	      return value;
	    }
	  }
	});

	var $Observable = function Observable(subscriber) {
	  anInstance(this, $Observable, 'Observable', '_f')._f = aFunction(subscriber);
	};

	redefineAll($Observable.prototype, {
	  subscribe: function subscribe(observer) {
	    return new Subscription(observer, this._f);
	  },
	  forEach: function forEach(fn) {
	    var that = this;
	    return new (core.Promise || global.Promise)(function (resolve, reject) {
	      aFunction(fn);
	      var subscription = that.subscribe({
	        next: function (value) {
	          try {
	            return fn(value);
	          } catch (e) {
	            reject(e);
	            subscription.unsubscribe();
	          }
	        },
	        error: reject,
	        complete: resolve
	      });
	    });
	  }
	});

	redefineAll($Observable, {
	  from: function from(x) {
	    var C = typeof this === 'function' ? this : $Observable;
	    var method = getMethod(anObject(x)[OBSERVABLE]);
	    if (method) {
	      var observable = anObject(method.call(x));
	      return observable.constructor === C ? observable : new C(function (observer) {
	        return observable.subscribe(observer);
	      });
	    }
	    return new C(function (observer) {
	      var done = false;
	      microtask(function () {
	        if (!done) {
	          try {
	            if (forOf(x, false, function (it) {
	              observer.next(it);
	              if (done) return RETURN;
	            }) === RETURN) return;
	          } catch (e) {
	            if (done) throw e;
	            observer.error(e);
	            return;
	          } observer.complete();
	        }
	      });
	      return function () { done = true; };
	    });
	  },
	  of: function of() {
	    for (var i = 0, l = arguments.length, items = new Array(l); i < l;) items[i] = arguments[i++];
	    return new (typeof this === 'function' ? this : $Observable)(function (observer) {
	      var done = false;
	      microtask(function () {
	        if (!done) {
	          for (var j = 0; j < items.length; ++j) {
	            observer.next(items[j]);
	            if (done) return;
	          } observer.complete();
	        }
	      });
	      return function () { done = true; };
	    });
	  }
	});

	hide($Observable.prototype, OBSERVABLE, function () { return this; });

	$export($export.G, { Observable: $Observable });

	__webpack_require__(194)('Observable');


/***/ }),
/* 325 */
/***/ (function(module, exports, __webpack_require__) {

	// ie9- setTimeout & setInterval additional parameters fix
	var global = __webpack_require__(4);
	var $export = __webpack_require__(8);
	var userAgent = __webpack_require__(218);
	var slice = [].slice;
	var MSIE = /MSIE .\./.test(userAgent); // <- dirty ie9- check
	var wrap = function (set) {
	  return function (fn, time /* , ...args */) {
	    var boundArgs = arguments.length > 2;
	    var args = boundArgs ? slice.call(arguments, 2) : false;
	    return set(boundArgs ? function () {
	      // eslint-disable-next-line no-new-func
	      (typeof fn == 'function' ? fn : Function(fn)).apply(this, args);
	    } : fn, time);
	  };
	};
	$export($export.G + $export.B + $export.F * MSIE, {
	  setTimeout: wrap(global.setTimeout),
	  setInterval: wrap(global.setInterval)
	});


/***/ }),
/* 326 */
/***/ (function(module, exports, __webpack_require__) {

	var $export = __webpack_require__(8);
	var $task = __webpack_require__(214);
	$export($export.G + $export.B, {
	  setImmediate: $task.set,
	  clearImmediate: $task.clear
	});


/***/ }),
/* 327 */
/***/ (function(module, exports, __webpack_require__) {

	var $iterators = __webpack_require__(195);
	var getKeys = __webpack_require__(31);
	var redefine = __webpack_require__(18);
	var global = __webpack_require__(4);
	var hide = __webpack_require__(10);
	var Iterators = __webpack_require__(130);
	var wks = __webpack_require__(27);
	var ITERATOR = wks('iterator');
	var TO_STRING_TAG = wks('toStringTag');
	var ArrayValues = Iterators.Array;

	var DOMIterables = {
	  CSSRuleList: true, // TODO: Not spec compliant, should be false.
	  CSSStyleDeclaration: false,
	  CSSValueList: false,
	  ClientRectList: false,
	  DOMRectList: false,
	  DOMStringList: false,
	  DOMTokenList: true,
	  DataTransferItemList: false,
	  FileList: false,
	  HTMLAllCollection: false,
	  HTMLCollection: false,
	  HTMLFormElement: false,
	  HTMLSelectElement: false,
	  MediaList: true, // TODO: Not spec compliant, should be false.
	  MimeTypeArray: false,
	  NamedNodeMap: false,
	  NodeList: true,
	  PaintRequestList: false,
	  Plugin: false,
	  PluginArray: false,
	  SVGLengthList: false,
	  SVGNumberList: false,
	  SVGPathSegList: false,
	  SVGPointList: false,
	  SVGStringList: false,
	  SVGTransformList: false,
	  SourceBufferList: false,
	  StyleSheetList: true, // TODO: Not spec compliant, should be false.
	  TextTrackCueList: false,
	  TextTrackList: false,
	  TouchList: false
	};

	for (var collections = getKeys(DOMIterables), i = 0; i < collections.length; i++) {
	  var NAME = collections[i];
	  var explicit = DOMIterables[NAME];
	  var Collection = global[NAME];
	  var proto = Collection && Collection.prototype;
	  var key;
	  if (proto) {
	    if (!proto[ITERATOR]) hide(proto, ITERATOR, ArrayValues);
	    if (!proto[TO_STRING_TAG]) hide(proto, TO_STRING_TAG, NAME);
	    Iterators[NAME] = ArrayValues;
	    if (explicit) for (key in $iterators) if (!proto[key]) redefine(proto, key, $iterators[key], true);
	  }
	}


/***/ }),
/* 328 */
/***/ (function(module, exports) {

	/* WEBPACK VAR INJECTION */(function(global) {/**
	 * Copyright (c) 2014, Facebook, Inc.
	 * All rights reserved.
	 *
	 * This source code is licensed under the BSD-style license found in the
	 * https://raw.github.com/facebook/regenerator/master/LICENSE file. An
	 * additional grant of patent rights can be found in the PATENTS file in
	 * the same directory.
	 */

	!(function(global) {
	  "use strict";

	  var Op = Object.prototype;
	  var hasOwn = Op.hasOwnProperty;
	  var undefined; // More compressible than void 0.
	  var $Symbol = typeof Symbol === "function" ? Symbol : {};
	  var iteratorSymbol = $Symbol.iterator || "@@iterator";
	  var asyncIteratorSymbol = $Symbol.asyncIterator || "@@asyncIterator";
	  var toStringTagSymbol = $Symbol.toStringTag || "@@toStringTag";

	  var inModule = typeof module === "object";
	  var runtime = global.regeneratorRuntime;
	  if (runtime) {
	    if (inModule) {
	      // If regeneratorRuntime is defined globally and we're in a module,
	      // make the exports object identical to regeneratorRuntime.
	      module.exports = runtime;
	    }
	    // Don't bother evaluating the rest of this file if the runtime was
	    // already defined globally.
	    return;
	  }

	  // Define the runtime globally (as expected by generated code) as either
	  // module.exports (if we're in a module) or a new, empty object.
	  runtime = global.regeneratorRuntime = inModule ? module.exports : {};

	  function wrap(innerFn, outerFn, self, tryLocsList) {
	    // If outerFn provided and outerFn.prototype is a Generator, then outerFn.prototype instanceof Generator.
	    var protoGenerator = outerFn && outerFn.prototype instanceof Generator ? outerFn : Generator;
	    var generator = Object.create(protoGenerator.prototype);
	    var context = new Context(tryLocsList || []);

	    // The ._invoke method unifies the implementations of the .next,
	    // .throw, and .return methods.
	    generator._invoke = makeInvokeMethod(innerFn, self, context);

	    return generator;
	  }
	  runtime.wrap = wrap;

	  // Try/catch helper to minimize deoptimizations. Returns a completion
	  // record like context.tryEntries[i].completion. This interface could
	  // have been (and was previously) designed to take a closure to be
	  // invoked without arguments, but in all the cases we care about we
	  // already have an existing method we want to call, so there's no need
	  // to create a new function object. We can even get away with assuming
	  // the method takes exactly one argument, since that happens to be true
	  // in every case, so we don't have to touch the arguments object. The
	  // only additional allocation required is the completion record, which
	  // has a stable shape and so hopefully should be cheap to allocate.
	  function tryCatch(fn, obj, arg) {
	    try {
	      return { type: "normal", arg: fn.call(obj, arg) };
	    } catch (err) {
	      return { type: "throw", arg: err };
	    }
	  }

	  var GenStateSuspendedStart = "suspendedStart";
	  var GenStateSuspendedYield = "suspendedYield";
	  var GenStateExecuting = "executing";
	  var GenStateCompleted = "completed";

	  // Returning this object from the innerFn has the same effect as
	  // breaking out of the dispatch switch statement.
	  var ContinueSentinel = {};

	  // Dummy constructor functions that we use as the .constructor and
	  // .constructor.prototype properties for functions that return Generator
	  // objects. For full spec compliance, you may wish to configure your
	  // minifier not to mangle the names of these two functions.
	  function Generator() {}
	  function GeneratorFunction() {}
	  function GeneratorFunctionPrototype() {}

	  // This is a polyfill for %IteratorPrototype% for environments that
	  // don't natively support it.
	  var IteratorPrototype = {};
	  IteratorPrototype[iteratorSymbol] = function () {
	    return this;
	  };

	  var getProto = Object.getPrototypeOf;
	  var NativeIteratorPrototype = getProto && getProto(getProto(values([])));
	  if (NativeIteratorPrototype &&
	      NativeIteratorPrototype !== Op &&
	      hasOwn.call(NativeIteratorPrototype, iteratorSymbol)) {
	    // This environment has a native %IteratorPrototype%; use it instead
	    // of the polyfill.
	    IteratorPrototype = NativeIteratorPrototype;
	  }

	  var Gp = GeneratorFunctionPrototype.prototype =
	    Generator.prototype = Object.create(IteratorPrototype);
	  GeneratorFunction.prototype = Gp.constructor = GeneratorFunctionPrototype;
	  GeneratorFunctionPrototype.constructor = GeneratorFunction;
	  GeneratorFunctionPrototype[toStringTagSymbol] =
	    GeneratorFunction.displayName = "GeneratorFunction";

	  // Helper for defining the .next, .throw, and .return methods of the
	  // Iterator interface in terms of a single ._invoke method.
	  function defineIteratorMethods(prototype) {
	    ["next", "throw", "return"].forEach(function(method) {
	      prototype[method] = function(arg) {
	        return this._invoke(method, arg);
	      };
	    });
	  }

	  runtime.isGeneratorFunction = function(genFun) {
	    var ctor = typeof genFun === "function" && genFun.constructor;
	    return ctor
	      ? ctor === GeneratorFunction ||
	        // For the native GeneratorFunction constructor, the best we can
	        // do is to check its .name property.
	        (ctor.displayName || ctor.name) === "GeneratorFunction"
	      : false;
	  };

	  runtime.mark = function(genFun) {
	    if (Object.setPrototypeOf) {
	      Object.setPrototypeOf(genFun, GeneratorFunctionPrototype);
	    } else {
	      genFun.__proto__ = GeneratorFunctionPrototype;
	      if (!(toStringTagSymbol in genFun)) {
	        genFun[toStringTagSymbol] = "GeneratorFunction";
	      }
	    }
	    genFun.prototype = Object.create(Gp);
	    return genFun;
	  };

	  // Within the body of any async function, `await x` is transformed to
	  // `yield regeneratorRuntime.awrap(x)`, so that the runtime can test
	  // `hasOwn.call(value, "__await")` to determine if the yielded value is
	  // meant to be awaited.
	  runtime.awrap = function(arg) {
	    return { __await: arg };
	  };

	  function AsyncIterator(generator) {
	    function invoke(method, arg, resolve, reject) {
	      var record = tryCatch(generator[method], generator, arg);
	      if (record.type === "throw") {
	        reject(record.arg);
	      } else {
	        var result = record.arg;
	        var value = result.value;
	        if (value &&
	            typeof value === "object" &&
	            hasOwn.call(value, "__await")) {
	          return Promise.resolve(value.__await).then(function(value) {
	            invoke("next", value, resolve, reject);
	          }, function(err) {
	            invoke("throw", err, resolve, reject);
	          });
	        }

	        return Promise.resolve(value).then(function(unwrapped) {
	          // When a yielded Promise is resolved, its final value becomes
	          // the .value of the Promise<{value,done}> result for the
	          // current iteration. If the Promise is rejected, however, the
	          // result for this iteration will be rejected with the same
	          // reason. Note that rejections of yielded Promises are not
	          // thrown back into the generator function, as is the case
	          // when an awaited Promise is rejected. This difference in
	          // behavior between yield and await is important, because it
	          // allows the consumer to decide what to do with the yielded
	          // rejection (swallow it and continue, manually .throw it back
	          // into the generator, abandon iteration, whatever). With
	          // await, by contrast, there is no opportunity to examine the
	          // rejection reason outside the generator function, so the
	          // only option is to throw it from the await expression, and
	          // let the generator function handle the exception.
	          result.value = unwrapped;
	          resolve(result);
	        }, reject);
	      }
	    }

	    if (typeof global.process === "object" && global.process.domain) {
	      invoke = global.process.domain.bind(invoke);
	    }

	    var previousPromise;

	    function enqueue(method, arg) {
	      function callInvokeWithMethodAndArg() {
	        return new Promise(function(resolve, reject) {
	          invoke(method, arg, resolve, reject);
	        });
	      }

	      return previousPromise =
	        // If enqueue has been called before, then we want to wait until
	        // all previous Promises have been resolved before calling invoke,
	        // so that results are always delivered in the correct order. If
	        // enqueue has not been called before, then it is important to
	        // call invoke immediately, without waiting on a callback to fire,
	        // so that the async generator function has the opportunity to do
	        // any necessary setup in a predictable way. This predictability
	        // is why the Promise constructor synchronously invokes its
	        // executor callback, and why async functions synchronously
	        // execute code before the first await. Since we implement simple
	        // async functions in terms of async generators, it is especially
	        // important to get this right, even though it requires care.
	        previousPromise ? previousPromise.then(
	          callInvokeWithMethodAndArg,
	          // Avoid propagating failures to Promises returned by later
	          // invocations of the iterator.
	          callInvokeWithMethodAndArg
	        ) : callInvokeWithMethodAndArg();
	    }

	    // Define the unified helper method that is used to implement .next,
	    // .throw, and .return (see defineIteratorMethods).
	    this._invoke = enqueue;
	  }

	  defineIteratorMethods(AsyncIterator.prototype);
	  AsyncIterator.prototype[asyncIteratorSymbol] = function () {
	    return this;
	  };
	  runtime.AsyncIterator = AsyncIterator;

	  // Note that simple async functions are implemented on top of
	  // AsyncIterator objects; they just return a Promise for the value of
	  // the final result produced by the iterator.
	  runtime.async = function(innerFn, outerFn, self, tryLocsList) {
	    var iter = new AsyncIterator(
	      wrap(innerFn, outerFn, self, tryLocsList)
	    );

	    return runtime.isGeneratorFunction(outerFn)
	      ? iter // If outerFn is a generator, return the full iterator.
	      : iter.next().then(function(result) {
	          return result.done ? result.value : iter.next();
	        });
	  };

	  function makeInvokeMethod(innerFn, self, context) {
	    var state = GenStateSuspendedStart;

	    return function invoke(method, arg) {
	      if (state === GenStateExecuting) {
	        throw new Error("Generator is already running");
	      }

	      if (state === GenStateCompleted) {
	        if (method === "throw") {
	          throw arg;
	        }

	        // Be forgiving, per 25.3.3.3.3 of the spec:
	        // https://people.mozilla.org/~jorendorff/es6-draft.html#sec-generatorresume
	        return doneResult();
	      }

	      context.method = method;
	      context.arg = arg;

	      while (true) {
	        var delegate = context.delegate;
	        if (delegate) {
	          var delegateResult = maybeInvokeDelegate(delegate, context);
	          if (delegateResult) {
	            if (delegateResult === ContinueSentinel) continue;
	            return delegateResult;
	          }
	        }

	        if (context.method === "next") {
	          // Setting context._sent for legacy support of Babel's
	          // function.sent implementation.
	          context.sent = context._sent = context.arg;

	        } else if (context.method === "throw") {
	          if (state === GenStateSuspendedStart) {
	            state = GenStateCompleted;
	            throw context.arg;
	          }

	          context.dispatchException(context.arg);

	        } else if (context.method === "return") {
	          context.abrupt("return", context.arg);
	        }

	        state = GenStateExecuting;

	        var record = tryCatch(innerFn, self, context);
	        if (record.type === "normal") {
	          // If an exception is thrown from innerFn, we leave state ===
	          // GenStateExecuting and loop back for another invocation.
	          state = context.done
	            ? GenStateCompleted
	            : GenStateSuspendedYield;

	          if (record.arg === ContinueSentinel) {
	            continue;
	          }

	          return {
	            value: record.arg,
	            done: context.done
	          };

	        } else if (record.type === "throw") {
	          state = GenStateCompleted;
	          // Dispatch the exception by looping back around to the
	          // context.dispatchException(context.arg) call above.
	          context.method = "throw";
	          context.arg = record.arg;
	        }
	      }
	    };
	  }

	  // Call delegate.iterator[context.method](context.arg) and handle the
	  // result, either by returning a { value, done } result from the
	  // delegate iterator, or by modifying context.method and context.arg,
	  // setting context.delegate to null, and returning the ContinueSentinel.
	  function maybeInvokeDelegate(delegate, context) {
	    var method = delegate.iterator[context.method];
	    if (method === undefined) {
	      // A .throw or .return when the delegate iterator has no .throw
	      // method always terminates the yield* loop.
	      context.delegate = null;

	      if (context.method === "throw") {
	        if (delegate.iterator.return) {
	          // If the delegate iterator has a return method, give it a
	          // chance to clean up.
	          context.method = "return";
	          context.arg = undefined;
	          maybeInvokeDelegate(delegate, context);

	          if (context.method === "throw") {
	            // If maybeInvokeDelegate(context) changed context.method from
	            // "return" to "throw", let that override the TypeError below.
	            return ContinueSentinel;
	          }
	        }

	        context.method = "throw";
	        context.arg = new TypeError(
	          "The iterator does not provide a 'throw' method");
	      }

	      return ContinueSentinel;
	    }

	    var record = tryCatch(method, delegate.iterator, context.arg);

	    if (record.type === "throw") {
	      context.method = "throw";
	      context.arg = record.arg;
	      context.delegate = null;
	      return ContinueSentinel;
	    }

	    var info = record.arg;

	    if (! info) {
	      context.method = "throw";
	      context.arg = new TypeError("iterator result is not an object");
	      context.delegate = null;
	      return ContinueSentinel;
	    }

	    if (info.done) {
	      // Assign the result of the finished delegate to the temporary
	      // variable specified by delegate.resultName (see delegateYield).
	      context[delegate.resultName] = info.value;

	      // Resume execution at the desired location (see delegateYield).
	      context.next = delegate.nextLoc;

	      // If context.method was "throw" but the delegate handled the
	      // exception, let the outer generator proceed normally. If
	      // context.method was "next", forget context.arg since it has been
	      // "consumed" by the delegate iterator. If context.method was
	      // "return", allow the original .return call to continue in the
	      // outer generator.
	      if (context.method !== "return") {
	        context.method = "next";
	        context.arg = undefined;
	      }

	    } else {
	      // Re-yield the result returned by the delegate method.
	      return info;
	    }

	    // The delegate iterator is finished, so forget it and continue with
	    // the outer generator.
	    context.delegate = null;
	    return ContinueSentinel;
	  }

	  // Define Generator.prototype.{next,throw,return} in terms of the
	  // unified ._invoke helper method.
	  defineIteratorMethods(Gp);

	  Gp[toStringTagSymbol] = "Generator";

	  // A Generator should always return itself as the iterator object when the
	  // @@iterator function is called on it. Some browsers' implementations of the
	  // iterator prototype chain incorrectly implement this, causing the Generator
	  // object to not be returned from this call. This ensures that doesn't happen.
	  // See https://github.com/facebook/regenerator/issues/274 for more details.
	  Gp[iteratorSymbol] = function() {
	    return this;
	  };

	  Gp.toString = function() {
	    return "[object Generator]";
	  };

	  function pushTryEntry(locs) {
	    var entry = { tryLoc: locs[0] };

	    if (1 in locs) {
	      entry.catchLoc = locs[1];
	    }

	    if (2 in locs) {
	      entry.finallyLoc = locs[2];
	      entry.afterLoc = locs[3];
	    }

	    this.tryEntries.push(entry);
	  }

	  function resetTryEntry(entry) {
	    var record = entry.completion || {};
	    record.type = "normal";
	    delete record.arg;
	    entry.completion = record;
	  }

	  function Context(tryLocsList) {
	    // The root entry object (effectively a try statement without a catch
	    // or a finally block) gives us a place to store values thrown from
	    // locations where there is no enclosing try statement.
	    this.tryEntries = [{ tryLoc: "root" }];
	    tryLocsList.forEach(pushTryEntry, this);
	    this.reset(true);
	  }

	  runtime.keys = function(object) {
	    var keys = [];
	    for (var key in object) {
	      keys.push(key);
	    }
	    keys.reverse();

	    // Rather than returning an object with a next method, we keep
	    // things simple and return the next function itself.
	    return function next() {
	      while (keys.length) {
	        var key = keys.pop();
	        if (key in object) {
	          next.value = key;
	          next.done = false;
	          return next;
	        }
	      }

	      // To avoid creating an additional object, we just hang the .value
	      // and .done properties off the next function object itself. This
	      // also ensures that the minifier will not anonymize the function.
	      next.done = true;
	      return next;
	    };
	  };

	  function values(iterable) {
	    if (iterable) {
	      var iteratorMethod = iterable[iteratorSymbol];
	      if (iteratorMethod) {
	        return iteratorMethod.call(iterable);
	      }

	      if (typeof iterable.next === "function") {
	        return iterable;
	      }

	      if (!isNaN(iterable.length)) {
	        var i = -1, next = function next() {
	          while (++i < iterable.length) {
	            if (hasOwn.call(iterable, i)) {
	              next.value = iterable[i];
	              next.done = false;
	              return next;
	            }
	          }

	          next.value = undefined;
	          next.done = true;

	          return next;
	        };

	        return next.next = next;
	      }
	    }

	    // Return an iterator with no values.
	    return { next: doneResult };
	  }
	  runtime.values = values;

	  function doneResult() {
	    return { value: undefined, done: true };
	  }

	  Context.prototype = {
	    constructor: Context,

	    reset: function(skipTempReset) {
	      this.prev = 0;
	      this.next = 0;
	      // Resetting context._sent for legacy support of Babel's
	      // function.sent implementation.
	      this.sent = this._sent = undefined;
	      this.done = false;
	      this.delegate = null;

	      this.method = "next";
	      this.arg = undefined;

	      this.tryEntries.forEach(resetTryEntry);

	      if (!skipTempReset) {
	        for (var name in this) {
	          // Not sure about the optimal order of these conditions:
	          if (name.charAt(0) === "t" &&
	              hasOwn.call(this, name) &&
	              !isNaN(+name.slice(1))) {
	            this[name] = undefined;
	          }
	        }
	      }
	    },

	    stop: function() {
	      this.done = true;

	      var rootEntry = this.tryEntries[0];
	      var rootRecord = rootEntry.completion;
	      if (rootRecord.type === "throw") {
	        throw rootRecord.arg;
	      }

	      return this.rval;
	    },

	    dispatchException: function(exception) {
	      if (this.done) {
	        throw exception;
	      }

	      var context = this;
	      function handle(loc, caught) {
	        record.type = "throw";
	        record.arg = exception;
	        context.next = loc;

	        if (caught) {
	          // If the dispatched exception was caught by a catch block,
	          // then let that catch block handle the exception normally.
	          context.method = "next";
	          context.arg = undefined;
	        }

	        return !! caught;
	      }

	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        var record = entry.completion;

	        if (entry.tryLoc === "root") {
	          // Exception thrown outside of any try block that could handle
	          // it, so set the completion value of the entire function to
	          // throw the exception.
	          return handle("end");
	        }

	        if (entry.tryLoc <= this.prev) {
	          var hasCatch = hasOwn.call(entry, "catchLoc");
	          var hasFinally = hasOwn.call(entry, "finallyLoc");

	          if (hasCatch && hasFinally) {
	            if (this.prev < entry.catchLoc) {
	              return handle(entry.catchLoc, true);
	            } else if (this.prev < entry.finallyLoc) {
	              return handle(entry.finallyLoc);
	            }

	          } else if (hasCatch) {
	            if (this.prev < entry.catchLoc) {
	              return handle(entry.catchLoc, true);
	            }

	          } else if (hasFinally) {
	            if (this.prev < entry.finallyLoc) {
	              return handle(entry.finallyLoc);
	            }

	          } else {
	            throw new Error("try statement without catch or finally");
	          }
	        }
	      }
	    },

	    abrupt: function(type, arg) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        if (entry.tryLoc <= this.prev &&
	            hasOwn.call(entry, "finallyLoc") &&
	            this.prev < entry.finallyLoc) {
	          var finallyEntry = entry;
	          break;
	        }
	      }

	      if (finallyEntry &&
	          (type === "break" ||
	           type === "continue") &&
	          finallyEntry.tryLoc <= arg &&
	          arg <= finallyEntry.finallyLoc) {
	        // Ignore the finally entry if control is not jumping to a
	        // location outside the try/catch block.
	        finallyEntry = null;
	      }

	      var record = finallyEntry ? finallyEntry.completion : {};
	      record.type = type;
	      record.arg = arg;

	      if (finallyEntry) {
	        this.method = "next";
	        this.next = finallyEntry.finallyLoc;
	        return ContinueSentinel;
	      }

	      return this.complete(record);
	    },

	    complete: function(record, afterLoc) {
	      if (record.type === "throw") {
	        throw record.arg;
	      }

	      if (record.type === "break" ||
	          record.type === "continue") {
	        this.next = record.arg;
	      } else if (record.type === "return") {
	        this.rval = this.arg = record.arg;
	        this.method = "return";
	        this.next = "end";
	      } else if (record.type === "normal" && afterLoc) {
	        this.next = afterLoc;
	      }

	      return ContinueSentinel;
	    },

	    finish: function(finallyLoc) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        if (entry.finallyLoc === finallyLoc) {
	          this.complete(entry.completion, entry.afterLoc);
	          resetTryEntry(entry);
	          return ContinueSentinel;
	        }
	      }
	    },

	    "catch": function(tryLoc) {
	      for (var i = this.tryEntries.length - 1; i >= 0; --i) {
	        var entry = this.tryEntries[i];
	        if (entry.tryLoc === tryLoc) {
	          var record = entry.completion;
	          if (record.type === "throw") {
	            var thrown = record.arg;
	            resetTryEntry(entry);
	          }
	          return thrown;
	        }
	      }

	      // The context.catch method must only be called with a location
	      // argument that corresponds to a known catch block.
	      throw new Error("illegal catch attempt");
	    },

	    delegateYield: function(iterable, resultName, nextLoc) {
	      this.delegate = {
	        iterator: values(iterable),
	        resultName: resultName,
	        nextLoc: nextLoc
	      };

	      if (this.method === "next") {
	        // Deliberately forget the last sent value so that we don't
	        // accidentally pass it on to the delegate.
	        this.arg = undefined;
	      }

	      return ContinueSentinel;
	    }
	  };
	})(
	  // Among the various tricks for obtaining a reference to the global
	  // object, this seems to be the most reliable technique that does not
	  // use indirect eval (which violates Content Security Policy).
	  typeof global === "object" ? global :
	  typeof window === "object" ? window :
	  typeof self === "object" ? self : this
	);

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 329 */
/***/ (function(module, exports, __webpack_require__) {

	__webpack_require__(330);
	module.exports = __webpack_require__(9).RegExp.escape;


/***/ }),
/* 330 */
/***/ (function(module, exports, __webpack_require__) {

	// https://github.com/benjamingr/RexExp.escape
	var $export = __webpack_require__(8);
	var $re = __webpack_require__(331)(/[\\^$*+?.()|[\]{}]/g, '\\$&');

	$export($export.S, 'RegExp', { escape: function escape(it) { return $re(it); } });


/***/ }),
/* 331 */
/***/ (function(module, exports) {

	module.exports = function (regExp, replace) {
	  var replacer = replace === Object(replace) ? function (part) {
	    return replace[part];
	  } : replace;
	  return function (it) {
	    return String(it).replace(regExp, replacer);
	  };
	};


/***/ }),
/* 332 */
/***/ (function(module, exports, __webpack_require__) {

	var BSON = __webpack_require__(333),
	    Binary = __webpack_require__(356),
	    Code = __webpack_require__(351),
	    DBRef = __webpack_require__(355),
	    Decimal128 = __webpack_require__(352),
	    Double = __webpack_require__(336),
	    Int32 = __webpack_require__(350),
	    Long = __webpack_require__(335),
	    Map = __webpack_require__(334),
	    MaxKey = __webpack_require__(354),
	    MinKey = __webpack_require__(353),
	    ObjectId = __webpack_require__(338),
	    BSONRegExp = __webpack_require__(348),
	    Symbol = __webpack_require__(349),
	    Timestamp = __webpack_require__(337);

	// BSON MAX VALUES
	BSON.BSON_INT32_MAX = 0x7fffffff;
	BSON.BSON_INT32_MIN = -0x80000000;

	BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
	BSON.BSON_INT64_MIN = -Math.pow(2, 63);

	// JS MAX PRECISE VALUES
	BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
	BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

	// Add BSON types to function creation
	BSON.Binary = Binary;
	BSON.Code = Code;
	BSON.DBRef = DBRef;
	BSON.Decimal128 = Decimal128;
	BSON.Double = Double;
	BSON.Int32 = Int32;
	BSON.Long = Long;
	BSON.Map = Map;
	BSON.MaxKey = MaxKey;
	BSON.MinKey = MinKey;
	BSON.ObjectId = ObjectId;
	BSON.ObjectID = ObjectId;
	BSON.BSONRegExp = BSONRegExp;
	BSON.Symbol = Symbol;
	BSON.Timestamp = Timestamp;

	// Return the BSON
	module.exports = BSON;

/***/ }),
/* 333 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var Map = __webpack_require__(334),
	    Long = __webpack_require__(335),
	    Double = __webpack_require__(336),
	    Timestamp = __webpack_require__(337),
	    ObjectID = __webpack_require__(338),
	    BSONRegExp = __webpack_require__(348),
	    Symbol = __webpack_require__(349),
	    Int32 = __webpack_require__(350),
	    Code = __webpack_require__(351),
	    Decimal128 = __webpack_require__(352),
	    MinKey = __webpack_require__(353),
	    MaxKey = __webpack_require__(354),
	    DBRef = __webpack_require__(355),
	    Binary = __webpack_require__(356);

	// Parts of the parser
	var deserialize = __webpack_require__(357),
	    serializer = __webpack_require__(358),
	    calculateObjectSize = __webpack_require__(360),
	    utils = __webpack_require__(344);

	/**
	 * @ignore
	 * @api private
	 */
	// Default Max Size
	var MAXSIZE = 1024 * 1024 * 17;

	// Current Internal Temporary Serialization Buffer
	var buffer = utils.allocBuffer(MAXSIZE);

	var BSON = function () {};

	/**
	 * Serialize a Javascript object.
	 *
	 * @param {Object} object the Javascript object to serialize.
	 * @param {Boolean} [options.checkKeys] the serializer will check if keys are valid.
	 * @param {Boolean} [options.serializeFunctions=false] serialize the javascript functions **(default:false)**.
	 * @param {Boolean} [options.ignoreUndefined=true] ignore undefined fields **(default:true)**.
	 * @param {Number} [options.minInternalBufferSize=1024*1024*17] minimum size of the internal temporary serialization buffer **(default:1024*1024*17)**.
	 * @return {Buffer} returns the Buffer object containing the serialized object.
	 * @api public
	 */
	BSON.prototype.serialize = function serialize(object, options) {
	  options = options || {};
	  // Unpack the options
	  var checkKeys = typeof options.checkKeys === 'boolean' ? options.checkKeys : false;
	  var serializeFunctions = typeof options.serializeFunctions === 'boolean' ? options.serializeFunctions : false;
	  var ignoreUndefined = typeof options.ignoreUndefined === 'boolean' ? options.ignoreUndefined : true;
	  var minInternalBufferSize = typeof options.minInternalBufferSize === 'number' ? options.minInternalBufferSize : MAXSIZE;

	  // Resize the internal serialization buffer if needed
	  if (buffer.length < minInternalBufferSize) {
	    buffer = utils.allocBuffer(minInternalBufferSize);
	  }

	  // Attempt to serialize
	  var serializationIndex = serializer(buffer, object, checkKeys, 0, 0, serializeFunctions, ignoreUndefined, []);
	  // Create the final buffer
	  var finishedBuffer = utils.allocBuffer(serializationIndex);
	  // Copy into the finished buffer
	  buffer.copy(finishedBuffer, 0, 0, finishedBuffer.length);
	  // Return the buffer
	  return finishedBuffer;
	};

	/**
	 * Serialize a Javascript object using a predefined Buffer and index into the buffer, useful when pre-allocating the space for serialization.
	 *
	 * @param {Object} object the Javascript object to serialize.
	 * @param {Buffer} buffer the Buffer you pre-allocated to store the serialized BSON object.
	 * @param {Boolean} [options.checkKeys] the serializer will check if keys are valid.
	 * @param {Boolean} [options.serializeFunctions=false] serialize the javascript functions **(default:false)**.
	 * @param {Boolean} [options.ignoreUndefined=true] ignore undefined fields **(default:true)**.
	 * @param {Number} [options.index] the index in the buffer where we wish to start serializing into.
	 * @return {Number} returns the index pointing to the last written byte in the buffer.
	 * @api public
	 */
	BSON.prototype.serializeWithBufferAndIndex = function (object, finalBuffer, options) {
	  options = options || {};
	  // Unpack the options
	  var checkKeys = typeof options.checkKeys === 'boolean' ? options.checkKeys : false;
	  var serializeFunctions = typeof options.serializeFunctions === 'boolean' ? options.serializeFunctions : false;
	  var ignoreUndefined = typeof options.ignoreUndefined === 'boolean' ? options.ignoreUndefined : true;
	  var startIndex = typeof options.index === 'number' ? options.index : 0;

	  // Attempt to serialize
	  var serializationIndex = serializer(finalBuffer, object, checkKeys, startIndex || 0, 0, serializeFunctions, ignoreUndefined);

	  // Return the index
	  return serializationIndex - 1;
	};

	/**
	 * Deserialize data as BSON.
	 *
	 * @param {Buffer} buffer the buffer containing the serialized set of BSON documents.
	 * @param {Object} [options.evalFunctions=false] evaluate functions in the BSON document scoped to the object deserialized.
	 * @param {Object} [options.cacheFunctions=false] cache evaluated functions for reuse.
	 * @param {Object} [options.cacheFunctionsCrc32=false] use a crc32 code for caching, otherwise use the string of the function.
	 * @param {Object} [options.promoteLongs=true] when deserializing a Long will fit it into a Number if it's smaller than 53 bits
	 * @param {Object} [options.promoteBuffers=false] when deserializing a Binary will return it as a node.js Buffer instance.
	 * @param {Object} [options.promoteValues=false] when deserializing will promote BSON values to their Node.js closest equivalent types.
	 * @param {Object} [options.fieldsAsRaw=null] allow to specify if there what fields we wish to return as unserialized raw buffer.
	 * @param {Object} [options.bsonRegExp=false] return BSON regular expressions as BSONRegExp instances.
	 * @return {Object} returns the deserialized Javascript Object.
	 * @api public
	 */
	BSON.prototype.deserialize = function (buffer, options) {
	  return deserialize(buffer, options);
	};

	/**
	 * Calculate the bson size for a passed in Javascript object.
	 *
	 * @param {Object} object the Javascript object to calculate the BSON byte size for.
	 * @param {Boolean} [options.serializeFunctions=false] serialize the javascript functions **(default:false)**.
	 * @param {Boolean} [options.ignoreUndefined=true] ignore undefined fields **(default:true)**.
	 * @return {Number} returns the number of bytes the BSON object will take up.
	 * @api public
	 */
	BSON.prototype.calculateObjectSize = function (object, options) {
	  options = options || {};

	  var serializeFunctions = typeof options.serializeFunctions === 'boolean' ? options.serializeFunctions : false;
	  var ignoreUndefined = typeof options.ignoreUndefined === 'boolean' ? options.ignoreUndefined : true;

	  return calculateObjectSize(object, serializeFunctions, ignoreUndefined);
	};

	/**
	 * Deserialize stream data as BSON documents.
	 *
	 * @param {Buffer} data the buffer containing the serialized set of BSON documents.
	 * @param {Number} startIndex the start index in the data Buffer where the deserialization is to start.
	 * @param {Number} numberOfDocuments number of documents to deserialize.
	 * @param {Array} documents an array where to store the deserialized documents.
	 * @param {Number} docStartIndex the index in the documents array from where to start inserting documents.
	 * @param {Object} [options] additional options used for the deserialization.
	 * @param {Object} [options.evalFunctions=false] evaluate functions in the BSON document scoped to the object deserialized.
	 * @param {Object} [options.cacheFunctions=false] cache evaluated functions for reuse.
	 * @param {Object} [options.cacheFunctionsCrc32=false] use a crc32 code for caching, otherwise use the string of the function.
	 * @param {Object} [options.promoteLongs=true] when deserializing a Long will fit it into a Number if it's smaller than 53 bits
	 * @param {Object} [options.promoteBuffers=false] when deserializing a Binary will return it as a node.js Buffer instance.
	 * @param {Object} [options.promoteValues=false] when deserializing will promote BSON values to their Node.js closest equivalent types.
	 * @param {Object} [options.fieldsAsRaw=null] allow to specify if there what fields we wish to return as unserialized raw buffer.
	 * @param {Object} [options.bsonRegExp=false] return BSON regular expressions as BSONRegExp instances.
	 * @return {Number} returns the next index in the buffer after deserialization **x** numbers of documents.
	 * @api public
	 */
	BSON.prototype.deserializeStream = function (data, startIndex, numberOfDocuments, documents, docStartIndex, options) {
	  options = options != null ? options : {};
	  var index = startIndex;
	  // Loop over all documents
	  for (var i = 0; i < numberOfDocuments; i++) {
	    // Find size of the document
	    var size = data[index] | data[index + 1] << 8 | data[index + 2] << 16 | data[index + 3] << 24;
	    // Update options with index
	    options['index'] = index;
	    // Parse the document at this point
	    documents[docStartIndex + i] = this.deserialize(data, options);
	    // Adjust index by the document size
	    index = index + size;
	  }

	  // Return object containing end index of parsing and list of documents
	  return index;
	};

	/**
	 * @ignore
	 * @api private
	 */
	// BSON MAX VALUES
	BSON.BSON_INT32_MAX = 0x7fffffff;
	BSON.BSON_INT32_MIN = -0x80000000;

	BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
	BSON.BSON_INT64_MIN = -Math.pow(2, 63);

	// JS MAX PRECISE VALUES
	BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
	BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

	// Internal long versions
	// var JS_INT_MAX_LONG = Long.fromNumber(0x20000000000000); // Any integer up to 2^53 can be precisely represented by a double.
	// var JS_INT_MIN_LONG = Long.fromNumber(-0x20000000000000); // Any integer down to -2^53 can be precisely represented by a double.

	/**
	 * Number BSON Type
	 *
	 * @classconstant BSON_DATA_NUMBER
	 **/
	BSON.BSON_DATA_NUMBER = 1;
	/**
	 * String BSON Type
	 *
	 * @classconstant BSON_DATA_STRING
	 **/
	BSON.BSON_DATA_STRING = 2;
	/**
	 * Object BSON Type
	 *
	 * @classconstant BSON_DATA_OBJECT
	 **/
	BSON.BSON_DATA_OBJECT = 3;
	/**
	 * Array BSON Type
	 *
	 * @classconstant BSON_DATA_ARRAY
	 **/
	BSON.BSON_DATA_ARRAY = 4;
	/**
	 * Binary BSON Type
	 *
	 * @classconstant BSON_DATA_BINARY
	 **/
	BSON.BSON_DATA_BINARY = 5;
	/**
	 * ObjectID BSON Type
	 *
	 * @classconstant BSON_DATA_OID
	 **/
	BSON.BSON_DATA_OID = 7;
	/**
	 * Boolean BSON Type
	 *
	 * @classconstant BSON_DATA_BOOLEAN
	 **/
	BSON.BSON_DATA_BOOLEAN = 8;
	/**
	 * Date BSON Type
	 *
	 * @classconstant BSON_DATA_DATE
	 **/
	BSON.BSON_DATA_DATE = 9;
	/**
	 * null BSON Type
	 *
	 * @classconstant BSON_DATA_NULL
	 **/
	BSON.BSON_DATA_NULL = 10;
	/**
	 * RegExp BSON Type
	 *
	 * @classconstant BSON_DATA_REGEXP
	 **/
	BSON.BSON_DATA_REGEXP = 11;
	/**
	 * Code BSON Type
	 *
	 * @classconstant BSON_DATA_CODE
	 **/
	BSON.BSON_DATA_CODE = 13;
	/**
	 * Symbol BSON Type
	 *
	 * @classconstant BSON_DATA_SYMBOL
	 **/
	BSON.BSON_DATA_SYMBOL = 14;
	/**
	 * Code with Scope BSON Type
	 *
	 * @classconstant BSON_DATA_CODE_W_SCOPE
	 **/
	BSON.BSON_DATA_CODE_W_SCOPE = 15;
	/**
	 * 32 bit Integer BSON Type
	 *
	 * @classconstant BSON_DATA_INT
	 **/
	BSON.BSON_DATA_INT = 16;
	/**
	 * Timestamp BSON Type
	 *
	 * @classconstant BSON_DATA_TIMESTAMP
	 **/
	BSON.BSON_DATA_TIMESTAMP = 17;
	/**
	 * Long BSON Type
	 *
	 * @classconstant BSON_DATA_LONG
	 **/
	BSON.BSON_DATA_LONG = 18;
	/**
	 * MinKey BSON Type
	 *
	 * @classconstant BSON_DATA_MIN_KEY
	 **/
	BSON.BSON_DATA_MIN_KEY = 0xff;
	/**
	 * MaxKey BSON Type
	 *
	 * @classconstant BSON_DATA_MAX_KEY
	 **/
	BSON.BSON_DATA_MAX_KEY = 0x7f;

	/**
	 * Binary Default Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_DEFAULT
	 **/
	BSON.BSON_BINARY_SUBTYPE_DEFAULT = 0;
	/**
	 * Binary Function Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_FUNCTION
	 **/
	BSON.BSON_BINARY_SUBTYPE_FUNCTION = 1;
	/**
	 * Binary Byte Array Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_BYTE_ARRAY
	 **/
	BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
	/**
	 * Binary UUID Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_UUID
	 **/
	BSON.BSON_BINARY_SUBTYPE_UUID = 3;
	/**
	 * Binary MD5 Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_MD5
	 **/
	BSON.BSON_BINARY_SUBTYPE_MD5 = 4;
	/**
	 * Binary User Defined Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_USER_DEFINED
	 **/
	BSON.BSON_BINARY_SUBTYPE_USER_DEFINED = 128;

	// Return BSON
	module.exports = BSON;
	module.exports.Code = Code;
	module.exports.Map = Map;
	module.exports.Symbol = Symbol;
	module.exports.BSON = BSON;
	module.exports.DBRef = DBRef;
	module.exports.Binary = Binary;
	module.exports.ObjectID = ObjectID;
	module.exports.Long = Long;
	module.exports.Timestamp = Timestamp;
	module.exports.Double = Double;
	module.exports.Int32 = Int32;
	module.exports.MinKey = MinKey;
	module.exports.MaxKey = MaxKey;
	module.exports.BSONRegExp = BSONRegExp;
	module.exports.Decimal128 = Decimal128;

/***/ }),
/* 334 */
/***/ (function(module, exports) {

	/* WEBPACK VAR INJECTION */(function(global) {'use strict';

	// We have an ES6 Map available, return the native instance

	if (typeof global.Map !== 'undefined') {
	  module.exports = global.Map;
	  module.exports.Map = global.Map;
	} else {
	  // We will return a polyfill
	  var Map = function (array) {
	    this._keys = [];
	    this._values = {};

	    for (var i = 0; i < array.length; i++) {
	      if (array[i] == null) continue; // skip null and undefined
	      var entry = array[i];
	      var key = entry[0];
	      var value = entry[1];
	      // Add the key to the list of keys in order
	      this._keys.push(key);
	      // Add the key and value to the values dictionary with a point
	      // to the location in the ordered keys list
	      this._values[key] = { v: value, i: this._keys.length - 1 };
	    }
	  };

	  Map.prototype.clear = function () {
	    this._keys = [];
	    this._values = {};
	  };

	  Map.prototype.delete = function (key) {
	    var value = this._values[key];
	    if (value == null) return false;
	    // Delete entry
	    delete this._values[key];
	    // Remove the key from the ordered keys list
	    this._keys.splice(value.i, 1);
	    return true;
	  };

	  Map.prototype.entries = function () {
	    var self = this;
	    var index = 0;

	    return {
	      next: function () {
	        var key = self._keys[index++];
	        return {
	          value: key !== undefined ? [key, self._values[key].v] : undefined,
	          done: key !== undefined ? false : true
	        };
	      }
	    };
	  };

	  Map.prototype.forEach = function (callback, self) {
	    self = self || this;

	    for (var i = 0; i < this._keys.length; i++) {
	      var key = this._keys[i];
	      // Call the forEach callback
	      callback.call(self, this._values[key].v, key, self);
	    }
	  };

	  Map.prototype.get = function (key) {
	    return this._values[key] ? this._values[key].v : undefined;
	  };

	  Map.prototype.has = function (key) {
	    return this._values[key] != null;
	  };

	  Map.prototype.keys = function () {
	    var self = this;
	    var index = 0;

	    return {
	      next: function () {
	        var key = self._keys[index++];
	        return {
	          value: key !== undefined ? key : undefined,
	          done: key !== undefined ? false : true
	        };
	      }
	    };
	  };

	  Map.prototype.set = function (key, value) {
	    if (this._values[key]) {
	      this._values[key].v = value;
	      return this;
	    }

	    // Add the key to the list of keys in order
	    this._keys.push(key);
	    // Add the key and value to the values dictionary with a point
	    // to the location in the ordered keys list
	    this._values[key] = { v: value, i: this._keys.length - 1 };
	    return this;
	  };

	  Map.prototype.values = function () {
	    var self = this;
	    var index = 0;

	    return {
	      next: function () {
	        var key = self._keys[index++];
	        return {
	          value: key !== undefined ? self._values[key].v : undefined,
	          done: key !== undefined ? false : true
	        };
	      }
	    };
	  };

	  // Last ismaster
	  Object.defineProperty(Map.prototype, 'size', {
	    enumerable: true,
	    get: function () {
	      return this._keys.length;
	    }
	  });

	  module.exports = Map;
	  module.exports.Map = Map;
	}
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 335 */
/***/ (function(module, exports) {

	// Licensed under the Apache License, Version 2.0 (the "License");
	// you may not use this file except in compliance with the License.
	// You may obtain a copy of the License at
	//
	//     http://www.apache.org/licenses/LICENSE-2.0
	//
	// Unless required by applicable law or agreed to in writing, software
	// distributed under the License is distributed on an "AS IS" BASIS,
	// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	// See the License for the specific language governing permissions and
	// limitations under the License.
	//
	// Copyright 2009 Google Inc. All Rights Reserved

	/**
	 * Defines a Long class for representing a 64-bit two's-complement
	 * integer value, which faithfully simulates the behavior of a Java "Long". This
	 * implementation is derived from LongLib in GWT.
	 *
	 * Constructs a 64-bit two's-complement integer, given its low and high 32-bit
	 * values as *signed* integers.  See the from* functions below for more
	 * convenient ways of constructing Longs.
	 *
	 * The internal representation of a Long is the two given signed, 32-bit values.
	 * We use 32-bit pieces because these are the size of integers on which
	 * Javascript performs bit-operations.  For operations like addition and
	 * multiplication, we split each number into 16-bit pieces, which can easily be
	 * multiplied within Javascript's floating-point representation without overflow
	 * or change in sign.
	 *
	 * In the algorithms below, we frequently reduce the negative case to the
	 * positive case by negating the input(s) and then post-processing the result.
	 * Note that we must ALWAYS check specially whether those values are MIN_VALUE
	 * (-2^63) because -MIN_VALUE == MIN_VALUE (since 2^63 cannot be represented as
	 * a positive number, it overflows back into a negative).  Not handling this
	 * case would often result in infinite recursion.
	 *
	 * @class
	 * @param {number} low  the low (signed) 32 bits of the Long.
	 * @param {number} high the high (signed) 32 bits of the Long.
	 * @return {Long}
	 */
	function Long(low, high) {
	  if (!(this instanceof Long)) return new Long(low, high);

	  this._bsontype = 'Long';
	  /**
	   * @type {number}
	   * @ignore
	   */
	  this.low_ = low | 0; // force into 32 signed bits.

	  /**
	   * @type {number}
	   * @ignore
	   */
	  this.high_ = high | 0; // force into 32 signed bits.
	}

	/**
	 * Return the int value.
	 *
	 * @method
	 * @return {number} the value, assuming it is a 32-bit integer.
	 */
	Long.prototype.toInt = function () {
	  return this.low_;
	};

	/**
	 * Return the Number value.
	 *
	 * @method
	 * @return {number} the closest floating-point representation to this value.
	 */
	Long.prototype.toNumber = function () {
	  return this.high_ * Long.TWO_PWR_32_DBL_ + this.getLowBitsUnsigned();
	};

	/**
	 * Return the JSON value.
	 *
	 * @method
	 * @return {string} the JSON representation.
	 */
	Long.prototype.toJSON = function () {
	  return this.toString();
	};

	/**
	 * Return the String value.
	 *
	 * @method
	 * @param {number} [opt_radix] the radix in which the text should be written.
	 * @return {string} the textual representation of this value.
	 */
	Long.prototype.toString = function (opt_radix) {
	  var radix = opt_radix || 10;
	  if (radix < 2 || 36 < radix) {
	    throw Error('radix out of range: ' + radix);
	  }

	  if (this.isZero()) {
	    return '0';
	  }

	  if (this.isNegative()) {
	    if (this.equals(Long.MIN_VALUE)) {
	      // We need to change the Long value before it can be negated, so we remove
	      // the bottom-most digit in this base and then recurse to do the rest.
	      var radixLong = Long.fromNumber(radix);
	      var div = this.div(radixLong);
	      var rem = div.multiply(radixLong).subtract(this);
	      return div.toString(radix) + rem.toInt().toString(radix);
	    } else {
	      return '-' + this.negate().toString(radix);
	    }
	  }

	  // Do several (6) digits each time through the loop, so as to
	  // minimize the calls to the very expensive emulated div.
	  var radixToPower = Long.fromNumber(Math.pow(radix, 6));

	  rem = this;
	  var result = '';

	  while (!rem.isZero()) {
	    var remDiv = rem.div(radixToPower);
	    var intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
	    var digits = intval.toString(radix);

	    rem = remDiv;
	    if (rem.isZero()) {
	      return digits + result;
	    } else {
	      while (digits.length < 6) {
	        digits = '0' + digits;
	      }
	      result = '' + digits + result;
	    }
	  }
	};

	/**
	 * Return the high 32-bits value.
	 *
	 * @method
	 * @return {number} the high 32-bits as a signed value.
	 */
	Long.prototype.getHighBits = function () {
	  return this.high_;
	};

	/**
	 * Return the low 32-bits value.
	 *
	 * @method
	 * @return {number} the low 32-bits as a signed value.
	 */
	Long.prototype.getLowBits = function () {
	  return this.low_;
	};

	/**
	 * Return the low unsigned 32-bits value.
	 *
	 * @method
	 * @return {number} the low 32-bits as an unsigned value.
	 */
	Long.prototype.getLowBitsUnsigned = function () {
	  return this.low_ >= 0 ? this.low_ : Long.TWO_PWR_32_DBL_ + this.low_;
	};

	/**
	 * Returns the number of bits needed to represent the absolute value of this Long.
	 *
	 * @method
	 * @return {number} Returns the number of bits needed to represent the absolute value of this Long.
	 */
	Long.prototype.getNumBitsAbs = function () {
	  if (this.isNegative()) {
	    if (this.equals(Long.MIN_VALUE)) {
	      return 64;
	    } else {
	      return this.negate().getNumBitsAbs();
	    }
	  } else {
	    var val = this.high_ !== 0 ? this.high_ : this.low_;
	    for (var bit = 31; bit > 0; bit--) {
	      if ((val & 1 << bit) !== 0) {
	        break;
	      }
	    }
	    return this.high_ !== 0 ? bit + 33 : bit + 1;
	  }
	};

	/**
	 * Return whether this value is zero.
	 *
	 * @method
	 * @return {boolean} whether this value is zero.
	 */
	Long.prototype.isZero = function () {
	  return this.high_ === 0 && this.low_ === 0;
	};

	/**
	 * Return whether this value is negative.
	 *
	 * @method
	 * @return {boolean} whether this value is negative.
	 */
	Long.prototype.isNegative = function () {
	  return this.high_ < 0;
	};

	/**
	 * Return whether this value is odd.
	 *
	 * @method
	 * @return {boolean} whether this value is odd.
	 */
	Long.prototype.isOdd = function () {
	  return (this.low_ & 1) === 1;
	};

	/**
	 * Return whether this Long equals the other
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long equals the other
	 */
	Long.prototype.equals = function (other) {
	  return this.high_ === other.high_ && this.low_ === other.low_;
	};

	/**
	 * Return whether this Long does not equal the other.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long does not equal the other.
	 */
	Long.prototype.notEquals = function (other) {
	  return this.high_ !== other.high_ || this.low_ !== other.low_;
	};

	/**
	 * Return whether this Long is less than the other.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long is less than the other.
	 */
	Long.prototype.lessThan = function (other) {
	  return this.compare(other) < 0;
	};

	/**
	 * Return whether this Long is less than or equal to the other.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long is less than or equal to the other.
	 */
	Long.prototype.lessThanOrEqual = function (other) {
	  return this.compare(other) <= 0;
	};

	/**
	 * Return whether this Long is greater than the other.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long is greater than the other.
	 */
	Long.prototype.greaterThan = function (other) {
	  return this.compare(other) > 0;
	};

	/**
	 * Return whether this Long is greater than or equal to the other.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} whether this Long is greater than or equal to the other.
	 */
	Long.prototype.greaterThanOrEqual = function (other) {
	  return this.compare(other) >= 0;
	};

	/**
	 * Compares this Long with the given one.
	 *
	 * @method
	 * @param {Long} other Long to compare against.
	 * @return {boolean} 0 if they are the same, 1 if the this is greater, and -1 if the given one is greater.
	 */
	Long.prototype.compare = function (other) {
	  if (this.equals(other)) {
	    return 0;
	  }

	  var thisNeg = this.isNegative();
	  var otherNeg = other.isNegative();
	  if (thisNeg && !otherNeg) {
	    return -1;
	  }
	  if (!thisNeg && otherNeg) {
	    return 1;
	  }

	  // at this point, the signs are the same, so subtraction will not overflow
	  if (this.subtract(other).isNegative()) {
	    return -1;
	  } else {
	    return 1;
	  }
	};

	/**
	 * The negation of this value.
	 *
	 * @method
	 * @return {Long} the negation of this value.
	 */
	Long.prototype.negate = function () {
	  if (this.equals(Long.MIN_VALUE)) {
	    return Long.MIN_VALUE;
	  } else {
	    return this.not().add(Long.ONE);
	  }
	};

	/**
	 * Returns the sum of this and the given Long.
	 *
	 * @method
	 * @param {Long} other Long to add to this one.
	 * @return {Long} the sum of this and the given Long.
	 */
	Long.prototype.add = function (other) {
	  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

	  var a48 = this.high_ >>> 16;
	  var a32 = this.high_ & 0xffff;
	  var a16 = this.low_ >>> 16;
	  var a00 = this.low_ & 0xffff;

	  var b48 = other.high_ >>> 16;
	  var b32 = other.high_ & 0xffff;
	  var b16 = other.low_ >>> 16;
	  var b00 = other.low_ & 0xffff;

	  var c48 = 0,
	      c32 = 0,
	      c16 = 0,
	      c00 = 0;
	  c00 += a00 + b00;
	  c16 += c00 >>> 16;
	  c00 &= 0xffff;
	  c16 += a16 + b16;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c32 += a32 + b32;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c48 += a48 + b48;
	  c48 &= 0xffff;
	  return Long.fromBits(c16 << 16 | c00, c48 << 16 | c32);
	};

	/**
	 * Returns the difference of this and the given Long.
	 *
	 * @method
	 * @param {Long} other Long to subtract from this.
	 * @return {Long} the difference of this and the given Long.
	 */
	Long.prototype.subtract = function (other) {
	  return this.add(other.negate());
	};

	/**
	 * Returns the product of this and the given Long.
	 *
	 * @method
	 * @param {Long} other Long to multiply with this.
	 * @return {Long} the product of this and the other.
	 */
	Long.prototype.multiply = function (other) {
	  if (this.isZero()) {
	    return Long.ZERO;
	  } else if (other.isZero()) {
	    return Long.ZERO;
	  }

	  if (this.equals(Long.MIN_VALUE)) {
	    return other.isOdd() ? Long.MIN_VALUE : Long.ZERO;
	  } else if (other.equals(Long.MIN_VALUE)) {
	    return this.isOdd() ? Long.MIN_VALUE : Long.ZERO;
	  }

	  if (this.isNegative()) {
	    if (other.isNegative()) {
	      return this.negate().multiply(other.negate());
	    } else {
	      return this.negate().multiply(other).negate();
	    }
	  } else if (other.isNegative()) {
	    return this.multiply(other.negate()).negate();
	  }

	  // If both Longs are small, use float multiplication
	  if (this.lessThan(Long.TWO_PWR_24_) && other.lessThan(Long.TWO_PWR_24_)) {
	    return Long.fromNumber(this.toNumber() * other.toNumber());
	  }

	  // Divide each Long into 4 chunks of 16 bits, and then add up 4x4 products.
	  // We can skip products that would overflow.

	  var a48 = this.high_ >>> 16;
	  var a32 = this.high_ & 0xffff;
	  var a16 = this.low_ >>> 16;
	  var a00 = this.low_ & 0xffff;

	  var b48 = other.high_ >>> 16;
	  var b32 = other.high_ & 0xffff;
	  var b16 = other.low_ >>> 16;
	  var b00 = other.low_ & 0xffff;

	  var c48 = 0,
	      c32 = 0,
	      c16 = 0,
	      c00 = 0;
	  c00 += a00 * b00;
	  c16 += c00 >>> 16;
	  c00 &= 0xffff;
	  c16 += a16 * b00;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c16 += a00 * b16;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c32 += a32 * b00;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c32 += a16 * b16;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c32 += a00 * b32;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c48 += a48 * b00 + a32 * b16 + a16 * b32 + a00 * b48;
	  c48 &= 0xffff;
	  return Long.fromBits(c16 << 16 | c00, c48 << 16 | c32);
	};

	/**
	 * Returns this Long divided by the given one.
	 *
	 * @method
	 * @param {Long} other Long by which to divide.
	 * @return {Long} this Long divided by the given one.
	 */
	Long.prototype.div = function (other) {
	  if (other.isZero()) {
	    throw Error('division by zero');
	  } else if (this.isZero()) {
	    return Long.ZERO;
	  }

	  if (this.equals(Long.MIN_VALUE)) {
	    if (other.equals(Long.ONE) || other.equals(Long.NEG_ONE)) {
	      return Long.MIN_VALUE; // recall that -MIN_VALUE == MIN_VALUE
	    } else if (other.equals(Long.MIN_VALUE)) {
	      return Long.ONE;
	    } else {
	      // At this point, we have |other| >= 2, so |this/other| < |MIN_VALUE|.
	      var halfThis = this.shiftRight(1);
	      var approx = halfThis.div(other).shiftLeft(1);
	      if (approx.equals(Long.ZERO)) {
	        return other.isNegative() ? Long.ONE : Long.NEG_ONE;
	      } else {
	        var rem = this.subtract(other.multiply(approx));
	        var result = approx.add(rem.div(other));
	        return result;
	      }
	    }
	  } else if (other.equals(Long.MIN_VALUE)) {
	    return Long.ZERO;
	  }

	  if (this.isNegative()) {
	    if (other.isNegative()) {
	      return this.negate().div(other.negate());
	    } else {
	      return this.negate().div(other).negate();
	    }
	  } else if (other.isNegative()) {
	    return this.div(other.negate()).negate();
	  }

	  // Repeat the following until the remainder is less than other:  find a
	  // floating-point that approximates remainder / other *from below*, add this
	  // into the result, and subtract it from the remainder.  It is critical that
	  // the approximate value is less than or equal to the real value so that the
	  // remainder never becomes negative.
	  var res = Long.ZERO;
	  rem = this;
	  while (rem.greaterThanOrEqual(other)) {
	    // Approximate the result of division. This may be a little greater or
	    // smaller than the actual value.
	    approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));

	    // We will tweak the approximate result by changing it in the 48-th digit or
	    // the smallest non-fractional digit, whichever is larger.
	    var log2 = Math.ceil(Math.log(approx) / Math.LN2);
	    var delta = log2 <= 48 ? 1 : Math.pow(2, log2 - 48);

	    // Decrease the approximation until it is smaller than the remainder.  Note
	    // that if it is too large, the product overflows and is negative.
	    var approxRes = Long.fromNumber(approx);
	    var approxRem = approxRes.multiply(other);
	    while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
	      approx -= delta;
	      approxRes = Long.fromNumber(approx);
	      approxRem = approxRes.multiply(other);
	    }

	    // We know the answer can't be zero... and actually, zero would cause
	    // infinite recursion since we would make no progress.
	    if (approxRes.isZero()) {
	      approxRes = Long.ONE;
	    }

	    res = res.add(approxRes);
	    rem = rem.subtract(approxRem);
	  }
	  return res;
	};

	/**
	 * Returns this Long modulo the given one.
	 *
	 * @method
	 * @param {Long} other Long by which to mod.
	 * @return {Long} this Long modulo the given one.
	 */
	Long.prototype.modulo = function (other) {
	  return this.subtract(this.div(other).multiply(other));
	};

	/**
	 * The bitwise-NOT of this value.
	 *
	 * @method
	 * @return {Long} the bitwise-NOT of this value.
	 */
	Long.prototype.not = function () {
	  return Long.fromBits(~this.low_, ~this.high_);
	};

	/**
	 * Returns the bitwise-AND of this Long and the given one.
	 *
	 * @method
	 * @param {Long} other the Long with which to AND.
	 * @return {Long} the bitwise-AND of this and the other.
	 */
	Long.prototype.and = function (other) {
	  return Long.fromBits(this.low_ & other.low_, this.high_ & other.high_);
	};

	/**
	 * Returns the bitwise-OR of this Long and the given one.
	 *
	 * @method
	 * @param {Long} other the Long with which to OR.
	 * @return {Long} the bitwise-OR of this and the other.
	 */
	Long.prototype.or = function (other) {
	  return Long.fromBits(this.low_ | other.low_, this.high_ | other.high_);
	};

	/**
	 * Returns the bitwise-XOR of this Long and the given one.
	 *
	 * @method
	 * @param {Long} other the Long with which to XOR.
	 * @return {Long} the bitwise-XOR of this and the other.
	 */
	Long.prototype.xor = function (other) {
	  return Long.fromBits(this.low_ ^ other.low_, this.high_ ^ other.high_);
	};

	/**
	 * Returns this Long with bits shifted to the left by the given amount.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Long} this shifted to the left by the given amount.
	 */
	Long.prototype.shiftLeft = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var low = this.low_;
	    if (numBits < 32) {
	      var high = this.high_;
	      return Long.fromBits(low << numBits, high << numBits | low >>> 32 - numBits);
	    } else {
	      return Long.fromBits(0, low << numBits - 32);
	    }
	  }
	};

	/**
	 * Returns this Long with bits shifted to the right by the given amount.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Long} this shifted to the right by the given amount.
	 */
	Long.prototype.shiftRight = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var high = this.high_;
	    if (numBits < 32) {
	      var low = this.low_;
	      return Long.fromBits(low >>> numBits | high << 32 - numBits, high >> numBits);
	    } else {
	      return Long.fromBits(high >> numBits - 32, high >= 0 ? 0 : -1);
	    }
	  }
	};

	/**
	 * Returns this Long with bits shifted to the right by the given amount, with the new top bits matching the current sign bit.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Long} this shifted to the right by the given amount, with zeros placed into the new leading bits.
	 */
	Long.prototype.shiftRightUnsigned = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var high = this.high_;
	    if (numBits < 32) {
	      var low = this.low_;
	      return Long.fromBits(low >>> numBits | high << 32 - numBits, high >>> numBits);
	    } else if (numBits === 32) {
	      return Long.fromBits(high, 0);
	    } else {
	      return Long.fromBits(high >>> numBits - 32, 0);
	    }
	  }
	};

	/**
	 * Returns a Long representing the given (32-bit) integer value.
	 *
	 * @method
	 * @param {number} value the 32-bit integer in question.
	 * @return {Long} the corresponding Long value.
	 */
	Long.fromInt = function (value) {
	  if (-128 <= value && value < 128) {
	    var cachedObj = Long.INT_CACHE_[value];
	    if (cachedObj) {
	      return cachedObj;
	    }
	  }

	  var obj = new Long(value | 0, value < 0 ? -1 : 0);
	  if (-128 <= value && value < 128) {
	    Long.INT_CACHE_[value] = obj;
	  }
	  return obj;
	};

	/**
	 * Returns a Long representing the given value, provided that it is a finite number. Otherwise, zero is returned.
	 *
	 * @method
	 * @param {number} value the number in question.
	 * @return {Long} the corresponding Long value.
	 */
	Long.fromNumber = function (value) {
	  if (isNaN(value) || !isFinite(value)) {
	    return Long.ZERO;
	  } else if (value <= -Long.TWO_PWR_63_DBL_) {
	    return Long.MIN_VALUE;
	  } else if (value + 1 >= Long.TWO_PWR_63_DBL_) {
	    return Long.MAX_VALUE;
	  } else if (value < 0) {
	    return Long.fromNumber(-value).negate();
	  } else {
	    return new Long(value % Long.TWO_PWR_32_DBL_ | 0, value / Long.TWO_PWR_32_DBL_ | 0);
	  }
	};

	/**
	 * Returns a Long representing the 64-bit integer that comes by concatenating the given high and low bits. Each is assumed to use 32 bits.
	 *
	 * @method
	 * @param {number} lowBits the low 32-bits.
	 * @param {number} highBits the high 32-bits.
	 * @return {Long} the corresponding Long value.
	 */
	Long.fromBits = function (lowBits, highBits) {
	  return new Long(lowBits, highBits);
	};

	/**
	 * Returns a Long representation of the given string, written using the given radix.
	 *
	 * @method
	 * @param {string} str the textual representation of the Long.
	 * @param {number} opt_radix the radix in which the text is written.
	 * @return {Long} the corresponding Long value.
	 */
	Long.fromString = function (str, opt_radix) {
	  if (str.length === 0) {
	    throw Error('number format error: empty string');
	  }

	  var radix = opt_radix || 10;
	  if (radix < 2 || 36 < radix) {
	    throw Error('radix out of range: ' + radix);
	  }

	  if (str.charAt(0) === '-') {
	    return Long.fromString(str.substring(1), radix).negate();
	  } else if (str.indexOf('-') >= 0) {
	    throw Error('number format error: interior "-" character: ' + str);
	  }

	  // Do several (8) digits each time through the loop, so as to
	  // minimize the calls to the very expensive emulated div.
	  var radixToPower = Long.fromNumber(Math.pow(radix, 8));

	  var result = Long.ZERO;
	  for (var i = 0; i < str.length; i += 8) {
	    var size = Math.min(8, str.length - i);
	    var value = parseInt(str.substring(i, i + size), radix);
	    if (size < 8) {
	      var power = Long.fromNumber(Math.pow(radix, size));
	      result = result.multiply(power).add(Long.fromNumber(value));
	    } else {
	      result = result.multiply(radixToPower);
	      result = result.add(Long.fromNumber(value));
	    }
	  }
	  return result;
	};

	// NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
	// from* methods on which they depend.

	/**
	 * A cache of the Long representations of small integer values.
	 * @type {Object}
	 * @ignore
	 */
	Long.INT_CACHE_ = {};

	// NOTE: the compiler should inline these constant values below and then remove
	// these variables, so there should be no runtime penalty for these.

	/**
	 * Number used repeated below in calculations.  This must appear before the
	 * first call to any from* function below.
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_16_DBL_ = 1 << 16;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_24_DBL_ = 1 << 24;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_32_DBL_ = Long.TWO_PWR_16_DBL_ * Long.TWO_PWR_16_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_31_DBL_ = Long.TWO_PWR_32_DBL_ / 2;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_48_DBL_ = Long.TWO_PWR_32_DBL_ * Long.TWO_PWR_16_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_64_DBL_ = Long.TWO_PWR_32_DBL_ * Long.TWO_PWR_32_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Long.TWO_PWR_63_DBL_ = Long.TWO_PWR_64_DBL_ / 2;

	/** @type {Long} */
	Long.ZERO = Long.fromInt(0);

	/** @type {Long} */
	Long.ONE = Long.fromInt(1);

	/** @type {Long} */
	Long.NEG_ONE = Long.fromInt(-1);

	/** @type {Long} */
	Long.MAX_VALUE = Long.fromBits(0xffffffff | 0, 0x7fffffff | 0);

	/** @type {Long} */
	Long.MIN_VALUE = Long.fromBits(0, 0x80000000 | 0);

	/**
	 * @type {Long}
	 * @ignore
	 */
	Long.TWO_PWR_24_ = Long.fromInt(1 << 24);

	/**
	 * Expose.
	 */
	module.exports = Long;
	module.exports.Long = Long;

/***/ }),
/* 336 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON Double type.
	 *
	 * @class
	 * @param {number} value the number we want to represent as a double.
	 * @return {Double}
	 */
	function Double(value) {
	  if (!(this instanceof Double)) return new Double(value);

	  this._bsontype = 'Double';
	  this.value = value;
	}

	/**
	 * Access the number value.
	 *
	 * @method
	 * @return {number} returns the wrapped double number.
	 */
	Double.prototype.valueOf = function () {
	  return this.value;
	};

	/**
	 * @ignore
	 */
	Double.prototype.toJSON = function () {
	  return this.value;
	};

	module.exports = Double;
	module.exports.Double = Double;

/***/ }),
/* 337 */
/***/ (function(module, exports) {

	// Licensed under the Apache License, Version 2.0 (the "License");
	// you may not use this file except in compliance with the License.
	// You may obtain a copy of the License at
	//
	//     http://www.apache.org/licenses/LICENSE-2.0
	//
	// Unless required by applicable law or agreed to in writing, software
	// distributed under the License is distributed on an "AS IS" BASIS,
	// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	// See the License for the specific language governing permissions and
	// limitations under the License.
	//
	// Copyright 2009 Google Inc. All Rights Reserved

	/**
	 * This type is for INTERNAL use in MongoDB only and should not be used in applications.
	 * The appropriate corresponding type is the JavaScript Date type.
	 * 
	 * Defines a Timestamp class for representing a 64-bit two's-complement
	 * integer value, which faithfully simulates the behavior of a Java "Timestamp". This
	 * implementation is derived from TimestampLib in GWT.
	 *
	 * Constructs a 64-bit two's-complement integer, given its low and high 32-bit
	 * values as *signed* integers.  See the from* functions below for more
	 * convenient ways of constructing Timestamps.
	 *
	 * The internal representation of a Timestamp is the two given signed, 32-bit values.
	 * We use 32-bit pieces because these are the size of integers on which
	 * Javascript performs bit-operations.  For operations like addition and
	 * multiplication, we split each number into 16-bit pieces, which can easily be
	 * multiplied within Javascript's floating-point representation without overflow
	 * or change in sign.
	 *
	 * In the algorithms below, we frequently reduce the negative case to the
	 * positive case by negating the input(s) and then post-processing the result.
	 * Note that we must ALWAYS check specially whether those values are MIN_VALUE
	 * (-2^63) because -MIN_VALUE == MIN_VALUE (since 2^63 cannot be represented as
	 * a positive number, it overflows back into a negative).  Not handling this
	 * case would often result in infinite recursion.
	 *
	 * @class
	 * @param {number} low  the low (signed) 32 bits of the Timestamp.
	 * @param {number} high the high (signed) 32 bits of the Timestamp.
	 */
	function Timestamp(low, high) {
	  if (!(this instanceof Timestamp)) return new Timestamp(low, high);
	  this._bsontype = 'Timestamp';
	  /**
	   * @type {number}
	   * @ignore
	   */
	  this.low_ = low | 0; // force into 32 signed bits.

	  /**
	   * @type {number}
	   * @ignore
	   */
	  this.high_ = high | 0; // force into 32 signed bits.
	}

	/**
	 * Return the int value.
	 *
	 * @return {number} the value, assuming it is a 32-bit integer.
	 */
	Timestamp.prototype.toInt = function () {
	  return this.low_;
	};

	/**
	 * Return the Number value.
	 *
	 * @method
	 * @return {number} the closest floating-point representation to this value.
	 */
	Timestamp.prototype.toNumber = function () {
	  return this.high_ * Timestamp.TWO_PWR_32_DBL_ + this.getLowBitsUnsigned();
	};

	/**
	 * Return the JSON value.
	 *
	 * @method
	 * @return {string} the JSON representation.
	 */
	Timestamp.prototype.toJSON = function () {
	  return this.toString();
	};

	/**
	 * Return the String value.
	 *
	 * @method
	 * @param {number} [opt_radix] the radix in which the text should be written.
	 * @return {string} the textual representation of this value.
	 */
	Timestamp.prototype.toString = function (opt_radix) {
	  var radix = opt_radix || 10;
	  if (radix < 2 || 36 < radix) {
	    throw Error('radix out of range: ' + radix);
	  }

	  if (this.isZero()) {
	    return '0';
	  }

	  if (this.isNegative()) {
	    if (this.equals(Timestamp.MIN_VALUE)) {
	      // We need to change the Timestamp value before it can be negated, so we remove
	      // the bottom-most digit in this base and then recurse to do the rest.
	      var radixTimestamp = Timestamp.fromNumber(radix);
	      var div = this.div(radixTimestamp);
	      var rem = div.multiply(radixTimestamp).subtract(this);
	      return div.toString(radix) + rem.toInt().toString(radix);
	    } else {
	      return '-' + this.negate().toString(radix);
	    }
	  }

	  // Do several (6) digits each time through the loop, so as to
	  // minimize the calls to the very expensive emulated div.
	  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 6));

	  rem = this;
	  var result = '';

	  while (!rem.isZero()) {
	    var remDiv = rem.div(radixToPower);
	    var intval = rem.subtract(remDiv.multiply(radixToPower)).toInt();
	    var digits = intval.toString(radix);

	    rem = remDiv;
	    if (rem.isZero()) {
	      return digits + result;
	    } else {
	      while (digits.length < 6) {
	        digits = '0' + digits;
	      }
	      result = '' + digits + result;
	    }
	  }
	};

	/**
	 * Return the high 32-bits value.
	 *
	 * @method
	 * @return {number} the high 32-bits as a signed value.
	 */
	Timestamp.prototype.getHighBits = function () {
	  return this.high_;
	};

	/**
	 * Return the low 32-bits value.
	 *
	 * @method
	 * @return {number} the low 32-bits as a signed value.
	 */
	Timestamp.prototype.getLowBits = function () {
	  return this.low_;
	};

	/**
	 * Return the low unsigned 32-bits value.
	 *
	 * @method
	 * @return {number} the low 32-bits as an unsigned value.
	 */
	Timestamp.prototype.getLowBitsUnsigned = function () {
	  return this.low_ >= 0 ? this.low_ : Timestamp.TWO_PWR_32_DBL_ + this.low_;
	};

	/**
	 * Returns the number of bits needed to represent the absolute value of this Timestamp.
	 *
	 * @method
	 * @return {number} Returns the number of bits needed to represent the absolute value of this Timestamp.
	 */
	Timestamp.prototype.getNumBitsAbs = function () {
	  if (this.isNegative()) {
	    if (this.equals(Timestamp.MIN_VALUE)) {
	      return 64;
	    } else {
	      return this.negate().getNumBitsAbs();
	    }
	  } else {
	    var val = this.high_ !== 0 ? this.high_ : this.low_;
	    for (var bit = 31; bit > 0; bit--) {
	      if ((val & 1 << bit) !== 0) {
	        break;
	      }
	    }
	    return this.high_ !== 0 ? bit + 33 : bit + 1;
	  }
	};

	/**
	 * Return whether this value is zero.
	 *
	 * @method
	 * @return {boolean} whether this value is zero.
	 */
	Timestamp.prototype.isZero = function () {
	  return this.high_ === 0 && this.low_ === 0;
	};

	/**
	 * Return whether this value is negative.
	 *
	 * @method
	 * @return {boolean} whether this value is negative.
	 */
	Timestamp.prototype.isNegative = function () {
	  return this.high_ < 0;
	};

	/**
	 * Return whether this value is odd.
	 *
	 * @method
	 * @return {boolean} whether this value is odd.
	 */
	Timestamp.prototype.isOdd = function () {
	  return (this.low_ & 1) === 1;
	};

	/**
	 * Return whether this Timestamp equals the other
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp equals the other
	 */
	Timestamp.prototype.equals = function (other) {
	  return this.high_ === other.high_ && this.low_ === other.low_;
	};

	/**
	 * Return whether this Timestamp does not equal the other.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp does not equal the other.
	 */
	Timestamp.prototype.notEquals = function (other) {
	  return this.high_ !== other.high_ || this.low_ !== other.low_;
	};

	/**
	 * Return whether this Timestamp is less than the other.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp is less than the other.
	 */
	Timestamp.prototype.lessThan = function (other) {
	  return this.compare(other) < 0;
	};

	/**
	 * Return whether this Timestamp is less than or equal to the other.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp is less than or equal to the other.
	 */
	Timestamp.prototype.lessThanOrEqual = function (other) {
	  return this.compare(other) <= 0;
	};

	/**
	 * Return whether this Timestamp is greater than the other.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp is greater than the other.
	 */
	Timestamp.prototype.greaterThan = function (other) {
	  return this.compare(other) > 0;
	};

	/**
	 * Return whether this Timestamp is greater than or equal to the other.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} whether this Timestamp is greater than or equal to the other.
	 */
	Timestamp.prototype.greaterThanOrEqual = function (other) {
	  return this.compare(other) >= 0;
	};

	/**
	 * Compares this Timestamp with the given one.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to compare against.
	 * @return {boolean} 0 if they are the same, 1 if the this is greater, and -1 if the given one is greater.
	 */
	Timestamp.prototype.compare = function (other) {
	  if (this.equals(other)) {
	    return 0;
	  }

	  var thisNeg = this.isNegative();
	  var otherNeg = other.isNegative();
	  if (thisNeg && !otherNeg) {
	    return -1;
	  }
	  if (!thisNeg && otherNeg) {
	    return 1;
	  }

	  // at this point, the signs are the same, so subtraction will not overflow
	  if (this.subtract(other).isNegative()) {
	    return -1;
	  } else {
	    return 1;
	  }
	};

	/**
	 * The negation of this value.
	 *
	 * @method
	 * @return {Timestamp} the negation of this value.
	 */
	Timestamp.prototype.negate = function () {
	  if (this.equals(Timestamp.MIN_VALUE)) {
	    return Timestamp.MIN_VALUE;
	  } else {
	    return this.not().add(Timestamp.ONE);
	  }
	};

	/**
	 * Returns the sum of this and the given Timestamp.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to add to this one.
	 * @return {Timestamp} the sum of this and the given Timestamp.
	 */
	Timestamp.prototype.add = function (other) {
	  // Divide each number into 4 chunks of 16 bits, and then sum the chunks.

	  var a48 = this.high_ >>> 16;
	  var a32 = this.high_ & 0xffff;
	  var a16 = this.low_ >>> 16;
	  var a00 = this.low_ & 0xffff;

	  var b48 = other.high_ >>> 16;
	  var b32 = other.high_ & 0xffff;
	  var b16 = other.low_ >>> 16;
	  var b00 = other.low_ & 0xffff;

	  var c48 = 0,
	      c32 = 0,
	      c16 = 0,
	      c00 = 0;
	  c00 += a00 + b00;
	  c16 += c00 >>> 16;
	  c00 &= 0xffff;
	  c16 += a16 + b16;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c32 += a32 + b32;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c48 += a48 + b48;
	  c48 &= 0xffff;
	  return Timestamp.fromBits(c16 << 16 | c00, c48 << 16 | c32);
	};

	/**
	 * Returns the difference of this and the given Timestamp.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to subtract from this.
	 * @return {Timestamp} the difference of this and the given Timestamp.
	 */
	Timestamp.prototype.subtract = function (other) {
	  return this.add(other.negate());
	};

	/**
	 * Returns the product of this and the given Timestamp.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp to multiply with this.
	 * @return {Timestamp} the product of this and the other.
	 */
	Timestamp.prototype.multiply = function (other) {
	  if (this.isZero()) {
	    return Timestamp.ZERO;
	  } else if (other.isZero()) {
	    return Timestamp.ZERO;
	  }

	  if (this.equals(Timestamp.MIN_VALUE)) {
	    return other.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
	  } else if (other.equals(Timestamp.MIN_VALUE)) {
	    return this.isOdd() ? Timestamp.MIN_VALUE : Timestamp.ZERO;
	  }

	  if (this.isNegative()) {
	    if (other.isNegative()) {
	      return this.negate().multiply(other.negate());
	    } else {
	      return this.negate().multiply(other).negate();
	    }
	  } else if (other.isNegative()) {
	    return this.multiply(other.negate()).negate();
	  }

	  // If both Timestamps are small, use float multiplication
	  if (this.lessThan(Timestamp.TWO_PWR_24_) && other.lessThan(Timestamp.TWO_PWR_24_)) {
	    return Timestamp.fromNumber(this.toNumber() * other.toNumber());
	  }

	  // Divide each Timestamp into 4 chunks of 16 bits, and then add up 4x4 products.
	  // We can skip products that would overflow.

	  var a48 = this.high_ >>> 16;
	  var a32 = this.high_ & 0xffff;
	  var a16 = this.low_ >>> 16;
	  var a00 = this.low_ & 0xffff;

	  var b48 = other.high_ >>> 16;
	  var b32 = other.high_ & 0xffff;
	  var b16 = other.low_ >>> 16;
	  var b00 = other.low_ & 0xffff;

	  var c48 = 0,
	      c32 = 0,
	      c16 = 0,
	      c00 = 0;
	  c00 += a00 * b00;
	  c16 += c00 >>> 16;
	  c00 &= 0xffff;
	  c16 += a16 * b00;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c16 += a00 * b16;
	  c32 += c16 >>> 16;
	  c16 &= 0xffff;
	  c32 += a32 * b00;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c32 += a16 * b16;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c32 += a00 * b32;
	  c48 += c32 >>> 16;
	  c32 &= 0xffff;
	  c48 += a48 * b00 + a32 * b16 + a16 * b32 + a00 * b48;
	  c48 &= 0xffff;
	  return Timestamp.fromBits(c16 << 16 | c00, c48 << 16 | c32);
	};

	/**
	 * Returns this Timestamp divided by the given one.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp by which to divide.
	 * @return {Timestamp} this Timestamp divided by the given one.
	 */
	Timestamp.prototype.div = function (other) {
	  if (other.isZero()) {
	    throw Error('division by zero');
	  } else if (this.isZero()) {
	    return Timestamp.ZERO;
	  }

	  if (this.equals(Timestamp.MIN_VALUE)) {
	    if (other.equals(Timestamp.ONE) || other.equals(Timestamp.NEG_ONE)) {
	      return Timestamp.MIN_VALUE; // recall that -MIN_VALUE == MIN_VALUE
	    } else if (other.equals(Timestamp.MIN_VALUE)) {
	      return Timestamp.ONE;
	    } else {
	      // At this point, we have |other| >= 2, so |this/other| < |MIN_VALUE|.
	      var halfThis = this.shiftRight(1);
	      var approx = halfThis.div(other).shiftLeft(1);
	      if (approx.equals(Timestamp.ZERO)) {
	        return other.isNegative() ? Timestamp.ONE : Timestamp.NEG_ONE;
	      } else {
	        var rem = this.subtract(other.multiply(approx));
	        var result = approx.add(rem.div(other));
	        return result;
	      }
	    }
	  } else if (other.equals(Timestamp.MIN_VALUE)) {
	    return Timestamp.ZERO;
	  }

	  if (this.isNegative()) {
	    if (other.isNegative()) {
	      return this.negate().div(other.negate());
	    } else {
	      return this.negate().div(other).negate();
	    }
	  } else if (other.isNegative()) {
	    return this.div(other.negate()).negate();
	  }

	  // Repeat the following until the remainder is less than other:  find a
	  // floating-point that approximates remainder / other *from below*, add this
	  // into the result, and subtract it from the remainder.  It is critical that
	  // the approximate value is less than or equal to the real value so that the
	  // remainder never becomes negative.
	  var res = Timestamp.ZERO;
	  rem = this;
	  while (rem.greaterThanOrEqual(other)) {
	    // Approximate the result of division. This may be a little greater or
	    // smaller than the actual value.
	    approx = Math.max(1, Math.floor(rem.toNumber() / other.toNumber()));

	    // We will tweak the approximate result by changing it in the 48-th digit or
	    // the smallest non-fractional digit, whichever is larger.
	    var log2 = Math.ceil(Math.log(approx) / Math.LN2);
	    var delta = log2 <= 48 ? 1 : Math.pow(2, log2 - 48);

	    // Decrease the approximation until it is smaller than the remainder.  Note
	    // that if it is too large, the product overflows and is negative.
	    var approxRes = Timestamp.fromNumber(approx);
	    var approxRem = approxRes.multiply(other);
	    while (approxRem.isNegative() || approxRem.greaterThan(rem)) {
	      approx -= delta;
	      approxRes = Timestamp.fromNumber(approx);
	      approxRem = approxRes.multiply(other);
	    }

	    // We know the answer can't be zero... and actually, zero would cause
	    // infinite recursion since we would make no progress.
	    if (approxRes.isZero()) {
	      approxRes = Timestamp.ONE;
	    }

	    res = res.add(approxRes);
	    rem = rem.subtract(approxRem);
	  }
	  return res;
	};

	/**
	 * Returns this Timestamp modulo the given one.
	 *
	 * @method
	 * @param {Timestamp} other Timestamp by which to mod.
	 * @return {Timestamp} this Timestamp modulo the given one.
	 */
	Timestamp.prototype.modulo = function (other) {
	  return this.subtract(this.div(other).multiply(other));
	};

	/**
	 * The bitwise-NOT of this value.
	 *
	 * @method
	 * @return {Timestamp} the bitwise-NOT of this value.
	 */
	Timestamp.prototype.not = function () {
	  return Timestamp.fromBits(~this.low_, ~this.high_);
	};

	/**
	 * Returns the bitwise-AND of this Timestamp and the given one.
	 *
	 * @method
	 * @param {Timestamp} other the Timestamp with which to AND.
	 * @return {Timestamp} the bitwise-AND of this and the other.
	 */
	Timestamp.prototype.and = function (other) {
	  return Timestamp.fromBits(this.low_ & other.low_, this.high_ & other.high_);
	};

	/**
	 * Returns the bitwise-OR of this Timestamp and the given one.
	 *
	 * @method
	 * @param {Timestamp} other the Timestamp with which to OR.
	 * @return {Timestamp} the bitwise-OR of this and the other.
	 */
	Timestamp.prototype.or = function (other) {
	  return Timestamp.fromBits(this.low_ | other.low_, this.high_ | other.high_);
	};

	/**
	 * Returns the bitwise-XOR of this Timestamp and the given one.
	 *
	 * @method
	 * @param {Timestamp} other the Timestamp with which to XOR.
	 * @return {Timestamp} the bitwise-XOR of this and the other.
	 */
	Timestamp.prototype.xor = function (other) {
	  return Timestamp.fromBits(this.low_ ^ other.low_, this.high_ ^ other.high_);
	};

	/**
	 * Returns this Timestamp with bits shifted to the left by the given amount.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Timestamp} this shifted to the left by the given amount.
	 */
	Timestamp.prototype.shiftLeft = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var low = this.low_;
	    if (numBits < 32) {
	      var high = this.high_;
	      return Timestamp.fromBits(low << numBits, high << numBits | low >>> 32 - numBits);
	    } else {
	      return Timestamp.fromBits(0, low << numBits - 32);
	    }
	  }
	};

	/**
	 * Returns this Timestamp with bits shifted to the right by the given amount.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Timestamp} this shifted to the right by the given amount.
	 */
	Timestamp.prototype.shiftRight = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var high = this.high_;
	    if (numBits < 32) {
	      var low = this.low_;
	      return Timestamp.fromBits(low >>> numBits | high << 32 - numBits, high >> numBits);
	    } else {
	      return Timestamp.fromBits(high >> numBits - 32, high >= 0 ? 0 : -1);
	    }
	  }
	};

	/**
	 * Returns this Timestamp with bits shifted to the right by the given amount, with the new top bits matching the current sign bit.
	 *
	 * @method
	 * @param {number} numBits the number of bits by which to shift.
	 * @return {Timestamp} this shifted to the right by the given amount, with zeros placed into the new leading bits.
	 */
	Timestamp.prototype.shiftRightUnsigned = function (numBits) {
	  numBits &= 63;
	  if (numBits === 0) {
	    return this;
	  } else {
	    var high = this.high_;
	    if (numBits < 32) {
	      var low = this.low_;
	      return Timestamp.fromBits(low >>> numBits | high << 32 - numBits, high >>> numBits);
	    } else if (numBits === 32) {
	      return Timestamp.fromBits(high, 0);
	    } else {
	      return Timestamp.fromBits(high >>> numBits - 32, 0);
	    }
	  }
	};

	/**
	 * Returns a Timestamp representing the given (32-bit) integer value.
	 *
	 * @method
	 * @param {number} value the 32-bit integer in question.
	 * @return {Timestamp} the corresponding Timestamp value.
	 */
	Timestamp.fromInt = function (value) {
	  if (-128 <= value && value < 128) {
	    var cachedObj = Timestamp.INT_CACHE_[value];
	    if (cachedObj) {
	      return cachedObj;
	    }
	  }

	  var obj = new Timestamp(value | 0, value < 0 ? -1 : 0);
	  if (-128 <= value && value < 128) {
	    Timestamp.INT_CACHE_[value] = obj;
	  }
	  return obj;
	};

	/**
	 * Returns a Timestamp representing the given value, provided that it is a finite number. Otherwise, zero is returned.
	 *
	 * @method
	 * @param {number} value the number in question.
	 * @return {Timestamp} the corresponding Timestamp value.
	 */
	Timestamp.fromNumber = function (value) {
	  if (isNaN(value) || !isFinite(value)) {
	    return Timestamp.ZERO;
	  } else if (value <= -Timestamp.TWO_PWR_63_DBL_) {
	    return Timestamp.MIN_VALUE;
	  } else if (value + 1 >= Timestamp.TWO_PWR_63_DBL_) {
	    return Timestamp.MAX_VALUE;
	  } else if (value < 0) {
	    return Timestamp.fromNumber(-value).negate();
	  } else {
	    return new Timestamp(value % Timestamp.TWO_PWR_32_DBL_ | 0, value / Timestamp.TWO_PWR_32_DBL_ | 0);
	  }
	};

	/**
	 * Returns a Timestamp representing the 64-bit integer that comes by concatenating the given high and low bits. Each is assumed to use 32 bits.
	 *
	 * @method
	 * @param {number} lowBits the low 32-bits.
	 * @param {number} highBits the high 32-bits.
	 * @return {Timestamp} the corresponding Timestamp value.
	 */
	Timestamp.fromBits = function (lowBits, highBits) {
	  return new Timestamp(lowBits, highBits);
	};

	/**
	 * Returns a Timestamp representation of the given string, written using the given radix.
	 *
	 * @method
	 * @param {string} str the textual representation of the Timestamp.
	 * @param {number} opt_radix the radix in which the text is written.
	 * @return {Timestamp} the corresponding Timestamp value.
	 */
	Timestamp.fromString = function (str, opt_radix) {
	  if (str.length === 0) {
	    throw Error('number format error: empty string');
	  }

	  var radix = opt_radix || 10;
	  if (radix < 2 || 36 < radix) {
	    throw Error('radix out of range: ' + radix);
	  }

	  if (str.charAt(0) === '-') {
	    return Timestamp.fromString(str.substring(1), radix).negate();
	  } else if (str.indexOf('-') >= 0) {
	    throw Error('number format error: interior "-" character: ' + str);
	  }

	  // Do several (8) digits each time through the loop, so as to
	  // minimize the calls to the very expensive emulated div.
	  var radixToPower = Timestamp.fromNumber(Math.pow(radix, 8));

	  var result = Timestamp.ZERO;
	  for (var i = 0; i < str.length; i += 8) {
	    var size = Math.min(8, str.length - i);
	    var value = parseInt(str.substring(i, i + size), radix);
	    if (size < 8) {
	      var power = Timestamp.fromNumber(Math.pow(radix, size));
	      result = result.multiply(power).add(Timestamp.fromNumber(value));
	    } else {
	      result = result.multiply(radixToPower);
	      result = result.add(Timestamp.fromNumber(value));
	    }
	  }
	  return result;
	};

	// NOTE: Common constant values ZERO, ONE, NEG_ONE, etc. are defined below the
	// from* methods on which they depend.

	/**
	 * A cache of the Timestamp representations of small integer values.
	 * @type {Object}
	 * @ignore
	 */
	Timestamp.INT_CACHE_ = {};

	// NOTE: the compiler should inline these constant values below and then remove
	// these variables, so there should be no runtime penalty for these.

	/**
	 * Number used repeated below in calculations.  This must appear before the
	 * first call to any from* function below.
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_16_DBL_ = 1 << 16;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_24_DBL_ = 1 << 24;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_32_DBL_ = Timestamp.TWO_PWR_16_DBL_ * Timestamp.TWO_PWR_16_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_31_DBL_ = Timestamp.TWO_PWR_32_DBL_ / 2;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_48_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_16_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_64_DBL_ = Timestamp.TWO_PWR_32_DBL_ * Timestamp.TWO_PWR_32_DBL_;

	/**
	 * @type {number}
	 * @ignore
	 */
	Timestamp.TWO_PWR_63_DBL_ = Timestamp.TWO_PWR_64_DBL_ / 2;

	/** @type {Timestamp} */
	Timestamp.ZERO = Timestamp.fromInt(0);

	/** @type {Timestamp} */
	Timestamp.ONE = Timestamp.fromInt(1);

	/** @type {Timestamp} */
	Timestamp.NEG_ONE = Timestamp.fromInt(-1);

	/** @type {Timestamp} */
	Timestamp.MAX_VALUE = Timestamp.fromBits(0xffffffff | 0, 0x7fffffff | 0);

	/** @type {Timestamp} */
	Timestamp.MIN_VALUE = Timestamp.fromBits(0, 0x80000000 | 0);

	/**
	 * @type {Timestamp}
	 * @ignore
	 */
	Timestamp.TWO_PWR_24_ = Timestamp.fromInt(1 << 24);

	/**
	 * Expose.
	 */
	module.exports = Timestamp;
	module.exports.Timestamp = Timestamp;

/***/ }),
/* 338 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer, process) {// Custom inspect property name / symbol.
	var inspect = 'inspect';

	var utils = __webpack_require__(344);

	/**
	 * Machine id.
	 *
	 * Create a random 3-byte value (i.e. unique for this
	 * process). Other drivers use a md5 of the machine id here, but
	 * that would mean an asyc call to gethostname, so we don't bother.
	 * @ignore
	 */
	var MACHINE_ID = parseInt(Math.random() * 0xffffff, 10);

	// Regular expression that checks for hex value
	var checkForHexRegExp = new RegExp('^[0-9a-fA-F]{24}$');

	// Check if buffer exists
	try {
	  if (Buffer && Buffer.from) {
	    var hasBufferType = true;
	    inspect = __webpack_require__(345).inspect.custom || 'inspect';
	  }
	} catch (err) {
	  hasBufferType = false;
	}

	/**
	* Create a new ObjectID instance
	*
	* @class
	* @param {(string|number)} id Can be a 24 byte hex string, 12 byte binary string or a Number.
	* @property {number} generationTime The generation time of this ObjectId instance
	* @return {ObjectID} instance of ObjectID.
	*/
	var ObjectID = function ObjectID(id) {
	  // Duck-typing to support ObjectId from different npm packages
	  if (id instanceof ObjectID) return id;
	  if (!(this instanceof ObjectID)) return new ObjectID(id);

	  this._bsontype = 'ObjectID';

	  // The most common usecase (blank id, new objectId instance)
	  if (id == null || typeof id === 'number') {
	    // Generate a new id
	    this.id = this.generate(id);
	    // If we are caching the hex string
	    if (ObjectID.cacheHexString) this.__id = this.toString('hex');
	    // Return the object
	    return;
	  }

	  // Check if the passed in id is valid
	  var valid = ObjectID.isValid(id);

	  // Throw an error if it's not a valid setup
	  if (!valid && id != null) {
	    throw new Error('Argument passed in must be a single String of 12 bytes or a string of 24 hex characters');
	  } else if (valid && typeof id === 'string' && id.length === 24 && hasBufferType) {
	    return new ObjectID(utils.toBuffer(id, 'hex'));
	  } else if (valid && typeof id === 'string' && id.length === 24) {
	    return ObjectID.createFromHexString(id);
	  } else if (id != null && id.length === 12) {
	    // assume 12 byte string
	    this.id = id;
	  } else if (id != null && id.toHexString) {
	    // Duck-typing to support ObjectId from different npm packages
	    return id;
	  } else {
	    throw new Error('Argument passed in must be a single String of 12 bytes or a string of 24 hex characters');
	  }

	  if (ObjectID.cacheHexString) this.__id = this.toString('hex');
	};

	// Allow usage of ObjectId as well as ObjectID
	// var ObjectId = ObjectID;

	// Precomputed hex table enables speedy hex string conversion
	var hexTable = [];
	for (var i = 0; i < 256; i++) {
	  hexTable[i] = (i <= 15 ? '0' : '') + i.toString(16);
	}

	/**
	* Return the ObjectID id as a 24 byte hex string representation
	*
	* @method
	* @return {string} return the 24 byte hex string representation.
	*/
	ObjectID.prototype.toHexString = function () {
	  if (ObjectID.cacheHexString && this.__id) return this.__id;

	  var hexString = '';
	  if (!this.id || !this.id.length) {
	    throw new Error('invalid ObjectId, ObjectId.id must be either a string or a Buffer, but is [' + JSON.stringify(this.id) + ']');
	  }

	  if (this.id instanceof _Buffer) {
	    hexString = convertToHex(this.id);
	    if (ObjectID.cacheHexString) this.__id = hexString;
	    return hexString;
	  }

	  for (var i = 0; i < this.id.length; i++) {
	    hexString += hexTable[this.id.charCodeAt(i)];
	  }

	  if (ObjectID.cacheHexString) this.__id = hexString;
	  return hexString;
	};

	/**
	* Update the ObjectID index used in generating new ObjectID's on the driver
	*
	* @method
	* @return {number} returns next index value.
	* @ignore
	*/
	ObjectID.prototype.get_inc = function () {
	  return ObjectID.index = (ObjectID.index + 1) % 0xffffff;
	};

	/**
	* Update the ObjectID index used in generating new ObjectID's on the driver
	*
	* @method
	* @return {number} returns next index value.
	* @ignore
	*/
	ObjectID.prototype.getInc = function () {
	  return this.get_inc();
	};

	/**
	* Generate a 12 byte id buffer used in ObjectID's
	*
	* @method
	* @param {number} [time] optional parameter allowing to pass in a second based timestamp.
	* @return {Buffer} return the 12 byte id buffer string.
	*/
	ObjectID.prototype.generate = function (time) {
	  if ('number' !== typeof time) {
	    time = ~~(Date.now() / 1000);
	  }

	  // Use pid
	  var pid = (typeof process === 'undefined' || process.pid === 1 ? Math.floor(Math.random() * 100000) : process.pid) % 0xffff;
	  var inc = this.get_inc();
	  // Buffer used
	  var buffer = utils.allocBuffer(12);
	  // Encode time
	  buffer[3] = time & 0xff;
	  buffer[2] = time >> 8 & 0xff;
	  buffer[1] = time >> 16 & 0xff;
	  buffer[0] = time >> 24 & 0xff;
	  // Encode machine
	  buffer[6] = MACHINE_ID & 0xff;
	  buffer[5] = MACHINE_ID >> 8 & 0xff;
	  buffer[4] = MACHINE_ID >> 16 & 0xff;
	  // Encode pid
	  buffer[8] = pid & 0xff;
	  buffer[7] = pid >> 8 & 0xff;
	  // Encode index
	  buffer[11] = inc & 0xff;
	  buffer[10] = inc >> 8 & 0xff;
	  buffer[9] = inc >> 16 & 0xff;
	  // Return the buffer
	  return buffer;
	};

	/**
	* Converts the id into a 24 byte hex string for printing
	*
	* @param {String} format The Buffer toString format parameter.
	* @return {String} return the 24 byte hex string representation.
	* @ignore
	*/
	ObjectID.prototype.toString = function (format) {
	  // Is the id a buffer then use the buffer toString method to return the format
	  if (this.id && this.id.copy) {
	    return this.id.toString(typeof format === 'string' ? format : 'hex');
	  }

	  // if(this.buffer )
	  return this.toHexString();
	};

	/**
	* Converts to a string representation of this Id.
	*
	* @return {String} return the 24 byte hex string representation.
	* @ignore
	*/
	ObjectID.prototype[inspect] = ObjectID.prototype.toString;

	/**
	* Converts to its JSON representation.
	*
	* @return {String} return the 24 byte hex string representation.
	* @ignore
	*/
	ObjectID.prototype.toJSON = function () {
	  return this.toHexString();
	};

	/**
	* Compares the equality of this ObjectID with `otherID`.
	*
	* @method
	* @param {object} otherID ObjectID instance to compare against.
	* @return {boolean} the result of comparing two ObjectID's
	*/
	ObjectID.prototype.equals = function equals(otherId) {
	  // var id;

	  if (otherId instanceof ObjectID) {
	    return this.toString() === otherId.toString();
	  } else if (typeof otherId === 'string' && ObjectID.isValid(otherId) && otherId.length === 12 && this.id instanceof _Buffer) {
	    return otherId === this.id.toString('binary');
	  } else if (typeof otherId === 'string' && ObjectID.isValid(otherId) && otherId.length === 24) {
	    return otherId.toLowerCase() === this.toHexString();
	  } else if (typeof otherId === 'string' && ObjectID.isValid(otherId) && otherId.length === 12) {
	    return otherId === this.id;
	  } else if (otherId != null && (otherId instanceof ObjectID || otherId.toHexString)) {
	    return otherId.toHexString() === this.toHexString();
	  } else {
	    return false;
	  }
	};

	/**
	* Returns the generation date (accurate up to the second) that this ID was generated.
	*
	* @method
	* @return {date} the generation date
	*/
	ObjectID.prototype.getTimestamp = function () {
	  var timestamp = new Date();
	  var time = this.id[3] | this.id[2] << 8 | this.id[1] << 16 | this.id[0] << 24;
	  timestamp.setTime(Math.floor(time) * 1000);
	  return timestamp;
	};

	/**
	* @ignore
	*/
	ObjectID.index = ~~(Math.random() * 0xffffff);

	/**
	* @ignore
	*/
	ObjectID.createPk = function createPk() {
	  return new ObjectID();
	};

	/**
	* Creates an ObjectID from a second based number, with the rest of the ObjectID zeroed out. Used for comparisons or sorting the ObjectID.
	*
	* @method
	* @param {number} time an integer number representing a number of seconds.
	* @return {ObjectID} return the created ObjectID
	*/
	ObjectID.createFromTime = function createFromTime(time) {
	  var buffer = utils.toBuffer([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
	  // Encode time into first 4 bytes
	  buffer[3] = time & 0xff;
	  buffer[2] = time >> 8 & 0xff;
	  buffer[1] = time >> 16 & 0xff;
	  buffer[0] = time >> 24 & 0xff;
	  // Return the new objectId
	  return new ObjectID(buffer);
	};

	// Lookup tables
	//var encodeLookup = '0123456789abcdef'.split('');
	var decodeLookup = [];
	i = 0;
	while (i < 10) decodeLookup[0x30 + i] = i++;
	while (i < 16) decodeLookup[0x41 - 10 + i] = decodeLookup[0x61 - 10 + i] = i++;

	var _Buffer = Buffer;
	var convertToHex = function (bytes) {
	  return bytes.toString('hex');
	};

	/**
	* Creates an ObjectID from a hex string representation of an ObjectID.
	*
	* @method
	* @param {string} hexString create a ObjectID from a passed in 24 byte hexstring.
	* @return {ObjectID} return the created ObjectID
	*/
	ObjectID.createFromHexString = function createFromHexString(string) {
	  // Throw an error if it's not a valid setup
	  if (typeof string === 'undefined' || string != null && string.length !== 24) {
	    throw new Error('Argument passed in must be a single String of 12 bytes or a string of 24 hex characters');
	  }

	  // Use Buffer.from method if available
	  if (hasBufferType) return new ObjectID(utils.toBuffer(string, 'hex'));

	  // Calculate lengths
	  var array = new _Buffer(12);
	  var n = 0;
	  var i = 0;

	  while (i < 24) {
	    array[n++] = decodeLookup[string.charCodeAt(i++)] << 4 | decodeLookup[string.charCodeAt(i++)];
	  }

	  return new ObjectID(array);
	};

	/**
	* Checks if a value is a valid bson ObjectId
	*
	* @method
	* @return {boolean} return true if the value is a valid bson ObjectId, return false otherwise.
	*/
	ObjectID.isValid = function isValid(id) {
	  if (id == null) return false;

	  if (typeof id === 'number') {
	    return true;
	  }

	  if (typeof id === 'string') {
	    return id.length === 12 || id.length === 24 && checkForHexRegExp.test(id);
	  }

	  if (id instanceof ObjectID) {
	    return true;
	  }

	  if (id instanceof _Buffer) {
	    return true;
	  }

	  // Duck-Typing detection of ObjectId like objects
	  if (id.toHexString) {
	    return id.id.length === 12 || id.id.length === 24 && checkForHexRegExp.test(id.id);
	  }

	  return false;
	};

	/**
	* @ignore
	*/
	Object.defineProperty(ObjectID.prototype, 'generationTime', {
	  enumerable: true,
	  get: function () {
	    return this.id[3] | this.id[2] << 8 | this.id[1] << 16 | this.id[0] << 24;
	  },
	  set: function (value) {
	    // Encode time into first 4 bytes
	    this.id[3] = value & 0xff;
	    this.id[2] = value >> 8 & 0xff;
	    this.id[1] = value >> 16 & 0xff;
	    this.id[0] = value >> 24 & 0xff;
	  }
	});

	/**
	 * Expose.
	 */
	module.exports = ObjectID;
	module.exports.ObjectID = ObjectID;
	module.exports.ObjectId = ObjectID;
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(339).Buffer, __webpack_require__(343)))

/***/ }),
/* 339 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {/*!
	 * The buffer module from node.js, for the browser.
	 *
	 * @author   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
	 * @license  MIT
	 */
	/* eslint-disable no-proto */

	'use strict'

	var base64 = __webpack_require__(340)
	var ieee754 = __webpack_require__(341)
	var isArray = __webpack_require__(342)

	exports.Buffer = Buffer
	exports.SlowBuffer = SlowBuffer
	exports.INSPECT_MAX_BYTES = 50

	/**
	 * If `Buffer.TYPED_ARRAY_SUPPORT`:
	 *   === true    Use Uint8Array implementation (fastest)
	 *   === false   Use Object implementation (most compatible, even IE6)
	 *
	 * Browsers that support typed arrays are IE 10+, Firefox 4+, Chrome 7+, Safari 5.1+,
	 * Opera 11.6+, iOS 4.2+.
	 *
	 * Due to various browser bugs, sometimes the Object implementation will be used even
	 * when the browser supports typed arrays.
	 *
	 * Note:
	 *
	 *   - Firefox 4-29 lacks support for adding new properties to `Uint8Array` instances,
	 *     See: https://bugzilla.mozilla.org/show_bug.cgi?id=695438.
	 *
	 *   - Chrome 9-10 is missing the `TypedArray.prototype.subarray` function.
	 *
	 *   - IE10 has a broken `TypedArray.prototype.subarray` function which returns arrays of
	 *     incorrect length in some situations.

	 * We detect these buggy browsers and set `Buffer.TYPED_ARRAY_SUPPORT` to `false` so they
	 * get the Object implementation, which is slower but behaves correctly.
	 */
	Buffer.TYPED_ARRAY_SUPPORT = global.TYPED_ARRAY_SUPPORT !== undefined
	  ? global.TYPED_ARRAY_SUPPORT
	  : typedArraySupport()

	/*
	 * Export kMaxLength after typed array support is determined.
	 */
	exports.kMaxLength = kMaxLength()

	function typedArraySupport () {
	  try {
	    var arr = new Uint8Array(1)
	    arr.__proto__ = {__proto__: Uint8Array.prototype, foo: function () { return 42 }}
	    return arr.foo() === 42 && // typed array instances can be augmented
	        typeof arr.subarray === 'function' && // chrome 9-10 lack `subarray`
	        arr.subarray(1, 1).byteLength === 0 // ie10 has broken `subarray`
	  } catch (e) {
	    return false
	  }
	}

	function kMaxLength () {
	  return Buffer.TYPED_ARRAY_SUPPORT
	    ? 0x7fffffff
	    : 0x3fffffff
	}

	function createBuffer (that, length) {
	  if (kMaxLength() < length) {
	    throw new RangeError('Invalid typed array length')
	  }
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    // Return an augmented `Uint8Array` instance, for best performance
	    that = new Uint8Array(length)
	    that.__proto__ = Buffer.prototype
	  } else {
	    // Fallback: Return an object instance of the Buffer class
	    if (that === null) {
	      that = new Buffer(length)
	    }
	    that.length = length
	  }

	  return that
	}

	/**
	 * The Buffer constructor returns instances of `Uint8Array` that have their
	 * prototype changed to `Buffer.prototype`. Furthermore, `Buffer` is a subclass of
	 * `Uint8Array`, so the returned instances will have all the node `Buffer` methods
	 * and the `Uint8Array` methods. Square bracket notation works as expected -- it
	 * returns a single octet.
	 *
	 * The `Uint8Array` prototype remains unmodified.
	 */

	function Buffer (arg, encodingOrOffset, length) {
	  if (!Buffer.TYPED_ARRAY_SUPPORT && !(this instanceof Buffer)) {
	    return new Buffer(arg, encodingOrOffset, length)
	  }

	  // Common case.
	  if (typeof arg === 'number') {
	    if (typeof encodingOrOffset === 'string') {
	      throw new Error(
	        'If encoding is specified then the first argument must be a string'
	      )
	    }
	    return allocUnsafe(this, arg)
	  }
	  return from(this, arg, encodingOrOffset, length)
	}

	Buffer.poolSize = 8192 // not used by this implementation

	// TODO: Legacy, not needed anymore. Remove in next major version.
	Buffer._augment = function (arr) {
	  arr.__proto__ = Buffer.prototype
	  return arr
	}

	function from (that, value, encodingOrOffset, length) {
	  if (typeof value === 'number') {
	    throw new TypeError('"value" argument must not be a number')
	  }

	  if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
	    return fromArrayBuffer(that, value, encodingOrOffset, length)
	  }

	  if (typeof value === 'string') {
	    return fromString(that, value, encodingOrOffset)
	  }

	  return fromObject(that, value)
	}

	/**
	 * Functionally equivalent to Buffer(arg, encoding) but throws a TypeError
	 * if value is a number.
	 * Buffer.from(str[, encoding])
	 * Buffer.from(array)
	 * Buffer.from(buffer)
	 * Buffer.from(arrayBuffer[, byteOffset[, length]])
	 **/
	Buffer.from = function (value, encodingOrOffset, length) {
	  return from(null, value, encodingOrOffset, length)
	}

	if (Buffer.TYPED_ARRAY_SUPPORT) {
	  Buffer.prototype.__proto__ = Uint8Array.prototype
	  Buffer.__proto__ = Uint8Array
	  if (typeof Symbol !== 'undefined' && Symbol.species &&
	      Buffer[Symbol.species] === Buffer) {
	    // Fix subarray() in ES2016. See: https://github.com/feross/buffer/pull/97
	    Object.defineProperty(Buffer, Symbol.species, {
	      value: null,
	      configurable: true
	    })
	  }
	}

	function assertSize (size) {
	  if (typeof size !== 'number') {
	    throw new TypeError('"size" argument must be a number')
	  } else if (size < 0) {
	    throw new RangeError('"size" argument must not be negative')
	  }
	}

	function alloc (that, size, fill, encoding) {
	  assertSize(size)
	  if (size <= 0) {
	    return createBuffer(that, size)
	  }
	  if (fill !== undefined) {
	    // Only pay attention to encoding if it's a string. This
	    // prevents accidentally sending in a number that would
	    // be interpretted as a start offset.
	    return typeof encoding === 'string'
	      ? createBuffer(that, size).fill(fill, encoding)
	      : createBuffer(that, size).fill(fill)
	  }
	  return createBuffer(that, size)
	}

	/**
	 * Creates a new filled Buffer instance.
	 * alloc(size[, fill[, encoding]])
	 **/
	Buffer.alloc = function (size, fill, encoding) {
	  return alloc(null, size, fill, encoding)
	}

	function allocUnsafe (that, size) {
	  assertSize(size)
	  that = createBuffer(that, size < 0 ? 0 : checked(size) | 0)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) {
	    for (var i = 0; i < size; ++i) {
	      that[i] = 0
	    }
	  }
	  return that
	}

	/**
	 * Equivalent to Buffer(num), by default creates a non-zero-filled Buffer instance.
	 * */
	Buffer.allocUnsafe = function (size) {
	  return allocUnsafe(null, size)
	}
	/**
	 * Equivalent to SlowBuffer(num), by default creates a non-zero-filled Buffer instance.
	 */
	Buffer.allocUnsafeSlow = function (size) {
	  return allocUnsafe(null, size)
	}

	function fromString (that, string, encoding) {
	  if (typeof encoding !== 'string' || encoding === '') {
	    encoding = 'utf8'
	  }

	  if (!Buffer.isEncoding(encoding)) {
	    throw new TypeError('"encoding" must be a valid string encoding')
	  }

	  var length = byteLength(string, encoding) | 0
	  that = createBuffer(that, length)

	  var actual = that.write(string, encoding)

	  if (actual !== length) {
	    // Writing a hex string, for example, that contains invalid characters will
	    // cause everything after the first invalid character to be ignored. (e.g.
	    // 'abxxcd' will be treated as 'ab')
	    that = that.slice(0, actual)
	  }

	  return that
	}

	function fromArrayLike (that, array) {
	  var length = array.length < 0 ? 0 : checked(array.length) | 0
	  that = createBuffer(that, length)
	  for (var i = 0; i < length; i += 1) {
	    that[i] = array[i] & 255
	  }
	  return that
	}

	function fromArrayBuffer (that, array, byteOffset, length) {
	  array.byteLength // this throws if `array` is not a valid ArrayBuffer

	  if (byteOffset < 0 || array.byteLength < byteOffset) {
	    throw new RangeError('\'offset\' is out of bounds')
	  }

	  if (array.byteLength < byteOffset + (length || 0)) {
	    throw new RangeError('\'length\' is out of bounds')
	  }

	  if (byteOffset === undefined && length === undefined) {
	    array = new Uint8Array(array)
	  } else if (length === undefined) {
	    array = new Uint8Array(array, byteOffset)
	  } else {
	    array = new Uint8Array(array, byteOffset, length)
	  }

	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    // Return an augmented `Uint8Array` instance, for best performance
	    that = array
	    that.__proto__ = Buffer.prototype
	  } else {
	    // Fallback: Return an object instance of the Buffer class
	    that = fromArrayLike(that, array)
	  }
	  return that
	}

	function fromObject (that, obj) {
	  if (Buffer.isBuffer(obj)) {
	    var len = checked(obj.length) | 0
	    that = createBuffer(that, len)

	    if (that.length === 0) {
	      return that
	    }

	    obj.copy(that, 0, 0, len)
	    return that
	  }

	  if (obj) {
	    if ((typeof ArrayBuffer !== 'undefined' &&
	        obj.buffer instanceof ArrayBuffer) || 'length' in obj) {
	      if (typeof obj.length !== 'number' || isnan(obj.length)) {
	        return createBuffer(that, 0)
	      }
	      return fromArrayLike(that, obj)
	    }

	    if (obj.type === 'Buffer' && isArray(obj.data)) {
	      return fromArrayLike(that, obj.data)
	    }
	  }

	  throw new TypeError('First argument must be a string, Buffer, ArrayBuffer, Array, or array-like object.')
	}

	function checked (length) {
	  // Note: cannot use `length < kMaxLength()` here because that fails when
	  // length is NaN (which is otherwise coerced to zero.)
	  if (length >= kMaxLength()) {
	    throw new RangeError('Attempt to allocate Buffer larger than maximum ' +
	                         'size: 0x' + kMaxLength().toString(16) + ' bytes')
	  }
	  return length | 0
	}

	function SlowBuffer (length) {
	  if (+length != length) { // eslint-disable-line eqeqeq
	    length = 0
	  }
	  return Buffer.alloc(+length)
	}

	Buffer.isBuffer = function isBuffer (b) {
	  return !!(b != null && b._isBuffer)
	}

	Buffer.compare = function compare (a, b) {
	  if (!Buffer.isBuffer(a) || !Buffer.isBuffer(b)) {
	    throw new TypeError('Arguments must be Buffers')
	  }

	  if (a === b) return 0

	  var x = a.length
	  var y = b.length

	  for (var i = 0, len = Math.min(x, y); i < len; ++i) {
	    if (a[i] !== b[i]) {
	      x = a[i]
	      y = b[i]
	      break
	    }
	  }

	  if (x < y) return -1
	  if (y < x) return 1
	  return 0
	}

	Buffer.isEncoding = function isEncoding (encoding) {
	  switch (String(encoding).toLowerCase()) {
	    case 'hex':
	    case 'utf8':
	    case 'utf-8':
	    case 'ascii':
	    case 'latin1':
	    case 'binary':
	    case 'base64':
	    case 'ucs2':
	    case 'ucs-2':
	    case 'utf16le':
	    case 'utf-16le':
	      return true
	    default:
	      return false
	  }
	}

	Buffer.concat = function concat (list, length) {
	  if (!isArray(list)) {
	    throw new TypeError('"list" argument must be an Array of Buffers')
	  }

	  if (list.length === 0) {
	    return Buffer.alloc(0)
	  }

	  var i
	  if (length === undefined) {
	    length = 0
	    for (i = 0; i < list.length; ++i) {
	      length += list[i].length
	    }
	  }

	  var buffer = Buffer.allocUnsafe(length)
	  var pos = 0
	  for (i = 0; i < list.length; ++i) {
	    var buf = list[i]
	    if (!Buffer.isBuffer(buf)) {
	      throw new TypeError('"list" argument must be an Array of Buffers')
	    }
	    buf.copy(buffer, pos)
	    pos += buf.length
	  }
	  return buffer
	}

	function byteLength (string, encoding) {
	  if (Buffer.isBuffer(string)) {
	    return string.length
	  }
	  if (typeof ArrayBuffer !== 'undefined' && typeof ArrayBuffer.isView === 'function' &&
	      (ArrayBuffer.isView(string) || string instanceof ArrayBuffer)) {
	    return string.byteLength
	  }
	  if (typeof string !== 'string') {
	    string = '' + string
	  }

	  var len = string.length
	  if (len === 0) return 0

	  // Use a for loop to avoid recursion
	  var loweredCase = false
	  for (;;) {
	    switch (encoding) {
	      case 'ascii':
	      case 'latin1':
	      case 'binary':
	        return len
	      case 'utf8':
	      case 'utf-8':
	      case undefined:
	        return utf8ToBytes(string).length
	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return len * 2
	      case 'hex':
	        return len >>> 1
	      case 'base64':
	        return base64ToBytes(string).length
	      default:
	        if (loweredCase) return utf8ToBytes(string).length // assume utf8
	        encoding = ('' + encoding).toLowerCase()
	        loweredCase = true
	    }
	  }
	}
	Buffer.byteLength = byteLength

	function slowToString (encoding, start, end) {
	  var loweredCase = false

	  // No need to verify that "this.length <= MAX_UINT32" since it's a read-only
	  // property of a typed array.

	  // This behaves neither like String nor Uint8Array in that we set start/end
	  // to their upper/lower bounds if the value passed is out of range.
	  // undefined is handled specially as per ECMA-262 6th Edition,
	  // Section 13.3.3.7 Runtime Semantics: KeyedBindingInitialization.
	  if (start === undefined || start < 0) {
	    start = 0
	  }
	  // Return early if start > this.length. Done here to prevent potential uint32
	  // coercion fail below.
	  if (start > this.length) {
	    return ''
	  }

	  if (end === undefined || end > this.length) {
	    end = this.length
	  }

	  if (end <= 0) {
	    return ''
	  }

	  // Force coersion to uint32. This will also coerce falsey/NaN values to 0.
	  end >>>= 0
	  start >>>= 0

	  if (end <= start) {
	    return ''
	  }

	  if (!encoding) encoding = 'utf8'

	  while (true) {
	    switch (encoding) {
	      case 'hex':
	        return hexSlice(this, start, end)

	      case 'utf8':
	      case 'utf-8':
	        return utf8Slice(this, start, end)

	      case 'ascii':
	        return asciiSlice(this, start, end)

	      case 'latin1':
	      case 'binary':
	        return latin1Slice(this, start, end)

	      case 'base64':
	        return base64Slice(this, start, end)

	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return utf16leSlice(this, start, end)

	      default:
	        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
	        encoding = (encoding + '').toLowerCase()
	        loweredCase = true
	    }
	  }
	}

	// The property is used by `Buffer.isBuffer` and `is-buffer` (in Safari 5-7) to detect
	// Buffer instances.
	Buffer.prototype._isBuffer = true

	function swap (b, n, m) {
	  var i = b[n]
	  b[n] = b[m]
	  b[m] = i
	}

	Buffer.prototype.swap16 = function swap16 () {
	  var len = this.length
	  if (len % 2 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 16-bits')
	  }
	  for (var i = 0; i < len; i += 2) {
	    swap(this, i, i + 1)
	  }
	  return this
	}

	Buffer.prototype.swap32 = function swap32 () {
	  var len = this.length
	  if (len % 4 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 32-bits')
	  }
	  for (var i = 0; i < len; i += 4) {
	    swap(this, i, i + 3)
	    swap(this, i + 1, i + 2)
	  }
	  return this
	}

	Buffer.prototype.swap64 = function swap64 () {
	  var len = this.length
	  if (len % 8 !== 0) {
	    throw new RangeError('Buffer size must be a multiple of 64-bits')
	  }
	  for (var i = 0; i < len; i += 8) {
	    swap(this, i, i + 7)
	    swap(this, i + 1, i + 6)
	    swap(this, i + 2, i + 5)
	    swap(this, i + 3, i + 4)
	  }
	  return this
	}

	Buffer.prototype.toString = function toString () {
	  var length = this.length | 0
	  if (length === 0) return ''
	  if (arguments.length === 0) return utf8Slice(this, 0, length)
	  return slowToString.apply(this, arguments)
	}

	Buffer.prototype.equals = function equals (b) {
	  if (!Buffer.isBuffer(b)) throw new TypeError('Argument must be a Buffer')
	  if (this === b) return true
	  return Buffer.compare(this, b) === 0
	}

	Buffer.prototype.inspect = function inspect () {
	  var str = ''
	  var max = exports.INSPECT_MAX_BYTES
	  if (this.length > 0) {
	    str = this.toString('hex', 0, max).match(/.{2}/g).join(' ')
	    if (this.length > max) str += ' ... '
	  }
	  return '<Buffer ' + str + '>'
	}

	Buffer.prototype.compare = function compare (target, start, end, thisStart, thisEnd) {
	  if (!Buffer.isBuffer(target)) {
	    throw new TypeError('Argument must be a Buffer')
	  }

	  if (start === undefined) {
	    start = 0
	  }
	  if (end === undefined) {
	    end = target ? target.length : 0
	  }
	  if (thisStart === undefined) {
	    thisStart = 0
	  }
	  if (thisEnd === undefined) {
	    thisEnd = this.length
	  }

	  if (start < 0 || end > target.length || thisStart < 0 || thisEnd > this.length) {
	    throw new RangeError('out of range index')
	  }

	  if (thisStart >= thisEnd && start >= end) {
	    return 0
	  }
	  if (thisStart >= thisEnd) {
	    return -1
	  }
	  if (start >= end) {
	    return 1
	  }

	  start >>>= 0
	  end >>>= 0
	  thisStart >>>= 0
	  thisEnd >>>= 0

	  if (this === target) return 0

	  var x = thisEnd - thisStart
	  var y = end - start
	  var len = Math.min(x, y)

	  var thisCopy = this.slice(thisStart, thisEnd)
	  var targetCopy = target.slice(start, end)

	  for (var i = 0; i < len; ++i) {
	    if (thisCopy[i] !== targetCopy[i]) {
	      x = thisCopy[i]
	      y = targetCopy[i]
	      break
	    }
	  }

	  if (x < y) return -1
	  if (y < x) return 1
	  return 0
	}

	// Finds either the first index of `val` in `buffer` at offset >= `byteOffset`,
	// OR the last index of `val` in `buffer` at offset <= `byteOffset`.
	//
	// Arguments:
	// - buffer - a Buffer to search
	// - val - a string, Buffer, or number
	// - byteOffset - an index into `buffer`; will be clamped to an int32
	// - encoding - an optional encoding, relevant is val is a string
	// - dir - true for indexOf, false for lastIndexOf
	function bidirectionalIndexOf (buffer, val, byteOffset, encoding, dir) {
	  // Empty buffer means no match
	  if (buffer.length === 0) return -1

	  // Normalize byteOffset
	  if (typeof byteOffset === 'string') {
	    encoding = byteOffset
	    byteOffset = 0
	  } else if (byteOffset > 0x7fffffff) {
	    byteOffset = 0x7fffffff
	  } else if (byteOffset < -0x80000000) {
	    byteOffset = -0x80000000
	  }
	  byteOffset = +byteOffset  // Coerce to Number.
	  if (isNaN(byteOffset)) {
	    // byteOffset: it it's undefined, null, NaN, "foo", etc, search whole buffer
	    byteOffset = dir ? 0 : (buffer.length - 1)
	  }

	  // Normalize byteOffset: negative offsets start from the end of the buffer
	  if (byteOffset < 0) byteOffset = buffer.length + byteOffset
	  if (byteOffset >= buffer.length) {
	    if (dir) return -1
	    else byteOffset = buffer.length - 1
	  } else if (byteOffset < 0) {
	    if (dir) byteOffset = 0
	    else return -1
	  }

	  // Normalize val
	  if (typeof val === 'string') {
	    val = Buffer.from(val, encoding)
	  }

	  // Finally, search either indexOf (if dir is true) or lastIndexOf
	  if (Buffer.isBuffer(val)) {
	    // Special case: looking for empty string/buffer always fails
	    if (val.length === 0) {
	      return -1
	    }
	    return arrayIndexOf(buffer, val, byteOffset, encoding, dir)
	  } else if (typeof val === 'number') {
	    val = val & 0xFF // Search for a byte value [0-255]
	    if (Buffer.TYPED_ARRAY_SUPPORT &&
	        typeof Uint8Array.prototype.indexOf === 'function') {
	      if (dir) {
	        return Uint8Array.prototype.indexOf.call(buffer, val, byteOffset)
	      } else {
	        return Uint8Array.prototype.lastIndexOf.call(buffer, val, byteOffset)
	      }
	    }
	    return arrayIndexOf(buffer, [ val ], byteOffset, encoding, dir)
	  }

	  throw new TypeError('val must be string, number or Buffer')
	}

	function arrayIndexOf (arr, val, byteOffset, encoding, dir) {
	  var indexSize = 1
	  var arrLength = arr.length
	  var valLength = val.length

	  if (encoding !== undefined) {
	    encoding = String(encoding).toLowerCase()
	    if (encoding === 'ucs2' || encoding === 'ucs-2' ||
	        encoding === 'utf16le' || encoding === 'utf-16le') {
	      if (arr.length < 2 || val.length < 2) {
	        return -1
	      }
	      indexSize = 2
	      arrLength /= 2
	      valLength /= 2
	      byteOffset /= 2
	    }
	  }

	  function read (buf, i) {
	    if (indexSize === 1) {
	      return buf[i]
	    } else {
	      return buf.readUInt16BE(i * indexSize)
	    }
	  }

	  var i
	  if (dir) {
	    var foundIndex = -1
	    for (i = byteOffset; i < arrLength; i++) {
	      if (read(arr, i) === read(val, foundIndex === -1 ? 0 : i - foundIndex)) {
	        if (foundIndex === -1) foundIndex = i
	        if (i - foundIndex + 1 === valLength) return foundIndex * indexSize
	      } else {
	        if (foundIndex !== -1) i -= i - foundIndex
	        foundIndex = -1
	      }
	    }
	  } else {
	    if (byteOffset + valLength > arrLength) byteOffset = arrLength - valLength
	    for (i = byteOffset; i >= 0; i--) {
	      var found = true
	      for (var j = 0; j < valLength; j++) {
	        if (read(arr, i + j) !== read(val, j)) {
	          found = false
	          break
	        }
	      }
	      if (found) return i
	    }
	  }

	  return -1
	}

	Buffer.prototype.includes = function includes (val, byteOffset, encoding) {
	  return this.indexOf(val, byteOffset, encoding) !== -1
	}

	Buffer.prototype.indexOf = function indexOf (val, byteOffset, encoding) {
	  return bidirectionalIndexOf(this, val, byteOffset, encoding, true)
	}

	Buffer.prototype.lastIndexOf = function lastIndexOf (val, byteOffset, encoding) {
	  return bidirectionalIndexOf(this, val, byteOffset, encoding, false)
	}

	function hexWrite (buf, string, offset, length) {
	  offset = Number(offset) || 0
	  var remaining = buf.length - offset
	  if (!length) {
	    length = remaining
	  } else {
	    length = Number(length)
	    if (length > remaining) {
	      length = remaining
	    }
	  }

	  // must be an even number of digits
	  var strLen = string.length
	  if (strLen % 2 !== 0) throw new TypeError('Invalid hex string')

	  if (length > strLen / 2) {
	    length = strLen / 2
	  }
	  for (var i = 0; i < length; ++i) {
	    var parsed = parseInt(string.substr(i * 2, 2), 16)
	    if (isNaN(parsed)) return i
	    buf[offset + i] = parsed
	  }
	  return i
	}

	function utf8Write (buf, string, offset, length) {
	  return blitBuffer(utf8ToBytes(string, buf.length - offset), buf, offset, length)
	}

	function asciiWrite (buf, string, offset, length) {
	  return blitBuffer(asciiToBytes(string), buf, offset, length)
	}

	function latin1Write (buf, string, offset, length) {
	  return asciiWrite(buf, string, offset, length)
	}

	function base64Write (buf, string, offset, length) {
	  return blitBuffer(base64ToBytes(string), buf, offset, length)
	}

	function ucs2Write (buf, string, offset, length) {
	  return blitBuffer(utf16leToBytes(string, buf.length - offset), buf, offset, length)
	}

	Buffer.prototype.write = function write (string, offset, length, encoding) {
	  // Buffer#write(string)
	  if (offset === undefined) {
	    encoding = 'utf8'
	    length = this.length
	    offset = 0
	  // Buffer#write(string, encoding)
	  } else if (length === undefined && typeof offset === 'string') {
	    encoding = offset
	    length = this.length
	    offset = 0
	  // Buffer#write(string, offset[, length][, encoding])
	  } else if (isFinite(offset)) {
	    offset = offset | 0
	    if (isFinite(length)) {
	      length = length | 0
	      if (encoding === undefined) encoding = 'utf8'
	    } else {
	      encoding = length
	      length = undefined
	    }
	  // legacy write(string, encoding, offset, length) - remove in v0.13
	  } else {
	    throw new Error(
	      'Buffer.write(string, encoding, offset[, length]) is no longer supported'
	    )
	  }

	  var remaining = this.length - offset
	  if (length === undefined || length > remaining) length = remaining

	  if ((string.length > 0 && (length < 0 || offset < 0)) || offset > this.length) {
	    throw new RangeError('Attempt to write outside buffer bounds')
	  }

	  if (!encoding) encoding = 'utf8'

	  var loweredCase = false
	  for (;;) {
	    switch (encoding) {
	      case 'hex':
	        return hexWrite(this, string, offset, length)

	      case 'utf8':
	      case 'utf-8':
	        return utf8Write(this, string, offset, length)

	      case 'ascii':
	        return asciiWrite(this, string, offset, length)

	      case 'latin1':
	      case 'binary':
	        return latin1Write(this, string, offset, length)

	      case 'base64':
	        // Warning: maxLength not taken into account in base64Write
	        return base64Write(this, string, offset, length)

	      case 'ucs2':
	      case 'ucs-2':
	      case 'utf16le':
	      case 'utf-16le':
	        return ucs2Write(this, string, offset, length)

	      default:
	        if (loweredCase) throw new TypeError('Unknown encoding: ' + encoding)
	        encoding = ('' + encoding).toLowerCase()
	        loweredCase = true
	    }
	  }
	}

	Buffer.prototype.toJSON = function toJSON () {
	  return {
	    type: 'Buffer',
	    data: Array.prototype.slice.call(this._arr || this, 0)
	  }
	}

	function base64Slice (buf, start, end) {
	  if (start === 0 && end === buf.length) {
	    return base64.fromByteArray(buf)
	  } else {
	    return base64.fromByteArray(buf.slice(start, end))
	  }
	}

	function utf8Slice (buf, start, end) {
	  end = Math.min(buf.length, end)
	  var res = []

	  var i = start
	  while (i < end) {
	    var firstByte = buf[i]
	    var codePoint = null
	    var bytesPerSequence = (firstByte > 0xEF) ? 4
	      : (firstByte > 0xDF) ? 3
	      : (firstByte > 0xBF) ? 2
	      : 1

	    if (i + bytesPerSequence <= end) {
	      var secondByte, thirdByte, fourthByte, tempCodePoint

	      switch (bytesPerSequence) {
	        case 1:
	          if (firstByte < 0x80) {
	            codePoint = firstByte
	          }
	          break
	        case 2:
	          secondByte = buf[i + 1]
	          if ((secondByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0x1F) << 0x6 | (secondByte & 0x3F)
	            if (tempCodePoint > 0x7F) {
	              codePoint = tempCodePoint
	            }
	          }
	          break
	        case 3:
	          secondByte = buf[i + 1]
	          thirdByte = buf[i + 2]
	          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0xF) << 0xC | (secondByte & 0x3F) << 0x6 | (thirdByte & 0x3F)
	            if (tempCodePoint > 0x7FF && (tempCodePoint < 0xD800 || tempCodePoint > 0xDFFF)) {
	              codePoint = tempCodePoint
	            }
	          }
	          break
	        case 4:
	          secondByte = buf[i + 1]
	          thirdByte = buf[i + 2]
	          fourthByte = buf[i + 3]
	          if ((secondByte & 0xC0) === 0x80 && (thirdByte & 0xC0) === 0x80 && (fourthByte & 0xC0) === 0x80) {
	            tempCodePoint = (firstByte & 0xF) << 0x12 | (secondByte & 0x3F) << 0xC | (thirdByte & 0x3F) << 0x6 | (fourthByte & 0x3F)
	            if (tempCodePoint > 0xFFFF && tempCodePoint < 0x110000) {
	              codePoint = tempCodePoint
	            }
	          }
	      }
	    }

	    if (codePoint === null) {
	      // we did not generate a valid codePoint so insert a
	      // replacement char (U+FFFD) and advance only 1 byte
	      codePoint = 0xFFFD
	      bytesPerSequence = 1
	    } else if (codePoint > 0xFFFF) {
	      // encode to utf16 (surrogate pair dance)
	      codePoint -= 0x10000
	      res.push(codePoint >>> 10 & 0x3FF | 0xD800)
	      codePoint = 0xDC00 | codePoint & 0x3FF
	    }

	    res.push(codePoint)
	    i += bytesPerSequence
	  }

	  return decodeCodePointsArray(res)
	}

	// Based on http://stackoverflow.com/a/22747272/680742, the browser with
	// the lowest limit is Chrome, with 0x10000 args.
	// We go 1 magnitude less, for safety
	var MAX_ARGUMENTS_LENGTH = 0x1000

	function decodeCodePointsArray (codePoints) {
	  var len = codePoints.length
	  if (len <= MAX_ARGUMENTS_LENGTH) {
	    return String.fromCharCode.apply(String, codePoints) // avoid extra slice()
	  }

	  // Decode in chunks to avoid "call stack size exceeded".
	  var res = ''
	  var i = 0
	  while (i < len) {
	    res += String.fromCharCode.apply(
	      String,
	      codePoints.slice(i, i += MAX_ARGUMENTS_LENGTH)
	    )
	  }
	  return res
	}

	function asciiSlice (buf, start, end) {
	  var ret = ''
	  end = Math.min(buf.length, end)

	  for (var i = start; i < end; ++i) {
	    ret += String.fromCharCode(buf[i] & 0x7F)
	  }
	  return ret
	}

	function latin1Slice (buf, start, end) {
	  var ret = ''
	  end = Math.min(buf.length, end)

	  for (var i = start; i < end; ++i) {
	    ret += String.fromCharCode(buf[i])
	  }
	  return ret
	}

	function hexSlice (buf, start, end) {
	  var len = buf.length

	  if (!start || start < 0) start = 0
	  if (!end || end < 0 || end > len) end = len

	  var out = ''
	  for (var i = start; i < end; ++i) {
	    out += toHex(buf[i])
	  }
	  return out
	}

	function utf16leSlice (buf, start, end) {
	  var bytes = buf.slice(start, end)
	  var res = ''
	  for (var i = 0; i < bytes.length; i += 2) {
	    res += String.fromCharCode(bytes[i] + bytes[i + 1] * 256)
	  }
	  return res
	}

	Buffer.prototype.slice = function slice (start, end) {
	  var len = this.length
	  start = ~~start
	  end = end === undefined ? len : ~~end

	  if (start < 0) {
	    start += len
	    if (start < 0) start = 0
	  } else if (start > len) {
	    start = len
	  }

	  if (end < 0) {
	    end += len
	    if (end < 0) end = 0
	  } else if (end > len) {
	    end = len
	  }

	  if (end < start) end = start

	  var newBuf
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    newBuf = this.subarray(start, end)
	    newBuf.__proto__ = Buffer.prototype
	  } else {
	    var sliceLen = end - start
	    newBuf = new Buffer(sliceLen, undefined)
	    for (var i = 0; i < sliceLen; ++i) {
	      newBuf[i] = this[i + start]
	    }
	  }

	  return newBuf
	}

	/*
	 * Need to make sure that buffer isn't trying to write out of bounds.
	 */
	function checkOffset (offset, ext, length) {
	  if ((offset % 1) !== 0 || offset < 0) throw new RangeError('offset is not uint')
	  if (offset + ext > length) throw new RangeError('Trying to access beyond buffer length')
	}

	Buffer.prototype.readUIntLE = function readUIntLE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var val = this[offset]
	  var mul = 1
	  var i = 0
	  while (++i < byteLength && (mul *= 0x100)) {
	    val += this[offset + i] * mul
	  }

	  return val
	}

	Buffer.prototype.readUIntBE = function readUIntBE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    checkOffset(offset, byteLength, this.length)
	  }

	  var val = this[offset + --byteLength]
	  var mul = 1
	  while (byteLength > 0 && (mul *= 0x100)) {
	    val += this[offset + --byteLength] * mul
	  }

	  return val
	}

	Buffer.prototype.readUInt8 = function readUInt8 (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 1, this.length)
	  return this[offset]
	}

	Buffer.prototype.readUInt16LE = function readUInt16LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  return this[offset] | (this[offset + 1] << 8)
	}

	Buffer.prototype.readUInt16BE = function readUInt16BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  return (this[offset] << 8) | this[offset + 1]
	}

	Buffer.prototype.readUInt32LE = function readUInt32LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return ((this[offset]) |
	      (this[offset + 1] << 8) |
	      (this[offset + 2] << 16)) +
	      (this[offset + 3] * 0x1000000)
	}

	Buffer.prototype.readUInt32BE = function readUInt32BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset] * 0x1000000) +
	    ((this[offset + 1] << 16) |
	    (this[offset + 2] << 8) |
	    this[offset + 3])
	}

	Buffer.prototype.readIntLE = function readIntLE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var val = this[offset]
	  var mul = 1
	  var i = 0
	  while (++i < byteLength && (mul *= 0x100)) {
	    val += this[offset + i] * mul
	  }
	  mul *= 0x80

	  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

	  return val
	}

	Buffer.prototype.readIntBE = function readIntBE (offset, byteLength, noAssert) {
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) checkOffset(offset, byteLength, this.length)

	  var i = byteLength
	  var mul = 1
	  var val = this[offset + --i]
	  while (i > 0 && (mul *= 0x100)) {
	    val += this[offset + --i] * mul
	  }
	  mul *= 0x80

	  if (val >= mul) val -= Math.pow(2, 8 * byteLength)

	  return val
	}

	Buffer.prototype.readInt8 = function readInt8 (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 1, this.length)
	  if (!(this[offset] & 0x80)) return (this[offset])
	  return ((0xff - this[offset] + 1) * -1)
	}

	Buffer.prototype.readInt16LE = function readInt16LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  var val = this[offset] | (this[offset + 1] << 8)
	  return (val & 0x8000) ? val | 0xFFFF0000 : val
	}

	Buffer.prototype.readInt16BE = function readInt16BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 2, this.length)
	  var val = this[offset + 1] | (this[offset] << 8)
	  return (val & 0x8000) ? val | 0xFFFF0000 : val
	}

	Buffer.prototype.readInt32LE = function readInt32LE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset]) |
	    (this[offset + 1] << 8) |
	    (this[offset + 2] << 16) |
	    (this[offset + 3] << 24)
	}

	Buffer.prototype.readInt32BE = function readInt32BE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)

	  return (this[offset] << 24) |
	    (this[offset + 1] << 16) |
	    (this[offset + 2] << 8) |
	    (this[offset + 3])
	}

	Buffer.prototype.readFloatLE = function readFloatLE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)
	  return ieee754.read(this, offset, true, 23, 4)
	}

	Buffer.prototype.readFloatBE = function readFloatBE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 4, this.length)
	  return ieee754.read(this, offset, false, 23, 4)
	}

	Buffer.prototype.readDoubleLE = function readDoubleLE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 8, this.length)
	  return ieee754.read(this, offset, true, 52, 8)
	}

	Buffer.prototype.readDoubleBE = function readDoubleBE (offset, noAssert) {
	  if (!noAssert) checkOffset(offset, 8, this.length)
	  return ieee754.read(this, offset, false, 52, 8)
	}

	function checkInt (buf, value, offset, ext, max, min) {
	  if (!Buffer.isBuffer(buf)) throw new TypeError('"buffer" argument must be a Buffer instance')
	  if (value > max || value < min) throw new RangeError('"value" argument is out of bounds')
	  if (offset + ext > buf.length) throw new RangeError('Index out of range')
	}

	Buffer.prototype.writeUIntLE = function writeUIntLE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    var maxBytes = Math.pow(2, 8 * byteLength) - 1
	    checkInt(this, value, offset, byteLength, maxBytes, 0)
	  }

	  var mul = 1
	  var i = 0
	  this[offset] = value & 0xFF
	  while (++i < byteLength && (mul *= 0x100)) {
	    this[offset + i] = (value / mul) & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeUIntBE = function writeUIntBE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  byteLength = byteLength | 0
	  if (!noAssert) {
	    var maxBytes = Math.pow(2, 8 * byteLength) - 1
	    checkInt(this, value, offset, byteLength, maxBytes, 0)
	  }

	  var i = byteLength - 1
	  var mul = 1
	  this[offset + i] = value & 0xFF
	  while (--i >= 0 && (mul *= 0x100)) {
	    this[offset + i] = (value / mul) & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeUInt8 = function writeUInt8 (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 1, 0xff, 0)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
	  this[offset] = (value & 0xff)
	  return offset + 1
	}

	function objectWriteUInt16 (buf, value, offset, littleEndian) {
	  if (value < 0) value = 0xffff + value + 1
	  for (var i = 0, j = Math.min(buf.length - offset, 2); i < j; ++i) {
	    buf[offset + i] = (value & (0xff << (8 * (littleEndian ? i : 1 - i)))) >>>
	      (littleEndian ? i : 1 - i) * 8
	  }
	}

	Buffer.prototype.writeUInt16LE = function writeUInt16LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	  } else {
	    objectWriteUInt16(this, value, offset, true)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeUInt16BE = function writeUInt16BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0xffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 8)
	    this[offset + 1] = (value & 0xff)
	  } else {
	    objectWriteUInt16(this, value, offset, false)
	  }
	  return offset + 2
	}

	function objectWriteUInt32 (buf, value, offset, littleEndian) {
	  if (value < 0) value = 0xffffffff + value + 1
	  for (var i = 0, j = Math.min(buf.length - offset, 4); i < j; ++i) {
	    buf[offset + i] = (value >>> (littleEndian ? i : 3 - i) * 8) & 0xff
	  }
	}

	Buffer.prototype.writeUInt32LE = function writeUInt32LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset + 3] = (value >>> 24)
	    this[offset + 2] = (value >>> 16)
	    this[offset + 1] = (value >>> 8)
	    this[offset] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, true)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeUInt32BE = function writeUInt32BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0xffffffff, 0)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 24)
	    this[offset + 1] = (value >>> 16)
	    this[offset + 2] = (value >>> 8)
	    this[offset + 3] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, false)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeIntLE = function writeIntLE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) {
	    var limit = Math.pow(2, 8 * byteLength - 1)

	    checkInt(this, value, offset, byteLength, limit - 1, -limit)
	  }

	  var i = 0
	  var mul = 1
	  var sub = 0
	  this[offset] = value & 0xFF
	  while (++i < byteLength && (mul *= 0x100)) {
	    if (value < 0 && sub === 0 && this[offset + i - 1] !== 0) {
	      sub = 1
	    }
	    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeIntBE = function writeIntBE (value, offset, byteLength, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) {
	    var limit = Math.pow(2, 8 * byteLength - 1)

	    checkInt(this, value, offset, byteLength, limit - 1, -limit)
	  }

	  var i = byteLength - 1
	  var mul = 1
	  var sub = 0
	  this[offset + i] = value & 0xFF
	  while (--i >= 0 && (mul *= 0x100)) {
	    if (value < 0 && sub === 0 && this[offset + i + 1] !== 0) {
	      sub = 1
	    }
	    this[offset + i] = ((value / mul) >> 0) - sub & 0xFF
	  }

	  return offset + byteLength
	}

	Buffer.prototype.writeInt8 = function writeInt8 (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 1, 0x7f, -0x80)
	  if (!Buffer.TYPED_ARRAY_SUPPORT) value = Math.floor(value)
	  if (value < 0) value = 0xff + value + 1
	  this[offset] = (value & 0xff)
	  return offset + 1
	}

	Buffer.prototype.writeInt16LE = function writeInt16LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	  } else {
	    objectWriteUInt16(this, value, offset, true)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeInt16BE = function writeInt16BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 2, 0x7fff, -0x8000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 8)
	    this[offset + 1] = (value & 0xff)
	  } else {
	    objectWriteUInt16(this, value, offset, false)
	  }
	  return offset + 2
	}

	Buffer.prototype.writeInt32LE = function writeInt32LE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value & 0xff)
	    this[offset + 1] = (value >>> 8)
	    this[offset + 2] = (value >>> 16)
	    this[offset + 3] = (value >>> 24)
	  } else {
	    objectWriteUInt32(this, value, offset, true)
	  }
	  return offset + 4
	}

	Buffer.prototype.writeInt32BE = function writeInt32BE (value, offset, noAssert) {
	  value = +value
	  offset = offset | 0
	  if (!noAssert) checkInt(this, value, offset, 4, 0x7fffffff, -0x80000000)
	  if (value < 0) value = 0xffffffff + value + 1
	  if (Buffer.TYPED_ARRAY_SUPPORT) {
	    this[offset] = (value >>> 24)
	    this[offset + 1] = (value >>> 16)
	    this[offset + 2] = (value >>> 8)
	    this[offset + 3] = (value & 0xff)
	  } else {
	    objectWriteUInt32(this, value, offset, false)
	  }
	  return offset + 4
	}

	function checkIEEE754 (buf, value, offset, ext, max, min) {
	  if (offset + ext > buf.length) throw new RangeError('Index out of range')
	  if (offset < 0) throw new RangeError('Index out of range')
	}

	function writeFloat (buf, value, offset, littleEndian, noAssert) {
	  if (!noAssert) {
	    checkIEEE754(buf, value, offset, 4, 3.4028234663852886e+38, -3.4028234663852886e+38)
	  }
	  ieee754.write(buf, value, offset, littleEndian, 23, 4)
	  return offset + 4
	}

	Buffer.prototype.writeFloatLE = function writeFloatLE (value, offset, noAssert) {
	  return writeFloat(this, value, offset, true, noAssert)
	}

	Buffer.prototype.writeFloatBE = function writeFloatBE (value, offset, noAssert) {
	  return writeFloat(this, value, offset, false, noAssert)
	}

	function writeDouble (buf, value, offset, littleEndian, noAssert) {
	  if (!noAssert) {
	    checkIEEE754(buf, value, offset, 8, 1.7976931348623157E+308, -1.7976931348623157E+308)
	  }
	  ieee754.write(buf, value, offset, littleEndian, 52, 8)
	  return offset + 8
	}

	Buffer.prototype.writeDoubleLE = function writeDoubleLE (value, offset, noAssert) {
	  return writeDouble(this, value, offset, true, noAssert)
	}

	Buffer.prototype.writeDoubleBE = function writeDoubleBE (value, offset, noAssert) {
	  return writeDouble(this, value, offset, false, noAssert)
	}

	// copy(targetBuffer, targetStart=0, sourceStart=0, sourceEnd=buffer.length)
	Buffer.prototype.copy = function copy (target, targetStart, start, end) {
	  if (!start) start = 0
	  if (!end && end !== 0) end = this.length
	  if (targetStart >= target.length) targetStart = target.length
	  if (!targetStart) targetStart = 0
	  if (end > 0 && end < start) end = start

	  // Copy 0 bytes; we're done
	  if (end === start) return 0
	  if (target.length === 0 || this.length === 0) return 0

	  // Fatal error conditions
	  if (targetStart < 0) {
	    throw new RangeError('targetStart out of bounds')
	  }
	  if (start < 0 || start >= this.length) throw new RangeError('sourceStart out of bounds')
	  if (end < 0) throw new RangeError('sourceEnd out of bounds')

	  // Are we oob?
	  if (end > this.length) end = this.length
	  if (target.length - targetStart < end - start) {
	    end = target.length - targetStart + start
	  }

	  var len = end - start
	  var i

	  if (this === target && start < targetStart && targetStart < end) {
	    // descending copy from end
	    for (i = len - 1; i >= 0; --i) {
	      target[i + targetStart] = this[i + start]
	    }
	  } else if (len < 1000 || !Buffer.TYPED_ARRAY_SUPPORT) {
	    // ascending copy from start
	    for (i = 0; i < len; ++i) {
	      target[i + targetStart] = this[i + start]
	    }
	  } else {
	    Uint8Array.prototype.set.call(
	      target,
	      this.subarray(start, start + len),
	      targetStart
	    )
	  }

	  return len
	}

	// Usage:
	//    buffer.fill(number[, offset[, end]])
	//    buffer.fill(buffer[, offset[, end]])
	//    buffer.fill(string[, offset[, end]][, encoding])
	Buffer.prototype.fill = function fill (val, start, end, encoding) {
	  // Handle string cases:
	  if (typeof val === 'string') {
	    if (typeof start === 'string') {
	      encoding = start
	      start = 0
	      end = this.length
	    } else if (typeof end === 'string') {
	      encoding = end
	      end = this.length
	    }
	    if (val.length === 1) {
	      var code = val.charCodeAt(0)
	      if (code < 256) {
	        val = code
	      }
	    }
	    if (encoding !== undefined && typeof encoding !== 'string') {
	      throw new TypeError('encoding must be a string')
	    }
	    if (typeof encoding === 'string' && !Buffer.isEncoding(encoding)) {
	      throw new TypeError('Unknown encoding: ' + encoding)
	    }
	  } else if (typeof val === 'number') {
	    val = val & 255
	  }

	  // Invalid ranges are not set to a default, so can range check early.
	  if (start < 0 || this.length < start || this.length < end) {
	    throw new RangeError('Out of range index')
	  }

	  if (end <= start) {
	    return this
	  }

	  start = start >>> 0
	  end = end === undefined ? this.length : end >>> 0

	  if (!val) val = 0

	  var i
	  if (typeof val === 'number') {
	    for (i = start; i < end; ++i) {
	      this[i] = val
	    }
	  } else {
	    var bytes = Buffer.isBuffer(val)
	      ? val
	      : utf8ToBytes(new Buffer(val, encoding).toString())
	    var len = bytes.length
	    for (i = 0; i < end - start; ++i) {
	      this[i + start] = bytes[i % len]
	    }
	  }

	  return this
	}

	// HELPER FUNCTIONS
	// ================

	var INVALID_BASE64_RE = /[^+\/0-9A-Za-z-_]/g

	function base64clean (str) {
	  // Node strips out invalid characters like \n and \t from the string, base64-js does not
	  str = stringtrim(str).replace(INVALID_BASE64_RE, '')
	  // Node converts strings with length < 2 to ''
	  if (str.length < 2) return ''
	  // Node allows for non-padded base64 strings (missing trailing ===), base64-js does not
	  while (str.length % 4 !== 0) {
	    str = str + '='
	  }
	  return str
	}

	function stringtrim (str) {
	  if (str.trim) return str.trim()
	  return str.replace(/^\s+|\s+$/g, '')
	}

	function toHex (n) {
	  if (n < 16) return '0' + n.toString(16)
	  return n.toString(16)
	}

	function utf8ToBytes (string, units) {
	  units = units || Infinity
	  var codePoint
	  var length = string.length
	  var leadSurrogate = null
	  var bytes = []

	  for (var i = 0; i < length; ++i) {
	    codePoint = string.charCodeAt(i)

	    // is surrogate component
	    if (codePoint > 0xD7FF && codePoint < 0xE000) {
	      // last char was a lead
	      if (!leadSurrogate) {
	        // no lead yet
	        if (codePoint > 0xDBFF) {
	          // unexpected trail
	          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	          continue
	        } else if (i + 1 === length) {
	          // unpaired lead
	          if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	          continue
	        }

	        // valid lead
	        leadSurrogate = codePoint

	        continue
	      }

	      // 2 leads in a row
	      if (codePoint < 0xDC00) {
	        if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	        leadSurrogate = codePoint
	        continue
	      }

	      // valid surrogate pair
	      codePoint = (leadSurrogate - 0xD800 << 10 | codePoint - 0xDC00) + 0x10000
	    } else if (leadSurrogate) {
	      // valid bmp char, but last char was a lead
	      if ((units -= 3) > -1) bytes.push(0xEF, 0xBF, 0xBD)
	    }

	    leadSurrogate = null

	    // encode utf8
	    if (codePoint < 0x80) {
	      if ((units -= 1) < 0) break
	      bytes.push(codePoint)
	    } else if (codePoint < 0x800) {
	      if ((units -= 2) < 0) break
	      bytes.push(
	        codePoint >> 0x6 | 0xC0,
	        codePoint & 0x3F | 0x80
	      )
	    } else if (codePoint < 0x10000) {
	      if ((units -= 3) < 0) break
	      bytes.push(
	        codePoint >> 0xC | 0xE0,
	        codePoint >> 0x6 & 0x3F | 0x80,
	        codePoint & 0x3F | 0x80
	      )
	    } else if (codePoint < 0x110000) {
	      if ((units -= 4) < 0) break
	      bytes.push(
	        codePoint >> 0x12 | 0xF0,
	        codePoint >> 0xC & 0x3F | 0x80,
	        codePoint >> 0x6 & 0x3F | 0x80,
	        codePoint & 0x3F | 0x80
	      )
	    } else {
	      throw new Error('Invalid code point')
	    }
	  }

	  return bytes
	}

	function asciiToBytes (str) {
	  var byteArray = []
	  for (var i = 0; i < str.length; ++i) {
	    // Node's code seems to be doing this and not & 0x7F..
	    byteArray.push(str.charCodeAt(i) & 0xFF)
	  }
	  return byteArray
	}

	function utf16leToBytes (str, units) {
	  var c, hi, lo
	  var byteArray = []
	  for (var i = 0; i < str.length; ++i) {
	    if ((units -= 2) < 0) break

	    c = str.charCodeAt(i)
	    hi = c >> 8
	    lo = c % 256
	    byteArray.push(lo)
	    byteArray.push(hi)
	  }

	  return byteArray
	}

	function base64ToBytes (str) {
	  return base64.toByteArray(base64clean(str))
	}

	function blitBuffer (src, dst, offset, length) {
	  for (var i = 0; i < length; ++i) {
	    if ((i + offset >= dst.length) || (i >= src.length)) break
	    dst[i + offset] = src[i]
	  }
	  return i
	}

	function isnan (val) {
	  return val !== val // eslint-disable-line no-self-compare
	}

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 340 */
/***/ (function(module, exports) {

	'use strict'

	exports.byteLength = byteLength
	exports.toByteArray = toByteArray
	exports.fromByteArray = fromByteArray

	var lookup = []
	var revLookup = []
	var Arr = typeof Uint8Array !== 'undefined' ? Uint8Array : Array

	var code = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
	for (var i = 0, len = code.length; i < len; ++i) {
	  lookup[i] = code[i]
	  revLookup[code.charCodeAt(i)] = i
	}

	// Support decoding URL-safe base64 strings, as Node.js does.
	// See: https://en.wikipedia.org/wiki/Base64#URL_applications
	revLookup['-'.charCodeAt(0)] = 62
	revLookup['_'.charCodeAt(0)] = 63

	function getLens (b64) {
	  var len = b64.length

	  if (len % 4 > 0) {
	    throw new Error('Invalid string. Length must be a multiple of 4')
	  }

	  // Trim off extra bytes after placeholder bytes are found
	  // See: https://github.com/beatgammit/base64-js/issues/42
	  var validLen = b64.indexOf('=')
	  if (validLen === -1) validLen = len

	  var placeHoldersLen = validLen === len
	    ? 0
	    : 4 - (validLen % 4)

	  return [validLen, placeHoldersLen]
	}

	// base64 is 4/3 + up to two characters of the original data
	function byteLength (b64) {
	  var lens = getLens(b64)
	  var validLen = lens[0]
	  var placeHoldersLen = lens[1]
	  return ((validLen + placeHoldersLen) * 3 / 4) - placeHoldersLen
	}

	function _byteLength (b64, validLen, placeHoldersLen) {
	  return ((validLen + placeHoldersLen) * 3 / 4) - placeHoldersLen
	}

	function toByteArray (b64) {
	  var tmp
	  var lens = getLens(b64)
	  var validLen = lens[0]
	  var placeHoldersLen = lens[1]

	  var arr = new Arr(_byteLength(b64, validLen, placeHoldersLen))

	  var curByte = 0

	  // if there are placeholders, only get up to the last complete 4 chars
	  var len = placeHoldersLen > 0
	    ? validLen - 4
	    : validLen

	  var i
	  for (i = 0; i < len; i += 4) {
	    tmp =
	      (revLookup[b64.charCodeAt(i)] << 18) |
	      (revLookup[b64.charCodeAt(i + 1)] << 12) |
	      (revLookup[b64.charCodeAt(i + 2)] << 6) |
	      revLookup[b64.charCodeAt(i + 3)]
	    arr[curByte++] = (tmp >> 16) & 0xFF
	    arr[curByte++] = (tmp >> 8) & 0xFF
	    arr[curByte++] = tmp & 0xFF
	  }

	  if (placeHoldersLen === 2) {
	    tmp =
	      (revLookup[b64.charCodeAt(i)] << 2) |
	      (revLookup[b64.charCodeAt(i + 1)] >> 4)
	    arr[curByte++] = tmp & 0xFF
	  }

	  if (placeHoldersLen === 1) {
	    tmp =
	      (revLookup[b64.charCodeAt(i)] << 10) |
	      (revLookup[b64.charCodeAt(i + 1)] << 4) |
	      (revLookup[b64.charCodeAt(i + 2)] >> 2)
	    arr[curByte++] = (tmp >> 8) & 0xFF
	    arr[curByte++] = tmp & 0xFF
	  }

	  return arr
	}

	function tripletToBase64 (num) {
	  return lookup[num >> 18 & 0x3F] +
	    lookup[num >> 12 & 0x3F] +
	    lookup[num >> 6 & 0x3F] +
	    lookup[num & 0x3F]
	}

	function encodeChunk (uint8, start, end) {
	  var tmp
	  var output = []
	  for (var i = start; i < end; i += 3) {
	    tmp =
	      ((uint8[i] << 16) & 0xFF0000) +
	      ((uint8[i + 1] << 8) & 0xFF00) +
	      (uint8[i + 2] & 0xFF)
	    output.push(tripletToBase64(tmp))
	  }
	  return output.join('')
	}

	function fromByteArray (uint8) {
	  var tmp
	  var len = uint8.length
	  var extraBytes = len % 3 // if we have 1 byte left, pad 2 bytes
	  var parts = []
	  var maxChunkLength = 16383 // must be multiple of 3

	  // go through the array every three bytes, we'll deal with trailing stuff later
	  for (var i = 0, len2 = len - extraBytes; i < len2; i += maxChunkLength) {
	    parts.push(encodeChunk(
	      uint8, i, (i + maxChunkLength) > len2 ? len2 : (i + maxChunkLength)
	    ))
	  }

	  // pad the end with zeros, but make sure to not forget the extra bytes
	  if (extraBytes === 1) {
	    tmp = uint8[len - 1]
	    parts.push(
	      lookup[tmp >> 2] +
	      lookup[(tmp << 4) & 0x3F] +
	      '=='
	    )
	  } else if (extraBytes === 2) {
	    tmp = (uint8[len - 2] << 8) + uint8[len - 1]
	    parts.push(
	      lookup[tmp >> 10] +
	      lookup[(tmp >> 4) & 0x3F] +
	      lookup[(tmp << 2) & 0x3F] +
	      '='
	    )
	  }

	  return parts.join('')
	}


/***/ }),
/* 341 */
/***/ (function(module, exports) {

	exports.read = function (buffer, offset, isLE, mLen, nBytes) {
	  var e, m
	  var eLen = (nBytes * 8) - mLen - 1
	  var eMax = (1 << eLen) - 1
	  var eBias = eMax >> 1
	  var nBits = -7
	  var i = isLE ? (nBytes - 1) : 0
	  var d = isLE ? -1 : 1
	  var s = buffer[offset + i]

	  i += d

	  e = s & ((1 << (-nBits)) - 1)
	  s >>= (-nBits)
	  nBits += eLen
	  for (; nBits > 0; e = (e * 256) + buffer[offset + i], i += d, nBits -= 8) {}

	  m = e & ((1 << (-nBits)) - 1)
	  e >>= (-nBits)
	  nBits += mLen
	  for (; nBits > 0; m = (m * 256) + buffer[offset + i], i += d, nBits -= 8) {}

	  if (e === 0) {
	    e = 1 - eBias
	  } else if (e === eMax) {
	    return m ? NaN : ((s ? -1 : 1) * Infinity)
	  } else {
	    m = m + Math.pow(2, mLen)
	    e = e - eBias
	  }
	  return (s ? -1 : 1) * m * Math.pow(2, e - mLen)
	}

	exports.write = function (buffer, value, offset, isLE, mLen, nBytes) {
	  var e, m, c
	  var eLen = (nBytes * 8) - mLen - 1
	  var eMax = (1 << eLen) - 1
	  var eBias = eMax >> 1
	  var rt = (mLen === 23 ? Math.pow(2, -24) - Math.pow(2, -77) : 0)
	  var i = isLE ? 0 : (nBytes - 1)
	  var d = isLE ? 1 : -1
	  var s = value < 0 || (value === 0 && 1 / value < 0) ? 1 : 0

	  value = Math.abs(value)

	  if (isNaN(value) || value === Infinity) {
	    m = isNaN(value) ? 1 : 0
	    e = eMax
	  } else {
	    e = Math.floor(Math.log(value) / Math.LN2)
	    if (value * (c = Math.pow(2, -e)) < 1) {
	      e--
	      c *= 2
	    }
	    if (e + eBias >= 1) {
	      value += rt / c
	    } else {
	      value += rt * Math.pow(2, 1 - eBias)
	    }
	    if (value * c >= 2) {
	      e++
	      c /= 2
	    }

	    if (e + eBias >= eMax) {
	      m = 0
	      e = eMax
	    } else if (e + eBias >= 1) {
	      m = ((value * c) - 1) * Math.pow(2, mLen)
	      e = e + eBias
	    } else {
	      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen)
	      e = 0
	    }
	  }

	  for (; mLen >= 8; buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8) {}

	  e = (e << mLen) | m
	  eLen += mLen
	  for (; eLen > 0; buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8) {}

	  buffer[offset + i - d] |= s * 128
	}


/***/ }),
/* 342 */
/***/ (function(module, exports) {

	var toString = {}.toString;

	module.exports = Array.isArray || function (arr) {
	  return toString.call(arr) == '[object Array]';
	};


/***/ }),
/* 343 */
/***/ (function(module, exports) {

	// shim for using process in browser
	var process = module.exports = {};

	// cached from whatever global is present so that test runners that stub it
	// don't break things.  But we need to wrap it in a try catch in case it is
	// wrapped in strict mode code which doesn't define any globals.  It's inside a
	// function because try/catches deoptimize in certain engines.

	var cachedSetTimeout;
	var cachedClearTimeout;

	function defaultSetTimout() {
	    throw new Error('setTimeout has not been defined');
	}
	function defaultClearTimeout () {
	    throw new Error('clearTimeout has not been defined');
	}
	(function () {
	    try {
	        if (typeof setTimeout === 'function') {
	            cachedSetTimeout = setTimeout;
	        } else {
	            cachedSetTimeout = defaultSetTimout;
	        }
	    } catch (e) {
	        cachedSetTimeout = defaultSetTimout;
	    }
	    try {
	        if (typeof clearTimeout === 'function') {
	            cachedClearTimeout = clearTimeout;
	        } else {
	            cachedClearTimeout = defaultClearTimeout;
	        }
	    } catch (e) {
	        cachedClearTimeout = defaultClearTimeout;
	    }
	} ())
	function runTimeout(fun) {
	    if (cachedSetTimeout === setTimeout) {
	        //normal enviroments in sane situations
	        return setTimeout(fun, 0);
	    }
	    // if setTimeout wasn't available but was latter defined
	    if ((cachedSetTimeout === defaultSetTimout || !cachedSetTimeout) && setTimeout) {
	        cachedSetTimeout = setTimeout;
	        return setTimeout(fun, 0);
	    }
	    try {
	        // when when somebody has screwed with setTimeout but no I.E. maddness
	        return cachedSetTimeout(fun, 0);
	    } catch(e){
	        try {
	            // When we are in I.E. but the script has been evaled so I.E. doesn't trust the global object when called normally
	            return cachedSetTimeout.call(null, fun, 0);
	        } catch(e){
	            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error
	            return cachedSetTimeout.call(this, fun, 0);
	        }
	    }


	}
	function runClearTimeout(marker) {
	    if (cachedClearTimeout === clearTimeout) {
	        //normal enviroments in sane situations
	        return clearTimeout(marker);
	    }
	    // if clearTimeout wasn't available but was latter defined
	    if ((cachedClearTimeout === defaultClearTimeout || !cachedClearTimeout) && clearTimeout) {
	        cachedClearTimeout = clearTimeout;
	        return clearTimeout(marker);
	    }
	    try {
	        // when when somebody has screwed with setTimeout but no I.E. maddness
	        return cachedClearTimeout(marker);
	    } catch (e){
	        try {
	            // When we are in I.E. but the script has been evaled so I.E. doesn't  trust the global object when called normally
	            return cachedClearTimeout.call(null, marker);
	        } catch (e){
	            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error.
	            // Some versions of I.E. have different rules for clearTimeout vs setTimeout
	            return cachedClearTimeout.call(this, marker);
	        }
	    }



	}
	var queue = [];
	var draining = false;
	var currentQueue;
	var queueIndex = -1;

	function cleanUpNextTick() {
	    if (!draining || !currentQueue) {
	        return;
	    }
	    draining = false;
	    if (currentQueue.length) {
	        queue = currentQueue.concat(queue);
	    } else {
	        queueIndex = -1;
	    }
	    if (queue.length) {
	        drainQueue();
	    }
	}

	function drainQueue() {
	    if (draining) {
	        return;
	    }
	    var timeout = runTimeout(cleanUpNextTick);
	    draining = true;

	    var len = queue.length;
	    while(len) {
	        currentQueue = queue;
	        queue = [];
	        while (++queueIndex < len) {
	            if (currentQueue) {
	                currentQueue[queueIndex].run();
	            }
	        }
	        queueIndex = -1;
	        len = queue.length;
	    }
	    currentQueue = null;
	    draining = false;
	    runClearTimeout(timeout);
	}

	process.nextTick = function (fun) {
	    var args = new Array(arguments.length - 1);
	    if (arguments.length > 1) {
	        for (var i = 1; i < arguments.length; i++) {
	            args[i - 1] = arguments[i];
	        }
	    }
	    queue.push(new Item(fun, args));
	    if (queue.length === 1 && !draining) {
	        runTimeout(drainQueue);
	    }
	};

	// v8 likes predictible objects
	function Item(fun, array) {
	    this.fun = fun;
	    this.array = array;
	}
	Item.prototype.run = function () {
	    this.fun.apply(null, this.array);
	};
	process.title = 'browser';
	process.browser = true;
	process.env = {};
	process.argv = [];
	process.version = ''; // empty string to avoid regexp issues
	process.versions = {};

	function noop() {}

	process.on = noop;
	process.addListener = noop;
	process.once = noop;
	process.off = noop;
	process.removeListener = noop;
	process.removeAllListeners = noop;
	process.emit = noop;
	process.prependListener = noop;
	process.prependOnceListener = noop;

	process.listeners = function (name) { return [] }

	process.binding = function (name) {
	    throw new Error('process.binding is not supported');
	};

	process.cwd = function () { return '/' };
	process.chdir = function (dir) {
	    throw new Error('process.chdir is not supported');
	};
	process.umask = function() { return 0; };


/***/ }),
/* 344 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {'use strict';

	/**
	 * Normalizes our expected stringified form of a function across versions of node
	 * @param {Function} fn The function to stringify
	 */

	function normalizedFunctionString(fn) {
	  return fn.toString().replace(/function *\(/, 'function (');
	}

	function newBuffer(item, encoding) {
	  return new Buffer(item, encoding);
	}

	function allocBuffer() {
	  return Buffer.alloc.apply(Buffer, arguments);
	}

	function toBuffer() {
	  return Buffer.from.apply(Buffer, arguments);
	}

	module.exports = {
	  normalizedFunctionString: normalizedFunctionString,
	  allocBuffer: typeof Buffer.alloc === 'function' ? allocBuffer : newBuffer,
	  toBuffer: typeof Buffer.from === 'function' ? toBuffer : newBuffer
	};
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(339).Buffer))

/***/ }),
/* 345 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global, process) {// Copyright Joyent, Inc. and other Node contributors.
	//
	// Permission is hereby granted, free of charge, to any person obtaining a
	// copy of this software and associated documentation files (the
	// "Software"), to deal in the Software without restriction, including
	// without limitation the rights to use, copy, modify, merge, publish,
	// distribute, sublicense, and/or sell copies of the Software, and to permit
	// persons to whom the Software is furnished to do so, subject to the
	// following conditions:
	//
	// The above copyright notice and this permission notice shall be included
	// in all copies or substantial portions of the Software.
	//
	// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
	// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
	// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
	// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
	// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
	// USE OR OTHER DEALINGS IN THE SOFTWARE.

	var formatRegExp = /%[sdj%]/g;
	exports.format = function(f) {
	  if (!isString(f)) {
	    var objects = [];
	    for (var i = 0; i < arguments.length; i++) {
	      objects.push(inspect(arguments[i]));
	    }
	    return objects.join(' ');
	  }

	  var i = 1;
	  var args = arguments;
	  var len = args.length;
	  var str = String(f).replace(formatRegExp, function(x) {
	    if (x === '%%') return '%';
	    if (i >= len) return x;
	    switch (x) {
	      case '%s': return String(args[i++]);
	      case '%d': return Number(args[i++]);
	      case '%j':
	        try {
	          return JSON.stringify(args[i++]);
	        } catch (_) {
	          return '[Circular]';
	        }
	      default:
	        return x;
	    }
	  });
	  for (var x = args[i]; i < len; x = args[++i]) {
	    if (isNull(x) || !isObject(x)) {
	      str += ' ' + x;
	    } else {
	      str += ' ' + inspect(x);
	    }
	  }
	  return str;
	};


	// Mark that a method should not be used.
	// Returns a modified function which warns once by default.
	// If --no-deprecation is set, then it is a no-op.
	exports.deprecate = function(fn, msg) {
	  // Allow for deprecating things in the process of starting up.
	  if (isUndefined(global.process)) {
	    return function() {
	      return exports.deprecate(fn, msg).apply(this, arguments);
	    };
	  }

	  if (process.noDeprecation === true) {
	    return fn;
	  }

	  var warned = false;
	  function deprecated() {
	    if (!warned) {
	      if (process.throwDeprecation) {
	        throw new Error(msg);
	      } else if (process.traceDeprecation) {
	        console.trace(msg);
	      } else {
	        console.error(msg);
	      }
	      warned = true;
	    }
	    return fn.apply(this, arguments);
	  }

	  return deprecated;
	};


	var debugs = {};
	var debugEnviron;
	exports.debuglog = function(set) {
	  if (isUndefined(debugEnviron))
	    debugEnviron = process.env.NODE_DEBUG || '';
	  set = set.toUpperCase();
	  if (!debugs[set]) {
	    if (new RegExp('\\b' + set + '\\b', 'i').test(debugEnviron)) {
	      var pid = process.pid;
	      debugs[set] = function() {
	        var msg = exports.format.apply(exports, arguments);
	        console.error('%s %d: %s', set, pid, msg);
	      };
	    } else {
	      debugs[set] = function() {};
	    }
	  }
	  return debugs[set];
	};


	/**
	 * Echos the value of a value. Trys to print the value out
	 * in the best way possible given the different types.
	 *
	 * @param {Object} obj The object to print out.
	 * @param {Object} opts Optional options object that alters the output.
	 */
	/* legacy: obj, showHidden, depth, colors*/
	function inspect(obj, opts) {
	  // default options
	  var ctx = {
	    seen: [],
	    stylize: stylizeNoColor
	  };
	  // legacy...
	  if (arguments.length >= 3) ctx.depth = arguments[2];
	  if (arguments.length >= 4) ctx.colors = arguments[3];
	  if (isBoolean(opts)) {
	    // legacy...
	    ctx.showHidden = opts;
	  } else if (opts) {
	    // got an "options" object
	    exports._extend(ctx, opts);
	  }
	  // set default options
	  if (isUndefined(ctx.showHidden)) ctx.showHidden = false;
	  if (isUndefined(ctx.depth)) ctx.depth = 2;
	  if (isUndefined(ctx.colors)) ctx.colors = false;
	  if (isUndefined(ctx.customInspect)) ctx.customInspect = true;
	  if (ctx.colors) ctx.stylize = stylizeWithColor;
	  return formatValue(ctx, obj, ctx.depth);
	}
	exports.inspect = inspect;


	// http://en.wikipedia.org/wiki/ANSI_escape_code#graphics
	inspect.colors = {
	  'bold' : [1, 22],
	  'italic' : [3, 23],
	  'underline' : [4, 24],
	  'inverse' : [7, 27],
	  'white' : [37, 39],
	  'grey' : [90, 39],
	  'black' : [30, 39],
	  'blue' : [34, 39],
	  'cyan' : [36, 39],
	  'green' : [32, 39],
	  'magenta' : [35, 39],
	  'red' : [31, 39],
	  'yellow' : [33, 39]
	};

	// Don't use 'blue' not visible on cmd.exe
	inspect.styles = {
	  'special': 'cyan',
	  'number': 'yellow',
	  'boolean': 'yellow',
	  'undefined': 'grey',
	  'null': 'bold',
	  'string': 'green',
	  'date': 'magenta',
	  // "name": intentionally not styling
	  'regexp': 'red'
	};


	function stylizeWithColor(str, styleType) {
	  var style = inspect.styles[styleType];

	  if (style) {
	    return '\u001b[' + inspect.colors[style][0] + 'm' + str +
	           '\u001b[' + inspect.colors[style][1] + 'm';
	  } else {
	    return str;
	  }
	}


	function stylizeNoColor(str, styleType) {
	  return str;
	}


	function arrayToHash(array) {
	  var hash = {};

	  array.forEach(function(val, idx) {
	    hash[val] = true;
	  });

	  return hash;
	}


	function formatValue(ctx, value, recurseTimes) {
	  // Provide a hook for user-specified inspect functions.
	  // Check that value is an object with an inspect function on it
	  if (ctx.customInspect &&
	      value &&
	      isFunction(value.inspect) &&
	      // Filter out the util module, it's inspect function is special
	      value.inspect !== exports.inspect &&
	      // Also filter out any prototype objects using the circular check.
	      !(value.constructor && value.constructor.prototype === value)) {
	    var ret = value.inspect(recurseTimes, ctx);
	    if (!isString(ret)) {
	      ret = formatValue(ctx, ret, recurseTimes);
	    }
	    return ret;
	  }

	  // Primitive types cannot have properties
	  var primitive = formatPrimitive(ctx, value);
	  if (primitive) {
	    return primitive;
	  }

	  // Look up the keys of the object.
	  var keys = Object.keys(value);
	  var visibleKeys = arrayToHash(keys);

	  if (ctx.showHidden) {
	    keys = Object.getOwnPropertyNames(value);
	  }

	  // IE doesn't make error fields non-enumerable
	  // http://msdn.microsoft.com/en-us/library/ie/dww52sbt(v=vs.94).aspx
	  if (isError(value)
	      && (keys.indexOf('message') >= 0 || keys.indexOf('description') >= 0)) {
	    return formatError(value);
	  }

	  // Some type of object without properties can be shortcutted.
	  if (keys.length === 0) {
	    if (isFunction(value)) {
	      var name = value.name ? ': ' + value.name : '';
	      return ctx.stylize('[Function' + name + ']', 'special');
	    }
	    if (isRegExp(value)) {
	      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
	    }
	    if (isDate(value)) {
	      return ctx.stylize(Date.prototype.toString.call(value), 'date');
	    }
	    if (isError(value)) {
	      return formatError(value);
	    }
	  }

	  var base = '', array = false, braces = ['{', '}'];

	  // Make Array say that they are Array
	  if (isArray(value)) {
	    array = true;
	    braces = ['[', ']'];
	  }

	  // Make functions say that they are functions
	  if (isFunction(value)) {
	    var n = value.name ? ': ' + value.name : '';
	    base = ' [Function' + n + ']';
	  }

	  // Make RegExps say that they are RegExps
	  if (isRegExp(value)) {
	    base = ' ' + RegExp.prototype.toString.call(value);
	  }

	  // Make dates with properties first say the date
	  if (isDate(value)) {
	    base = ' ' + Date.prototype.toUTCString.call(value);
	  }

	  // Make error with message first say the error
	  if (isError(value)) {
	    base = ' ' + formatError(value);
	  }

	  if (keys.length === 0 && (!array || value.length == 0)) {
	    return braces[0] + base + braces[1];
	  }

	  if (recurseTimes < 0) {
	    if (isRegExp(value)) {
	      return ctx.stylize(RegExp.prototype.toString.call(value), 'regexp');
	    } else {
	      return ctx.stylize('[Object]', 'special');
	    }
	  }

	  ctx.seen.push(value);

	  var output;
	  if (array) {
	    output = formatArray(ctx, value, recurseTimes, visibleKeys, keys);
	  } else {
	    output = keys.map(function(key) {
	      return formatProperty(ctx, value, recurseTimes, visibleKeys, key, array);
	    });
	  }

	  ctx.seen.pop();

	  return reduceToSingleString(output, base, braces);
	}


	function formatPrimitive(ctx, value) {
	  if (isUndefined(value))
	    return ctx.stylize('undefined', 'undefined');
	  if (isString(value)) {
	    var simple = '\'' + JSON.stringify(value).replace(/^"|"$/g, '')
	                                             .replace(/'/g, "\\'")
	                                             .replace(/\\"/g, '"') + '\'';
	    return ctx.stylize(simple, 'string');
	  }
	  if (isNumber(value))
	    return ctx.stylize('' + value, 'number');
	  if (isBoolean(value))
	    return ctx.stylize('' + value, 'boolean');
	  // For some reason typeof null is "object", so special case here.
	  if (isNull(value))
	    return ctx.stylize('null', 'null');
	}


	function formatError(value) {
	  return '[' + Error.prototype.toString.call(value) + ']';
	}


	function formatArray(ctx, value, recurseTimes, visibleKeys, keys) {
	  var output = [];
	  for (var i = 0, l = value.length; i < l; ++i) {
	    if (hasOwnProperty(value, String(i))) {
	      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
	          String(i), true));
	    } else {
	      output.push('');
	    }
	  }
	  keys.forEach(function(key) {
	    if (!key.match(/^\d+$/)) {
	      output.push(formatProperty(ctx, value, recurseTimes, visibleKeys,
	          key, true));
	    }
	  });
	  return output;
	}


	function formatProperty(ctx, value, recurseTimes, visibleKeys, key, array) {
	  var name, str, desc;
	  desc = Object.getOwnPropertyDescriptor(value, key) || { value: value[key] };
	  if (desc.get) {
	    if (desc.set) {
	      str = ctx.stylize('[Getter/Setter]', 'special');
	    } else {
	      str = ctx.stylize('[Getter]', 'special');
	    }
	  } else {
	    if (desc.set) {
	      str = ctx.stylize('[Setter]', 'special');
	    }
	  }
	  if (!hasOwnProperty(visibleKeys, key)) {
	    name = '[' + key + ']';
	  }
	  if (!str) {
	    if (ctx.seen.indexOf(desc.value) < 0) {
	      if (isNull(recurseTimes)) {
	        str = formatValue(ctx, desc.value, null);
	      } else {
	        str = formatValue(ctx, desc.value, recurseTimes - 1);
	      }
	      if (str.indexOf('\n') > -1) {
	        if (array) {
	          str = str.split('\n').map(function(line) {
	            return '  ' + line;
	          }).join('\n').substr(2);
	        } else {
	          str = '\n' + str.split('\n').map(function(line) {
	            return '   ' + line;
	          }).join('\n');
	        }
	      }
	    } else {
	      str = ctx.stylize('[Circular]', 'special');
	    }
	  }
	  if (isUndefined(name)) {
	    if (array && key.match(/^\d+$/)) {
	      return str;
	    }
	    name = JSON.stringify('' + key);
	    if (name.match(/^"([a-zA-Z_][a-zA-Z_0-9]*)"$/)) {
	      name = name.substr(1, name.length - 2);
	      name = ctx.stylize(name, 'name');
	    } else {
	      name = name.replace(/'/g, "\\'")
	                 .replace(/\\"/g, '"')
	                 .replace(/(^"|"$)/g, "'");
	      name = ctx.stylize(name, 'string');
	    }
	  }

	  return name + ': ' + str;
	}


	function reduceToSingleString(output, base, braces) {
	  var numLinesEst = 0;
	  var length = output.reduce(function(prev, cur) {
	    numLinesEst++;
	    if (cur.indexOf('\n') >= 0) numLinesEst++;
	    return prev + cur.replace(/\u001b\[\d\d?m/g, '').length + 1;
	  }, 0);

	  if (length > 60) {
	    return braces[0] +
	           (base === '' ? '' : base + '\n ') +
	           ' ' +
	           output.join(',\n  ') +
	           ' ' +
	           braces[1];
	  }

	  return braces[0] + base + ' ' + output.join(', ') + ' ' + braces[1];
	}


	// NOTE: These type checking functions intentionally don't use `instanceof`
	// because it is fragile and can be easily faked with `Object.create()`.
	function isArray(ar) {
	  return Array.isArray(ar);
	}
	exports.isArray = isArray;

	function isBoolean(arg) {
	  return typeof arg === 'boolean';
	}
	exports.isBoolean = isBoolean;

	function isNull(arg) {
	  return arg === null;
	}
	exports.isNull = isNull;

	function isNullOrUndefined(arg) {
	  return arg == null;
	}
	exports.isNullOrUndefined = isNullOrUndefined;

	function isNumber(arg) {
	  return typeof arg === 'number';
	}
	exports.isNumber = isNumber;

	function isString(arg) {
	  return typeof arg === 'string';
	}
	exports.isString = isString;

	function isSymbol(arg) {
	  return typeof arg === 'symbol';
	}
	exports.isSymbol = isSymbol;

	function isUndefined(arg) {
	  return arg === void 0;
	}
	exports.isUndefined = isUndefined;

	function isRegExp(re) {
	  return isObject(re) && objectToString(re) === '[object RegExp]';
	}
	exports.isRegExp = isRegExp;

	function isObject(arg) {
	  return typeof arg === 'object' && arg !== null;
	}
	exports.isObject = isObject;

	function isDate(d) {
	  return isObject(d) && objectToString(d) === '[object Date]';
	}
	exports.isDate = isDate;

	function isError(e) {
	  return isObject(e) &&
	      (objectToString(e) === '[object Error]' || e instanceof Error);
	}
	exports.isError = isError;

	function isFunction(arg) {
	  return typeof arg === 'function';
	}
	exports.isFunction = isFunction;

	function isPrimitive(arg) {
	  return arg === null ||
	         typeof arg === 'boolean' ||
	         typeof arg === 'number' ||
	         typeof arg === 'string' ||
	         typeof arg === 'symbol' ||  // ES6 symbol
	         typeof arg === 'undefined';
	}
	exports.isPrimitive = isPrimitive;

	exports.isBuffer = __webpack_require__(346);

	function objectToString(o) {
	  return Object.prototype.toString.call(o);
	}


	function pad(n) {
	  return n < 10 ? '0' + n.toString(10) : n.toString(10);
	}


	var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
	              'Oct', 'Nov', 'Dec'];

	// 26 Feb 16:19:34
	function timestamp() {
	  var d = new Date();
	  var time = [pad(d.getHours()),
	              pad(d.getMinutes()),
	              pad(d.getSeconds())].join(':');
	  return [d.getDate(), months[d.getMonth()], time].join(' ');
	}


	// log is just a thin wrapper to console.log that prepends a timestamp
	exports.log = function() {
	  console.log('%s - %s', timestamp(), exports.format.apply(exports, arguments));
	};


	/**
	 * Inherit the prototype methods from one constructor into another.
	 *
	 * The Function.prototype.inherits from lang.js rewritten as a standalone
	 * function (not on Function.prototype). NOTE: If this file is to be loaded
	 * during bootstrapping this function needs to be rewritten using some native
	 * functions as prototype setup using normal JavaScript does not work as
	 * expected during bootstrapping (see mirror.js in r114903).
	 *
	 * @param {function} ctor Constructor function which needs to inherit the
	 *     prototype.
	 * @param {function} superCtor Constructor function to inherit prototype from.
	 */
	exports.inherits = __webpack_require__(347);

	exports._extend = function(origin, add) {
	  // Don't do anything if add isn't an object
	  if (!add || !isObject(add)) return origin;

	  var keys = Object.keys(add);
	  var i = keys.length;
	  while (i--) {
	    origin[keys[i]] = add[keys[i]];
	  }
	  return origin;
	};

	function hasOwnProperty(obj, prop) {
	  return Object.prototype.hasOwnProperty.call(obj, prop);
	}

	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }()), __webpack_require__(343)))

/***/ }),
/* 346 */
/***/ (function(module, exports) {

	module.exports = function isBuffer(arg) {
	  return arg && typeof arg === 'object'
	    && typeof arg.copy === 'function'
	    && typeof arg.fill === 'function'
	    && typeof arg.readUInt8 === 'function';
	}

/***/ }),
/* 347 */
/***/ (function(module, exports) {

	if (typeof Object.create === 'function') {
	  // implementation from standard node.js 'util' module
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    ctor.prototype = Object.create(superCtor.prototype, {
	      constructor: {
	        value: ctor,
	        enumerable: false,
	        writable: true,
	        configurable: true
	      }
	    });
	  };
	} else {
	  // old school shim for old browsers
	  module.exports = function inherits(ctor, superCtor) {
	    ctor.super_ = superCtor
	    var TempCtor = function () {}
	    TempCtor.prototype = superCtor.prototype
	    ctor.prototype = new TempCtor()
	    ctor.prototype.constructor = ctor
	  }
	}


/***/ }),
/* 348 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON RegExp type.
	 *
	 * @class
	 * @return {BSONRegExp} A MinKey instance
	 */
	function BSONRegExp(pattern, options) {
	  if (!(this instanceof BSONRegExp)) return new BSONRegExp();

	  // Execute
	  this._bsontype = 'BSONRegExp';
	  this.pattern = pattern || '';
	  this.options = options || '';

	  // Validate options
	  for (var i = 0; i < this.options.length; i++) {
	    if (!(this.options[i] === 'i' || this.options[i] === 'm' || this.options[i] === 'x' || this.options[i] === 'l' || this.options[i] === 's' || this.options[i] === 'u')) {
	      throw new Error('the regular expression options [' + this.options[i] + '] is not supported');
	    }
	  }
	}

	module.exports = BSONRegExp;
	module.exports.BSONRegExp = BSONRegExp;

/***/ }),
/* 349 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {// Custom inspect property name / symbol.
	var inspect = Buffer ? __webpack_require__(345).inspect.custom || 'inspect' : 'inspect';

	/**
	 * A class representation of the BSON Symbol type.
	 *
	 * @class
	 * @deprecated
	 * @param {string} value the string representing the symbol.
	 * @return {Symbol}
	 */
	function Symbol(value) {
	  if (!(this instanceof Symbol)) return new Symbol(value);
	  this._bsontype = 'Symbol';
	  this.value = value;
	}

	/**
	 * Access the wrapped string value.
	 *
	 * @method
	 * @return {String} returns the wrapped string.
	 */
	Symbol.prototype.valueOf = function () {
	  return this.value;
	};

	/**
	 * @ignore
	 */
	Symbol.prototype.toString = function () {
	  return this.value;
	};

	/**
	 * @ignore
	 */
	Symbol.prototype[inspect] = function () {
	  return this.value;
	};

	/**
	 * @ignore
	 */
	Symbol.prototype.toJSON = function () {
	  return this.value;
	};

	module.exports = Symbol;
	module.exports.Symbol = Symbol;
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(339).Buffer))

/***/ }),
/* 350 */
/***/ (function(module, exports) {

	/**
	 * A class representation of a BSON Int32 type.
	 *
	 * @class
	 * @param {number} value the number we want to represent as an int32.
	 * @return {Int32}
	 */
	var Int32 = function (value) {
	  if (!(this instanceof Int32)) return new Int32(value);

	  this._bsontype = 'Int32';
	  this.value = value;
	};

	/**
	 * Access the number value.
	 *
	 * @method
	 * @return {number} returns the wrapped int32 number.
	 */
	Int32.prototype.valueOf = function () {
	  return this.value;
	};

	/**
	 * @ignore
	 */
	Int32.prototype.toJSON = function () {
	  return this.value;
	};

	module.exports = Int32;
	module.exports.Int32 = Int32;

/***/ }),
/* 351 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON Code type.
	 *
	 * @class
	 * @param {(string|function)} code a string or function.
	 * @param {Object} [scope] an optional scope for the function.
	 * @return {Code}
	 */
	var Code = function Code(code, scope) {
	  if (!(this instanceof Code)) return new Code(code, scope);
	  this._bsontype = 'Code';
	  this.code = code;
	  this.scope = scope;
	};

	/**
	 * @ignore
	 */
	Code.prototype.toJSON = function () {
	  return { scope: this.scope, code: this.code };
	};

	module.exports = Code;
	module.exports.Code = Code;

/***/ }),
/* 352 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var Long = __webpack_require__(335);

	var PARSE_STRING_REGEXP = /^(\+|-)?(\d+|(\d*\.\d*))?(E|e)?([-+])?(\d+)?$/;
	var PARSE_INF_REGEXP = /^(\+|-)?(Infinity|inf)$/i;
	var PARSE_NAN_REGEXP = /^(\+|-)?NaN$/i;

	var EXPONENT_MAX = 6111;
	var EXPONENT_MIN = -6176;
	var EXPONENT_BIAS = 6176;
	var MAX_DIGITS = 34;

	// Nan value bits as 32 bit values (due to lack of longs)
	var NAN_BUFFER = [0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00].reverse();
	// Infinity value bits 32 bit values (due to lack of longs)
	var INF_NEGATIVE_BUFFER = [0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00].reverse();
	var INF_POSITIVE_BUFFER = [0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00].reverse();

	var EXPONENT_REGEX = /^([-+])?(\d+)?$/;

	var utils = __webpack_require__(344);

	// Detect if the value is a digit
	var isDigit = function (value) {
	  return !isNaN(parseInt(value, 10));
	};

	// Divide two uint128 values
	var divideu128 = function (value) {
	  var DIVISOR = Long.fromNumber(1000 * 1000 * 1000);
	  var _rem = Long.fromNumber(0);
	  var i = 0;

	  if (!value.parts[0] && !value.parts[1] && !value.parts[2] && !value.parts[3]) {
	    return { quotient: value, rem: _rem };
	  }

	  for (i = 0; i <= 3; i++) {
	    // Adjust remainder to match value of next dividend
	    _rem = _rem.shiftLeft(32);
	    // Add the divided to _rem
	    _rem = _rem.add(new Long(value.parts[i], 0));
	    value.parts[i] = _rem.div(DIVISOR).low_;
	    _rem = _rem.modulo(DIVISOR);
	  }

	  return { quotient: value, rem: _rem };
	};

	// Multiply two Long values and return the 128 bit value
	var multiply64x2 = function (left, right) {
	  if (!left && !right) {
	    return { high: Long.fromNumber(0), low: Long.fromNumber(0) };
	  }

	  var leftHigh = left.shiftRightUnsigned(32);
	  var leftLow = new Long(left.getLowBits(), 0);
	  var rightHigh = right.shiftRightUnsigned(32);
	  var rightLow = new Long(right.getLowBits(), 0);

	  var productHigh = leftHigh.multiply(rightHigh);
	  var productMid = leftHigh.multiply(rightLow);
	  var productMid2 = leftLow.multiply(rightHigh);
	  var productLow = leftLow.multiply(rightLow);

	  productHigh = productHigh.add(productMid.shiftRightUnsigned(32));
	  productMid = new Long(productMid.getLowBits(), 0).add(productMid2).add(productLow.shiftRightUnsigned(32));

	  productHigh = productHigh.add(productMid.shiftRightUnsigned(32));
	  productLow = productMid.shiftLeft(32).add(new Long(productLow.getLowBits(), 0));

	  // Return the 128 bit result
	  return { high: productHigh, low: productLow };
	};

	var lessThan = function (left, right) {
	  // Make values unsigned
	  var uhleft = left.high_ >>> 0;
	  var uhright = right.high_ >>> 0;

	  // Compare high bits first
	  if (uhleft < uhright) {
	    return true;
	  } else if (uhleft === uhright) {
	    var ulleft = left.low_ >>> 0;
	    var ulright = right.low_ >>> 0;
	    if (ulleft < ulright) return true;
	  }

	  return false;
	};

	// var longtoHex = function(value) {
	//   var buffer = utils.allocBuffer(8);
	//   var index = 0;
	//   // Encode the low 64 bits of the decimal
	//   // Encode low bits
	//   buffer[index++] = value.low_ & 0xff;
	//   buffer[index++] = (value.low_ >> 8) & 0xff;
	//   buffer[index++] = (value.low_ >> 16) & 0xff;
	//   buffer[index++] = (value.low_ >> 24) & 0xff;
	//   // Encode high bits
	//   buffer[index++] = value.high_ & 0xff;
	//   buffer[index++] = (value.high_ >> 8) & 0xff;
	//   buffer[index++] = (value.high_ >> 16) & 0xff;
	//   buffer[index++] = (value.high_ >> 24) & 0xff;
	//   return buffer.reverse().toString('hex');
	// };

	// var int32toHex = function(value) {
	//   var buffer = utils.allocBuffer(4);
	//   var index = 0;
	//   // Encode the low 64 bits of the decimal
	//   // Encode low bits
	//   buffer[index++] = value & 0xff;
	//   buffer[index++] = (value >> 8) & 0xff;
	//   buffer[index++] = (value >> 16) & 0xff;
	//   buffer[index++] = (value >> 24) & 0xff;
	//   return buffer.reverse().toString('hex');
	// };

	/**
	 * A class representation of the BSON Decimal128 type.
	 *
	 * @class
	 * @param {Buffer} bytes a buffer containing the raw Decimal128 bytes.
	 * @return {Double}
	 */
	var Decimal128 = function (bytes) {
	  this._bsontype = 'Decimal128';
	  this.bytes = bytes;
	};

	/**
	 * Create a Decimal128 instance from a string representation
	 *
	 * @method
	 * @param {string} string a numeric string representation.
	 * @return {Decimal128} returns a Decimal128 instance.
	 */
	Decimal128.fromString = function (string) {
	  // Parse state tracking
	  var isNegative = false;
	  var sawRadix = false;
	  var foundNonZero = false;

	  // Total number of significant digits (no leading or trailing zero)
	  var significantDigits = 0;
	  // Total number of significand digits read
	  var nDigitsRead = 0;
	  // Total number of digits (no leading zeros)
	  var nDigits = 0;
	  // The number of the digits after radix
	  var radixPosition = 0;
	  // The index of the first non-zero in *str*
	  var firstNonZero = 0;

	  // Digits Array
	  var digits = [0];
	  // The number of digits in digits
	  var nDigitsStored = 0;
	  // Insertion pointer for digits
	  var digitsInsert = 0;
	  // The index of the first non-zero digit
	  var firstDigit = 0;
	  // The index of the last digit
	  var lastDigit = 0;

	  // Exponent
	  var exponent = 0;
	  // loop index over array
	  var i = 0;
	  // The high 17 digits of the significand
	  var significandHigh = [0, 0];
	  // The low 17 digits of the significand
	  var significandLow = [0, 0];
	  // The biased exponent
	  var biasedExponent = 0;

	  // Read index
	  var index = 0;

	  // Trim the string
	  string = string.trim();

	  // Naively prevent against REDOS attacks.
	  // TODO: implementing a custom parsing for this, or refactoring the regex would yield
	  //       further gains.
	  if (string.length >= 7000) {
	    throw new Error('' + string + ' not a valid Decimal128 string');
	  }

	  // Results
	  var stringMatch = string.match(PARSE_STRING_REGEXP);
	  var infMatch = string.match(PARSE_INF_REGEXP);
	  var nanMatch = string.match(PARSE_NAN_REGEXP);

	  // Validate the string
	  if (!stringMatch && !infMatch && !nanMatch || string.length === 0) {
	    throw new Error('' + string + ' not a valid Decimal128 string');
	  }

	  // Check if we have an illegal exponent format
	  if (stringMatch && stringMatch[4] && stringMatch[2] === undefined) {
	    throw new Error('' + string + ' not a valid Decimal128 string');
	  }

	  // Get the negative or positive sign
	  if (string[index] === '+' || string[index] === '-') {
	    isNegative = string[index++] === '-';
	  }

	  // Check if user passed Infinity or NaN
	  if (!isDigit(string[index]) && string[index] !== '.') {
	    if (string[index] === 'i' || string[index] === 'I') {
	      return new Decimal128(utils.toBuffer(isNegative ? INF_NEGATIVE_BUFFER : INF_POSITIVE_BUFFER));
	    } else if (string[index] === 'N') {
	      return new Decimal128(utils.toBuffer(NAN_BUFFER));
	    }
	  }

	  // Read all the digits
	  while (isDigit(string[index]) || string[index] === '.') {
	    if (string[index] === '.') {
	      if (sawRadix) {
	        return new Decimal128(utils.toBuffer(NAN_BUFFER));
	      }

	      sawRadix = true;
	      index = index + 1;
	      continue;
	    }

	    if (nDigitsStored < 34) {
	      if (string[index] !== '0' || foundNonZero) {
	        if (!foundNonZero) {
	          firstNonZero = nDigitsRead;
	        }

	        foundNonZero = true;

	        // Only store 34 digits
	        digits[digitsInsert++] = parseInt(string[index], 10);
	        nDigitsStored = nDigitsStored + 1;
	      }
	    }

	    if (foundNonZero) {
	      nDigits = nDigits + 1;
	    }

	    if (sawRadix) {
	      radixPosition = radixPosition + 1;
	    }

	    nDigitsRead = nDigitsRead + 1;
	    index = index + 1;
	  }

	  if (sawRadix && !nDigitsRead) {
	    throw new Error('' + string + ' not a valid Decimal128 string');
	  }

	  // Read exponent if exists
	  if (string[index] === 'e' || string[index] === 'E') {
	    // Read exponent digits
	    var match = string.substr(++index).match(EXPONENT_REGEX);

	    // No digits read
	    if (!match || !match[2]) {
	      return new Decimal128(utils.toBuffer(NAN_BUFFER));
	    }

	    // Get exponent
	    exponent = parseInt(match[0], 10);

	    // Adjust the index
	    index = index + match[0].length;
	  }

	  // Return not a number
	  if (string[index]) {
	    return new Decimal128(utils.toBuffer(NAN_BUFFER));
	  }

	  // Done reading input
	  // Find first non-zero digit in digits
	  firstDigit = 0;

	  if (!nDigitsStored) {
	    firstDigit = 0;
	    lastDigit = 0;
	    digits[0] = 0;
	    nDigits = 1;
	    nDigitsStored = 1;
	    significantDigits = 0;
	  } else {
	    lastDigit = nDigitsStored - 1;
	    significantDigits = nDigits;

	    if (exponent !== 0 && significantDigits !== 1) {
	      while (string[firstNonZero + significantDigits - 1] === '0') {
	        significantDigits = significantDigits - 1;
	      }
	    }
	  }

	  // Normalization of exponent
	  // Correct exponent based on radix position, and shift significand as needed
	  // to represent user input

	  // Overflow prevention
	  if (exponent <= radixPosition && radixPosition - exponent > 1 << 14) {
	    exponent = EXPONENT_MIN;
	  } else {
	    exponent = exponent - radixPosition;
	  }

	  // Attempt to normalize the exponent
	  while (exponent > EXPONENT_MAX) {
	    // Shift exponent to significand and decrease
	    lastDigit = lastDigit + 1;

	    if (lastDigit - firstDigit > MAX_DIGITS) {
	      // Check if we have a zero then just hard clamp, otherwise fail
	      var digitsString = digits.join('');
	      if (digitsString.match(/^0+$/)) {
	        exponent = EXPONENT_MAX;
	        break;
	      } else {
	        return new Decimal128(utils.toBuffer(isNegative ? INF_NEGATIVE_BUFFER : INF_POSITIVE_BUFFER));
	      }
	    }

	    exponent = exponent - 1;
	  }

	  while (exponent < EXPONENT_MIN || nDigitsStored < nDigits) {
	    // Shift last digit
	    if (lastDigit === 0) {
	      exponent = EXPONENT_MIN;
	      significantDigits = 0;
	      break;
	    }

	    if (nDigitsStored < nDigits) {
	      // adjust to match digits not stored
	      nDigits = nDigits - 1;
	    } else {
	      // adjust to round
	      lastDigit = lastDigit - 1;
	    }

	    if (exponent < EXPONENT_MAX) {
	      exponent = exponent + 1;
	    } else {
	      // Check if we have a zero then just hard clamp, otherwise fail
	      digitsString = digits.join('');
	      if (digitsString.match(/^0+$/)) {
	        exponent = EXPONENT_MAX;
	        break;
	      } else {
	        return new Decimal128(utils.toBuffer(isNegative ? INF_NEGATIVE_BUFFER : INF_POSITIVE_BUFFER));
	      }
	    }
	  }

	  // Round
	  // We've normalized the exponent, but might still need to round.
	  if (lastDigit - firstDigit + 1 < significantDigits && string[significantDigits] !== '0') {
	    var endOfString = nDigitsRead;

	    // If we have seen a radix point, 'string' is 1 longer than we have
	    // documented with ndigits_read, so inc the position of the first nonzero
	    // digit and the position that digits are read to.
	    if (sawRadix && exponent === EXPONENT_MIN) {
	      firstNonZero = firstNonZero + 1;
	      endOfString = endOfString + 1;
	    }

	    var roundDigit = parseInt(string[firstNonZero + lastDigit + 1], 10);
	    var roundBit = 0;

	    if (roundDigit >= 5) {
	      roundBit = 1;

	      if (roundDigit === 5) {
	        roundBit = digits[lastDigit] % 2 === 1;

	        for (i = firstNonZero + lastDigit + 2; i < endOfString; i++) {
	          if (parseInt(string[i], 10)) {
	            roundBit = 1;
	            break;
	          }
	        }
	      }
	    }

	    if (roundBit) {
	      var dIdx = lastDigit;

	      for (; dIdx >= 0; dIdx--) {
	        if (++digits[dIdx] > 9) {
	          digits[dIdx] = 0;

	          // overflowed most significant digit
	          if (dIdx === 0) {
	            if (exponent < EXPONENT_MAX) {
	              exponent = exponent + 1;
	              digits[dIdx] = 1;
	            } else {
	              return new Decimal128(utils.toBuffer(isNegative ? INF_NEGATIVE_BUFFER : INF_POSITIVE_BUFFER));
	            }
	          }
	        } else {
	          break;
	        }
	      }
	    }
	  }

	  // Encode significand
	  // The high 17 digits of the significand
	  significandHigh = Long.fromNumber(0);
	  // The low 17 digits of the significand
	  significandLow = Long.fromNumber(0);

	  // read a zero
	  if (significantDigits === 0) {
	    significandHigh = Long.fromNumber(0);
	    significandLow = Long.fromNumber(0);
	  } else if (lastDigit - firstDigit < 17) {
	    dIdx = firstDigit;
	    significandLow = Long.fromNumber(digits[dIdx++]);
	    significandHigh = new Long(0, 0);

	    for (; dIdx <= lastDigit; dIdx++) {
	      significandLow = significandLow.multiply(Long.fromNumber(10));
	      significandLow = significandLow.add(Long.fromNumber(digits[dIdx]));
	    }
	  } else {
	    dIdx = firstDigit;
	    significandHigh = Long.fromNumber(digits[dIdx++]);

	    for (; dIdx <= lastDigit - 17; dIdx++) {
	      significandHigh = significandHigh.multiply(Long.fromNumber(10));
	      significandHigh = significandHigh.add(Long.fromNumber(digits[dIdx]));
	    }

	    significandLow = Long.fromNumber(digits[dIdx++]);

	    for (; dIdx <= lastDigit; dIdx++) {
	      significandLow = significandLow.multiply(Long.fromNumber(10));
	      significandLow = significandLow.add(Long.fromNumber(digits[dIdx]));
	    }
	  }

	  var significand = multiply64x2(significandHigh, Long.fromString('100000000000000000'));

	  significand.low = significand.low.add(significandLow);

	  if (lessThan(significand.low, significandLow)) {
	    significand.high = significand.high.add(Long.fromNumber(1));
	  }

	  // Biased exponent
	  biasedExponent = exponent + EXPONENT_BIAS;
	  var dec = { low: Long.fromNumber(0), high: Long.fromNumber(0) };

	  // Encode combination, exponent, and significand.
	  if (significand.high.shiftRightUnsigned(49).and(Long.fromNumber(1)).equals(Long.fromNumber)) {
	    // Encode '11' into bits 1 to 3
	    dec.high = dec.high.or(Long.fromNumber(0x3).shiftLeft(61));
	    dec.high = dec.high.or(Long.fromNumber(biasedExponent).and(Long.fromNumber(0x3fff).shiftLeft(47)));
	    dec.high = dec.high.or(significand.high.and(Long.fromNumber(0x7fffffffffff)));
	  } else {
	    dec.high = dec.high.or(Long.fromNumber(biasedExponent & 0x3fff).shiftLeft(49));
	    dec.high = dec.high.or(significand.high.and(Long.fromNumber(0x1ffffffffffff)));
	  }

	  dec.low = significand.low;

	  // Encode sign
	  if (isNegative) {
	    dec.high = dec.high.or(Long.fromString('9223372036854775808'));
	  }

	  // Encode into a buffer
	  var buffer = utils.allocBuffer(16);
	  index = 0;

	  // Encode the low 64 bits of the decimal
	  // Encode low bits
	  buffer[index++] = dec.low.low_ & 0xff;
	  buffer[index++] = dec.low.low_ >> 8 & 0xff;
	  buffer[index++] = dec.low.low_ >> 16 & 0xff;
	  buffer[index++] = dec.low.low_ >> 24 & 0xff;
	  // Encode high bits
	  buffer[index++] = dec.low.high_ & 0xff;
	  buffer[index++] = dec.low.high_ >> 8 & 0xff;
	  buffer[index++] = dec.low.high_ >> 16 & 0xff;
	  buffer[index++] = dec.low.high_ >> 24 & 0xff;

	  // Encode the high 64 bits of the decimal
	  // Encode low bits
	  buffer[index++] = dec.high.low_ & 0xff;
	  buffer[index++] = dec.high.low_ >> 8 & 0xff;
	  buffer[index++] = dec.high.low_ >> 16 & 0xff;
	  buffer[index++] = dec.high.low_ >> 24 & 0xff;
	  // Encode high bits
	  buffer[index++] = dec.high.high_ & 0xff;
	  buffer[index++] = dec.high.high_ >> 8 & 0xff;
	  buffer[index++] = dec.high.high_ >> 16 & 0xff;
	  buffer[index++] = dec.high.high_ >> 24 & 0xff;

	  // Return the new Decimal128
	  return new Decimal128(buffer);
	};

	// Extract least significant 5 bits
	var COMBINATION_MASK = 0x1f;
	// Extract least significant 14 bits
	var EXPONENT_MASK = 0x3fff;
	// Value of combination field for Inf
	var COMBINATION_INFINITY = 30;
	// Value of combination field for NaN
	var COMBINATION_NAN = 31;
	// Value of combination field for NaN
	// var COMBINATION_SNAN = 32;
	// decimal128 exponent bias
	EXPONENT_BIAS = 6176;

	/**
	 * Create a string representation of the raw Decimal128 value
	 *
	 * @method
	 * @return {string} returns a Decimal128 string representation.
	 */
	Decimal128.prototype.toString = function () {
	  // Note: bits in this routine are referred to starting at 0,
	  // from the sign bit, towards the coefficient.

	  // bits 0 - 31
	  var high;
	  // bits 32 - 63
	  var midh;
	  // bits 64 - 95
	  var midl;
	  // bits 96 - 127
	  var low;
	  // bits 1 - 5
	  var combination;
	  // decoded biased exponent (14 bits)
	  var biased_exponent;
	  // the number of significand digits
	  var significand_digits = 0;
	  // the base-10 digits in the significand
	  var significand = new Array(36);
	  for (var i = 0; i < significand.length; i++) significand[i] = 0;
	  // read pointer into significand
	  var index = 0;

	  // unbiased exponent
	  var exponent;
	  // the exponent if scientific notation is used
	  var scientific_exponent;

	  // true if the number is zero
	  var is_zero = false;

	  // the most signifcant significand bits (50-46)
	  var significand_msb;
	  // temporary storage for significand decoding
	  var significand128 = { parts: new Array(4) };
	  // indexing variables
	  i;
	  var j, k;

	  // Output string
	  var string = [];

	  // Unpack index
	  index = 0;

	  // Buffer reference
	  var buffer = this.bytes;

	  // Unpack the low 64bits into a long
	  low = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	  midl = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;

	  // Unpack the high 64bits into a long
	  midh = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	  high = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;

	  // Unpack index
	  index = 0;

	  // Create the state of the decimal
	  var dec = {
	    low: new Long(low, midl),
	    high: new Long(midh, high)
	  };

	  if (dec.high.lessThan(Long.ZERO)) {
	    string.push('-');
	  }

	  // Decode combination field and exponent
	  combination = high >> 26 & COMBINATION_MASK;

	  if (combination >> 3 === 3) {
	    // Check for 'special' values
	    if (combination === COMBINATION_INFINITY) {
	      return string.join('') + 'Infinity';
	    } else if (combination === COMBINATION_NAN) {
	      return 'NaN';
	    } else {
	      biased_exponent = high >> 15 & EXPONENT_MASK;
	      significand_msb = 0x08 + (high >> 14 & 0x01);
	    }
	  } else {
	    significand_msb = high >> 14 & 0x07;
	    biased_exponent = high >> 17 & EXPONENT_MASK;
	  }

	  exponent = biased_exponent - EXPONENT_BIAS;

	  // Create string of significand digits

	  // Convert the 114-bit binary number represented by
	  // (significand_high, significand_low) to at most 34 decimal
	  // digits through modulo and division.
	  significand128.parts[0] = (high & 0x3fff) + ((significand_msb & 0xf) << 14);
	  significand128.parts[1] = midh;
	  significand128.parts[2] = midl;
	  significand128.parts[3] = low;

	  if (significand128.parts[0] === 0 && significand128.parts[1] === 0 && significand128.parts[2] === 0 && significand128.parts[3] === 0) {
	    is_zero = true;
	  } else {
	    for (k = 3; k >= 0; k--) {
	      var least_digits = 0;
	      // Peform the divide
	      var result = divideu128(significand128);
	      significand128 = result.quotient;
	      least_digits = result.rem.low_;

	      // We now have the 9 least significant digits (in base 2).
	      // Convert and output to string.
	      if (!least_digits) continue;

	      for (j = 8; j >= 0; j--) {
	        // significand[k * 9 + j] = Math.round(least_digits % 10);
	        significand[k * 9 + j] = least_digits % 10;
	        // least_digits = Math.round(least_digits / 10);
	        least_digits = Math.floor(least_digits / 10);
	      }
	    }
	  }

	  // Output format options:
	  // Scientific - [-]d.dddE(+/-)dd or [-]dE(+/-)dd
	  // Regular    - ddd.ddd

	  if (is_zero) {
	    significand_digits = 1;
	    significand[index] = 0;
	  } else {
	    significand_digits = 36;
	    i = 0;

	    while (!significand[index]) {
	      i++;
	      significand_digits = significand_digits - 1;
	      index = index + 1;
	    }
	  }

	  scientific_exponent = significand_digits - 1 + exponent;

	  // The scientific exponent checks are dictated by the string conversion
	  // specification and are somewhat arbitrary cutoffs.
	  //
	  // We must check exponent > 0, because if this is the case, the number
	  // has trailing zeros.  However, we *cannot* output these trailing zeros,
	  // because doing so would change the precision of the value, and would
	  // change stored data if the string converted number is round tripped.

	  if (scientific_exponent >= 34 || scientific_exponent <= -7 || exponent > 0) {
	    // Scientific format
	    string.push(significand[index++]);
	    significand_digits = significand_digits - 1;

	    if (significand_digits) {
	      string.push('.');
	    }

	    for (i = 0; i < significand_digits; i++) {
	      string.push(significand[index++]);
	    }

	    // Exponent
	    string.push('E');
	    if (scientific_exponent > 0) {
	      string.push('+' + scientific_exponent);
	    } else {
	      string.push(scientific_exponent);
	    }
	  } else {
	    // Regular format with no decimal place
	    if (exponent >= 0) {
	      for (i = 0; i < significand_digits; i++) {
	        string.push(significand[index++]);
	      }
	    } else {
	      var radix_position = significand_digits + exponent;

	      // non-zero digits before radix
	      if (radix_position > 0) {
	        for (i = 0; i < radix_position; i++) {
	          string.push(significand[index++]);
	        }
	      } else {
	        string.push('0');
	      }

	      string.push('.');
	      // add leading zeros after radix
	      while (radix_position++ < 0) {
	        string.push('0');
	      }

	      for (i = 0; i < significand_digits - Math.max(radix_position - 1, 0); i++) {
	        string.push(significand[index++]);
	      }
	    }
	  }

	  return string.join('');
	};

	Decimal128.prototype.toJSON = function () {
	  return { $numberDecimal: this.toString() };
	};

	module.exports = Decimal128;
	module.exports.Decimal128 = Decimal128;

/***/ }),
/* 353 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON MinKey type.
	 *
	 * @class
	 * @return {MinKey} A MinKey instance
	 */
	function MinKey() {
	  if (!(this instanceof MinKey)) return new MinKey();

	  this._bsontype = 'MinKey';
	}

	module.exports = MinKey;
	module.exports.MinKey = MinKey;

/***/ }),
/* 354 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON MaxKey type.
	 *
	 * @class
	 * @return {MaxKey} A MaxKey instance
	 */
	function MaxKey() {
	  if (!(this instanceof MaxKey)) return new MaxKey();

	  this._bsontype = 'MaxKey';
	}

	module.exports = MaxKey;
	module.exports.MaxKey = MaxKey;

/***/ }),
/* 355 */
/***/ (function(module, exports) {

	/**
	 * A class representation of the BSON DBRef type.
	 *
	 * @class
	 * @param {string} namespace the collection name.
	 * @param {ObjectID} oid the reference ObjectID.
	 * @param {string} [db] optional db name, if omitted the reference is local to the current db.
	 * @return {DBRef}
	 */
	function DBRef(namespace, oid, db) {
	  if (!(this instanceof DBRef)) return new DBRef(namespace, oid, db);

	  this._bsontype = 'DBRef';
	  this.namespace = namespace;
	  this.oid = oid;
	  this.db = db;
	}

	/**
	 * @ignore
	 * @api private
	 */
	DBRef.prototype.toJSON = function () {
	  return {
	    $ref: this.namespace,
	    $id: this.oid,
	    $db: this.db == null ? '' : this.db
	  };
	};

	module.exports = DBRef;
	module.exports.DBRef = DBRef;

/***/ }),
/* 356 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(global) {/**
	 * Module dependencies.
	 * @ignore
	 */

	// Test if we're in Node via presence of "global" not absence of "window"
	// to support hybrid environments like Electron
	if (typeof global !== 'undefined') {
	  var Buffer = __webpack_require__(339).Buffer; // TODO just use global Buffer
	}

	var utils = __webpack_require__(344);

	/**
	 * A class representation of the BSON Binary type.
	 *
	 * Sub types
	 *  - **BSON.BSON_BINARY_SUBTYPE_DEFAULT**, default BSON type.
	 *  - **BSON.BSON_BINARY_SUBTYPE_FUNCTION**, BSON function type.
	 *  - **BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY**, BSON byte array type.
	 *  - **BSON.BSON_BINARY_SUBTYPE_UUID**, BSON uuid type.
	 *  - **BSON.BSON_BINARY_SUBTYPE_MD5**, BSON md5 type.
	 *  - **BSON.BSON_BINARY_SUBTYPE_USER_DEFINED**, BSON user defined type.
	 *
	 * @class
	 * @param {Buffer} buffer a buffer object containing the binary data.
	 * @param {Number} [subType] the option binary type.
	 * @return {Binary}
	 */
	function Binary(buffer, subType) {
	  if (!(this instanceof Binary)) return new Binary(buffer, subType);

	  if (buffer != null && !(typeof buffer === 'string') && !Buffer.isBuffer(buffer) && !(buffer instanceof Uint8Array) && !Array.isArray(buffer)) {
	    throw new Error('only String, Buffer, Uint8Array or Array accepted');
	  }

	  this._bsontype = 'Binary';

	  if (buffer instanceof Number) {
	    this.sub_type = buffer;
	    this.position = 0;
	  } else {
	    this.sub_type = subType == null ? BSON_BINARY_SUBTYPE_DEFAULT : subType;
	    this.position = 0;
	  }

	  if (buffer != null && !(buffer instanceof Number)) {
	    // Only accept Buffer, Uint8Array or Arrays
	    if (typeof buffer === 'string') {
	      // Different ways of writing the length of the string for the different types
	      if (typeof Buffer !== 'undefined') {
	        this.buffer = utils.toBuffer(buffer);
	      } else if (typeof Uint8Array !== 'undefined' || Object.prototype.toString.call(buffer) === '[object Array]') {
	        this.buffer = writeStringToArray(buffer);
	      } else {
	        throw new Error('only String, Buffer, Uint8Array or Array accepted');
	      }
	    } else {
	      this.buffer = buffer;
	    }
	    this.position = buffer.length;
	  } else {
	    if (typeof Buffer !== 'undefined') {
	      this.buffer = utils.allocBuffer(Binary.BUFFER_SIZE);
	    } else if (typeof Uint8Array !== 'undefined') {
	      this.buffer = new Uint8Array(new ArrayBuffer(Binary.BUFFER_SIZE));
	    } else {
	      this.buffer = new Array(Binary.BUFFER_SIZE);
	    }
	    // Set position to start of buffer
	    this.position = 0;
	  }
	}

	/**
	 * Updates this binary with byte_value.
	 *
	 * @method
	 * @param {string} byte_value a single byte we wish to write.
	 */
	Binary.prototype.put = function put(byte_value) {
	  // If it's a string and a has more than one character throw an error
	  if (byte_value['length'] != null && typeof byte_value !== 'number' && byte_value.length !== 1) throw new Error('only accepts single character String, Uint8Array or Array');
	  if (typeof byte_value !== 'number' && byte_value < 0 || byte_value > 255) throw new Error('only accepts number in a valid unsigned byte range 0-255');

	  // Decode the byte value once
	  var decoded_byte = null;
	  if (typeof byte_value === 'string') {
	    decoded_byte = byte_value.charCodeAt(0);
	  } else if (byte_value['length'] != null) {
	    decoded_byte = byte_value[0];
	  } else {
	    decoded_byte = byte_value;
	  }

	  if (this.buffer.length > this.position) {
	    this.buffer[this.position++] = decoded_byte;
	  } else {
	    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(this.buffer)) {
	      // Create additional overflow buffer
	      var buffer = utils.allocBuffer(Binary.BUFFER_SIZE + this.buffer.length);
	      // Combine the two buffers together
	      this.buffer.copy(buffer, 0, 0, this.buffer.length);
	      this.buffer = buffer;
	      this.buffer[this.position++] = decoded_byte;
	    } else {
	      buffer = null;
	      // Create a new buffer (typed or normal array)
	      if (Object.prototype.toString.call(this.buffer) === '[object Uint8Array]') {
	        buffer = new Uint8Array(new ArrayBuffer(Binary.BUFFER_SIZE + this.buffer.length));
	      } else {
	        buffer = new Array(Binary.BUFFER_SIZE + this.buffer.length);
	      }

	      // We need to copy all the content to the new array
	      for (var i = 0; i < this.buffer.length; i++) {
	        buffer[i] = this.buffer[i];
	      }

	      // Reassign the buffer
	      this.buffer = buffer;
	      // Write the byte
	      this.buffer[this.position++] = decoded_byte;
	    }
	  }
	};

	/**
	 * Writes a buffer or string to the binary.
	 *
	 * @method
	 * @param {(Buffer|string)} string a string or buffer to be written to the Binary BSON object.
	 * @param {number} offset specify the binary of where to write the content.
	 * @return {null}
	 */
	Binary.prototype.write = function write(string, offset) {
	  offset = typeof offset === 'number' ? offset : this.position;

	  // If the buffer is to small let's extend the buffer
	  if (this.buffer.length < offset + string.length) {
	    var buffer = null;
	    // If we are in node.js
	    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(this.buffer)) {
	      buffer = utils.allocBuffer(this.buffer.length + string.length);
	      this.buffer.copy(buffer, 0, 0, this.buffer.length);
	    } else if (Object.prototype.toString.call(this.buffer) === '[object Uint8Array]') {
	      // Create a new buffer
	      buffer = new Uint8Array(new ArrayBuffer(this.buffer.length + string.length));
	      // Copy the content
	      for (var i = 0; i < this.position; i++) {
	        buffer[i] = this.buffer[i];
	      }
	    }

	    // Assign the new buffer
	    this.buffer = buffer;
	  }

	  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(string) && Buffer.isBuffer(this.buffer)) {
	    string.copy(this.buffer, offset, 0, string.length);
	    this.position = offset + string.length > this.position ? offset + string.length : this.position;
	    // offset = string.length
	  } else if (typeof Buffer !== 'undefined' && typeof string === 'string' && Buffer.isBuffer(this.buffer)) {
	    this.buffer.write(string, offset, 'binary');
	    this.position = offset + string.length > this.position ? offset + string.length : this.position;
	    // offset = string.length;
	  } else if (Object.prototype.toString.call(string) === '[object Uint8Array]' || Object.prototype.toString.call(string) === '[object Array]' && typeof string !== 'string') {
	    for (i = 0; i < string.length; i++) {
	      this.buffer[offset++] = string[i];
	    }

	    this.position = offset > this.position ? offset : this.position;
	  } else if (typeof string === 'string') {
	    for (i = 0; i < string.length; i++) {
	      this.buffer[offset++] = string.charCodeAt(i);
	    }

	    this.position = offset > this.position ? offset : this.position;
	  }
	};

	/**
	 * Reads **length** bytes starting at **position**.
	 *
	 * @method
	 * @param {number} position read from the given position in the Binary.
	 * @param {number} length the number of bytes to read.
	 * @return {Buffer}
	 */
	Binary.prototype.read = function read(position, length) {
	  length = length && length > 0 ? length : this.position;

	  // Let's return the data based on the type we have
	  if (this.buffer['slice']) {
	    return this.buffer.slice(position, position + length);
	  } else {
	    // Create a buffer to keep the result
	    var buffer = typeof Uint8Array !== 'undefined' ? new Uint8Array(new ArrayBuffer(length)) : new Array(length);
	    for (var i = 0; i < length; i++) {
	      buffer[i] = this.buffer[position++];
	    }
	  }
	  // Return the buffer
	  return buffer;
	};

	/**
	 * Returns the value of this binary as a string.
	 *
	 * @method
	 * @return {string}
	 */
	Binary.prototype.value = function value(asRaw) {
	  asRaw = asRaw == null ? false : asRaw;

	  // Optimize to serialize for the situation where the data == size of buffer
	  if (asRaw && typeof Buffer !== 'undefined' && Buffer.isBuffer(this.buffer) && this.buffer.length === this.position) return this.buffer;

	  // If it's a node.js buffer object
	  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(this.buffer)) {
	    return asRaw ? this.buffer.slice(0, this.position) : this.buffer.toString('binary', 0, this.position);
	  } else {
	    if (asRaw) {
	      // we support the slice command use it
	      if (this.buffer['slice'] != null) {
	        return this.buffer.slice(0, this.position);
	      } else {
	        // Create a new buffer to copy content to
	        var newBuffer = Object.prototype.toString.call(this.buffer) === '[object Uint8Array]' ? new Uint8Array(new ArrayBuffer(this.position)) : new Array(this.position);
	        // Copy content
	        for (var i = 0; i < this.position; i++) {
	          newBuffer[i] = this.buffer[i];
	        }
	        // Return the buffer
	        return newBuffer;
	      }
	    } else {
	      return convertArraytoUtf8BinaryString(this.buffer, 0, this.position);
	    }
	  }
	};

	/**
	 * Length.
	 *
	 * @method
	 * @return {number} the length of the binary.
	 */
	Binary.prototype.length = function length() {
	  return this.position;
	};

	/**
	 * @ignore
	 */
	Binary.prototype.toJSON = function () {
	  return this.buffer != null ? this.buffer.toString('base64') : '';
	};

	/**
	 * @ignore
	 */
	Binary.prototype.toString = function (format) {
	  return this.buffer != null ? this.buffer.slice(0, this.position).toString(format) : '';
	};

	/**
	 * Binary default subtype
	 * @ignore
	 */
	var BSON_BINARY_SUBTYPE_DEFAULT = 0;

	/**
	 * @ignore
	 */
	var writeStringToArray = function (data) {
	  // Create a buffer
	  var buffer = typeof Uint8Array !== 'undefined' ? new Uint8Array(new ArrayBuffer(data.length)) : new Array(data.length);
	  // Write the content to the buffer
	  for (var i = 0; i < data.length; i++) {
	    buffer[i] = data.charCodeAt(i);
	  }
	  // Write the string to the buffer
	  return buffer;
	};

	/**
	 * Convert Array ot Uint8Array to Binary String
	 *
	 * @ignore
	 */
	var convertArraytoUtf8BinaryString = function (byteArray, startIndex, endIndex) {
	  var result = '';
	  for (var i = startIndex; i < endIndex; i++) {
	    result = result + String.fromCharCode(byteArray[i]);
	  }
	  return result;
	};

	Binary.BUFFER_SIZE = 256;

	/**
	 * Default BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_DEFAULT = 0;
	/**
	 * Function BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_FUNCTION = 1;
	/**
	 * Byte Array BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_BYTE_ARRAY = 2;
	/**
	 * OLD UUID BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_UUID_OLD = 3;
	/**
	 * UUID BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_UUID = 4;
	/**
	 * MD5 BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_MD5 = 5;
	/**
	 * User BSON type
	 *
	 * @classconstant SUBTYPE_DEFAULT
	 **/
	Binary.SUBTYPE_USER_DEFINED = 128;

	/**
	 * Expose.
	 */
	module.exports = Binary;
	module.exports.Binary = Binary;
	/* WEBPACK VAR INJECTION */}.call(exports, (function() { return this; }())))

/***/ }),
/* 357 */
/***/ (function(module, exports, __webpack_require__) {

	'use strict';

	var Long = __webpack_require__(335).Long,
	    Double = __webpack_require__(336).Double,
	    Timestamp = __webpack_require__(337).Timestamp,
	    ObjectID = __webpack_require__(338).ObjectID,
	    Symbol = __webpack_require__(349).Symbol,
	    Code = __webpack_require__(351).Code,
	    MinKey = __webpack_require__(353).MinKey,
	    MaxKey = __webpack_require__(354).MaxKey,
	    Decimal128 = __webpack_require__(352),
	    Int32 = __webpack_require__(350),
	    DBRef = __webpack_require__(355).DBRef,
	    BSONRegExp = __webpack_require__(348).BSONRegExp,
	    Binary = __webpack_require__(356).Binary;

	var utils = __webpack_require__(344);

	var deserialize = function (buffer, options, isArray) {
	  options = options == null ? {} : options;
	  var index = options && options.index ? options.index : 0;
	  // Read the document size
	  var size = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;

	  // Ensure buffer is valid size
	  if (size < 5 || buffer.length < size || size + index > buffer.length) {
	    throw new Error('corrupt bson message');
	  }

	  // Illegal end value
	  if (buffer[index + size - 1] !== 0) {
	    throw new Error("One object, sized correctly, with a spot for an EOO, but the EOO isn't 0x00");
	  }

	  // Start deserializtion
	  return deserializeObject(buffer, index, options, isArray);
	};

	var deserializeObject = function (buffer, index, options, isArray) {
	  var evalFunctions = options['evalFunctions'] == null ? false : options['evalFunctions'];
	  var cacheFunctions = options['cacheFunctions'] == null ? false : options['cacheFunctions'];
	  var cacheFunctionsCrc32 = options['cacheFunctionsCrc32'] == null ? false : options['cacheFunctionsCrc32'];

	  if (!cacheFunctionsCrc32) var crc32 = null;

	  var fieldsAsRaw = options['fieldsAsRaw'] == null ? null : options['fieldsAsRaw'];

	  // Return raw bson buffer instead of parsing it
	  var raw = options['raw'] == null ? false : options['raw'];

	  // Return BSONRegExp objects instead of native regular expressions
	  var bsonRegExp = typeof options['bsonRegExp'] === 'boolean' ? options['bsonRegExp'] : false;

	  // Controls the promotion of values vs wrapper classes
	  var promoteBuffers = options['promoteBuffers'] == null ? false : options['promoteBuffers'];
	  var promoteLongs = options['promoteLongs'] == null ? true : options['promoteLongs'];
	  var promoteValues = options['promoteValues'] == null ? true : options['promoteValues'];

	  // Set the start index
	  var startIndex = index;

	  // Validate that we have at least 4 bytes of buffer
	  if (buffer.length < 5) throw new Error('corrupt bson message < 5 bytes long');

	  // Read the document size
	  var size = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;

	  // Ensure buffer is valid size
	  if (size < 5 || size > buffer.length) throw new Error('corrupt bson message');

	  // Create holding object
	  var object = isArray ? [] : {};
	  // Used for arrays to skip having to perform utf8 decoding
	  var arrayIndex = 0;

	  var done = false;

	  // While we have more left data left keep parsing
	  // while (buffer[index + 1] !== 0) {
	  while (!done) {
	    // Read the type
	    var elementType = buffer[index++];
	    // If we get a zero it's the last byte, exit
	    if (elementType === 0) break;

	    // Get the start search index
	    var i = index;
	    // Locate the end of the c string
	    while (buffer[i] !== 0x00 && i < buffer.length) {
	      i++;
	    }

	    // If are at the end of the buffer there is a problem with the document
	    if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
	    var name = isArray ? arrayIndex++ : buffer.toString('utf8', index, i);

	    index = i + 1;

	    if (elementType === BSON.BSON_DATA_STRING) {
	      var stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      if (stringSize <= 0 || stringSize > buffer.length - index || buffer[index + stringSize - 1] !== 0) throw new Error('bad string length in bson');
	      object[name] = buffer.toString('utf8', index, index + stringSize - 1);
	      index = index + stringSize;
	    } else if (elementType === BSON.BSON_DATA_OID) {
	      var oid = utils.allocBuffer(12);
	      buffer.copy(oid, 0, index, index + 12);
	      object[name] = new ObjectID(oid);
	      index = index + 12;
	    } else if (elementType === BSON.BSON_DATA_INT && promoteValues === false) {
	      object[name] = new Int32(buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24);
	    } else if (elementType === BSON.BSON_DATA_INT) {
	      object[name] = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	    } else if (elementType === BSON.BSON_DATA_NUMBER && promoteValues === false) {
	      object[name] = new Double(buffer.readDoubleLE(index));
	      index = index + 8;
	    } else if (elementType === BSON.BSON_DATA_NUMBER) {
	      object[name] = buffer.readDoubleLE(index);
	      index = index + 8;
	    } else if (elementType === BSON.BSON_DATA_DATE) {
	      var lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      var highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      object[name] = new Date(new Long(lowBits, highBits).toNumber());
	    } else if (elementType === BSON.BSON_DATA_BOOLEAN) {
	      if (buffer[index] !== 0 && buffer[index] !== 1) throw new Error('illegal boolean type value');
	      object[name] = buffer[index++] === 1;
	    } else if (elementType === BSON.BSON_DATA_OBJECT) {
	      var _index = index;
	      var objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
	      if (objectSize <= 0 || objectSize > buffer.length - index) throw new Error('bad embedded document length in bson');

	      // We have a raw value
	      if (raw) {
	        object[name] = buffer.slice(index, index + objectSize);
	      } else {
	        object[name] = deserializeObject(buffer, _index, options, false);
	      }

	      index = index + objectSize;
	    } else if (elementType === BSON.BSON_DATA_ARRAY) {
	      _index = index;
	      objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
	      var arrayOptions = options;

	      // Stop index
	      var stopIndex = index + objectSize;

	      // All elements of array to be returned as raw bson
	      if (fieldsAsRaw && fieldsAsRaw[name]) {
	        arrayOptions = {};
	        for (var n in options) arrayOptions[n] = options[n];
	        arrayOptions['raw'] = true;
	      }

	      object[name] = deserializeObject(buffer, _index, arrayOptions, true);
	      index = index + objectSize;

	      if (buffer[index - 1] !== 0) throw new Error('invalid array terminator byte');
	      if (index !== stopIndex) throw new Error('corrupted array bson');
	    } else if (elementType === BSON.BSON_DATA_UNDEFINED) {
	      object[name] = undefined;
	    } else if (elementType === BSON.BSON_DATA_NULL) {
	      object[name] = null;
	    } else if (elementType === BSON.BSON_DATA_LONG) {
	      // Unpack the low and high bits
	      lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      var long = new Long(lowBits, highBits);
	      // Promote the long if possible
	      if (promoteLongs && promoteValues === true) {
	        object[name] = long.lessThanOrEqual(JS_INT_MAX_LONG) && long.greaterThanOrEqual(JS_INT_MIN_LONG) ? long.toNumber() : long;
	      } else {
	        object[name] = long;
	      }
	    } else if (elementType === BSON.BSON_DATA_DECIMAL128) {
	      // Buffer to contain the decimal bytes
	      var bytes = utils.allocBuffer(16);
	      // Copy the next 16 bytes into the bytes buffer
	      buffer.copy(bytes, 0, index, index + 16);
	      // Update index
	      index = index + 16;
	      // Assign the new Decimal128 value
	      var decimal128 = new Decimal128(bytes);
	      // If we have an alternative mapper use that
	      object[name] = decimal128.toObject ? decimal128.toObject() : decimal128;
	    } else if (elementType === BSON.BSON_DATA_BINARY) {
	      var binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      var totalBinarySize = binarySize;
	      var subType = buffer[index++];

	      // Did we have a negative binary size, throw
	      if (binarySize < 0) throw new Error('Negative binary type element size found');

	      // Is the length longer than the document
	      if (binarySize > buffer.length) throw new Error('Binary type size larger than document size');

	      // Decode as raw Buffer object if options specifies it
	      if (buffer['slice'] != null) {
	        // If we have subtype 2 skip the 4 bytes for the size
	        if (subType === Binary.SUBTYPE_BYTE_ARRAY) {
	          binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	          if (binarySize < 0) throw new Error('Negative binary type element size found for subtype 0x02');
	          if (binarySize > totalBinarySize - 4) throw new Error('Binary type with subtype 0x02 contains to long binary size');
	          if (binarySize < totalBinarySize - 4) throw new Error('Binary type with subtype 0x02 contains to short binary size');
	        }

	        if (promoteBuffers && promoteValues) {
	          object[name] = buffer.slice(index, index + binarySize);
	        } else {
	          object[name] = new Binary(buffer.slice(index, index + binarySize), subType);
	        }
	      } else {
	        var _buffer = typeof Uint8Array !== 'undefined' ? new Uint8Array(new ArrayBuffer(binarySize)) : new Array(binarySize);
	        // If we have subtype 2 skip the 4 bytes for the size
	        if (subType === Binary.SUBTYPE_BYTE_ARRAY) {
	          binarySize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	          if (binarySize < 0) throw new Error('Negative binary type element size found for subtype 0x02');
	          if (binarySize > totalBinarySize - 4) throw new Error('Binary type with subtype 0x02 contains to long binary size');
	          if (binarySize < totalBinarySize - 4) throw new Error('Binary type with subtype 0x02 contains to short binary size');
	        }

	        // Copy the data
	        for (i = 0; i < binarySize; i++) {
	          _buffer[i] = buffer[index + i];
	        }

	        if (promoteBuffers && promoteValues) {
	          object[name] = _buffer;
	        } else {
	          object[name] = new Binary(_buffer, subType);
	        }
	      }

	      // Update the index
	      index = index + binarySize;
	    } else if (elementType === BSON.BSON_DATA_REGEXP && bsonRegExp === false) {
	      // Get the start search index
	      i = index;
	      // Locate the end of the c string
	      while (buffer[i] !== 0x00 && i < buffer.length) {
	        i++;
	      }
	      // If are at the end of the buffer there is a problem with the document
	      if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
	      // Return the C string
	      var source = buffer.toString('utf8', index, i);
	      // Create the regexp
	      index = i + 1;

	      // Get the start search index
	      i = index;
	      // Locate the end of the c string
	      while (buffer[i] !== 0x00 && i < buffer.length) {
	        i++;
	      }
	      // If are at the end of the buffer there is a problem with the document
	      if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
	      // Return the C string
	      var regExpOptions = buffer.toString('utf8', index, i);
	      index = i + 1;

	      // For each option add the corresponding one for javascript
	      var optionsArray = new Array(regExpOptions.length);

	      // Parse options
	      for (i = 0; i < regExpOptions.length; i++) {
	        switch (regExpOptions[i]) {
	          case 'm':
	            optionsArray[i] = 'm';
	            break;
	          case 's':
	            optionsArray[i] = 'g';
	            break;
	          case 'i':
	            optionsArray[i] = 'i';
	            break;
	        }
	      }

	      object[name] = new RegExp(source, optionsArray.join(''));
	    } else if (elementType === BSON.BSON_DATA_REGEXP && bsonRegExp === true) {
	      // Get the start search index
	      i = index;
	      // Locate the end of the c string
	      while (buffer[i] !== 0x00 && i < buffer.length) {
	        i++;
	      }
	      // If are at the end of the buffer there is a problem with the document
	      if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
	      // Return the C string
	      source = buffer.toString('utf8', index, i);
	      index = i + 1;

	      // Get the start search index
	      i = index;
	      // Locate the end of the c string
	      while (buffer[i] !== 0x00 && i < buffer.length) {
	        i++;
	      }
	      // If are at the end of the buffer there is a problem with the document
	      if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
	      // Return the C string
	      regExpOptions = buffer.toString('utf8', index, i);
	      index = i + 1;

	      // Set the object
	      object[name] = new BSONRegExp(source, regExpOptions);
	    } else if (elementType === BSON.BSON_DATA_SYMBOL) {
	      stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      if (stringSize <= 0 || stringSize > buffer.length - index || buffer[index + stringSize - 1] !== 0) throw new Error('bad string length in bson');
	      object[name] = new Symbol(buffer.toString('utf8', index, index + stringSize - 1));
	      index = index + stringSize;
	    } else if (elementType === BSON.BSON_DATA_TIMESTAMP) {
	      lowBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      highBits = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      object[name] = new Timestamp(lowBits, highBits);
	    } else if (elementType === BSON.BSON_DATA_MIN_KEY) {
	      object[name] = new MinKey();
	    } else if (elementType === BSON.BSON_DATA_MAX_KEY) {
	      object[name] = new MaxKey();
	    } else if (elementType === BSON.BSON_DATA_CODE) {
	      stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      if (stringSize <= 0 || stringSize > buffer.length - index || buffer[index + stringSize - 1] !== 0) throw new Error('bad string length in bson');
	      var functionString = buffer.toString('utf8', index, index + stringSize - 1);

	      // If we are evaluating the functions
	      if (evalFunctions) {
	        // If we have cache enabled let's look for the md5 of the function in the cache
	        if (cacheFunctions) {
	          var hash = cacheFunctionsCrc32 ? crc32(functionString) : functionString;
	          // Got to do this to avoid V8 deoptimizing the call due to finding eval
	          object[name] = isolateEvalWithHash(functionCache, hash, functionString, object);
	        } else {
	          object[name] = isolateEval(functionString);
	        }
	      } else {
	        object[name] = new Code(functionString);
	      }

	      // Update parse index position
	      index = index + stringSize;
	    } else if (elementType === BSON.BSON_DATA_CODE_W_SCOPE) {
	      var totalSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;

	      // Element cannot be shorter than totalSize + stringSize + documentSize + terminator
	      if (totalSize < 4 + 4 + 4 + 1) {
	        throw new Error('code_w_scope total size shorter minimum expected length');
	      }

	      // Get the code string size
	      stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      // Check if we have a valid string
	      if (stringSize <= 0 || stringSize > buffer.length - index || buffer[index + stringSize - 1] !== 0) throw new Error('bad string length in bson');

	      // Javascript function
	      functionString = buffer.toString('utf8', index, index + stringSize - 1);
	      // Update parse index position
	      index = index + stringSize;
	      // Parse the element
	      _index = index;
	      // Decode the size of the object document
	      objectSize = buffer[index] | buffer[index + 1] << 8 | buffer[index + 2] << 16 | buffer[index + 3] << 24;
	      // Decode the scope object
	      var scopeObject = deserializeObject(buffer, _index, options, false);
	      // Adjust the index
	      index = index + objectSize;

	      // Check if field length is to short
	      if (totalSize < 4 + 4 + objectSize + stringSize) {
	        throw new Error('code_w_scope total size is to short, truncating scope');
	      }

	      // Check if totalSize field is to long
	      if (totalSize > 4 + 4 + objectSize + stringSize) {
	        throw new Error('code_w_scope total size is to long, clips outer document');
	      }

	      // If we are evaluating the functions
	      if (evalFunctions) {
	        // If we have cache enabled let's look for the md5 of the function in the cache
	        if (cacheFunctions) {
	          hash = cacheFunctionsCrc32 ? crc32(functionString) : functionString;
	          // Got to do this to avoid V8 deoptimizing the call due to finding eval
	          object[name] = isolateEvalWithHash(functionCache, hash, functionString, object);
	        } else {
	          object[name] = isolateEval(functionString);
	        }

	        object[name].scope = scopeObject;
	      } else {
	        object[name] = new Code(functionString, scopeObject);
	      }
	    } else if (elementType === BSON.BSON_DATA_DBPOINTER) {
	      // Get the code string size
	      stringSize = buffer[index++] | buffer[index++] << 8 | buffer[index++] << 16 | buffer[index++] << 24;
	      // Check if we have a valid string
	      if (stringSize <= 0 || stringSize > buffer.length - index || buffer[index + stringSize - 1] !== 0) throw new Error('bad string length in bson');
	      // Namespace
	      var namespace = buffer.toString('utf8', index, index + stringSize - 1);
	      // Update parse index position
	      index = index + stringSize;

	      // Read the oid
	      var oidBuffer = utils.allocBuffer(12);
	      buffer.copy(oidBuffer, 0, index, index + 12);
	      oid = new ObjectID(oidBuffer);

	      // Update the index
	      index = index + 12;

	      // Split the namespace
	      var parts = namespace.split('.');
	      var db = parts.shift();
	      var collection = parts.join('.');
	      // Upgrade to DBRef type
	      object[name] = new DBRef(collection, oid, db);
	    } else {
	      throw new Error('Detected unknown BSON type ' + elementType.toString(16) + ' for fieldname "' + name + '", are you using the latest BSON parser');
	    }
	  }

	  // Check if the deserialization was against a valid array/object
	  if (size !== index - startIndex) {
	    if (isArray) throw new Error('corrupt array bson');
	    throw new Error('corrupt object bson');
	  }

	  // Check if we have a db ref object
	  if (object['$id'] != null) object = new DBRef(object['$ref'], object['$id'], object['$db']);
	  return object;
	};

	/**
	 * Ensure eval is isolated.
	 *
	 * @ignore
	 * @api private
	 */
	var isolateEvalWithHash = function (functionCache, hash, functionString, object) {
	  // Contains the value we are going to set
	  var value = null;

	  // Check for cache hit, eval if missing and return cached function
	  if (functionCache[hash] == null) {
	    eval('value = ' + functionString);
	    functionCache[hash] = value;
	  }
	  // Set the object
	  return functionCache[hash].bind(object);
	};

	/**
	 * Ensure eval is isolated.
	 *
	 * @ignore
	 * @api private
	 */
	var isolateEval = function (functionString) {
	  // Contains the value we are going to set
	  var value = null;
	  // Eval the function
	  eval('value = ' + functionString);
	  return value;
	};

	var BSON = {};

	/**
	 * Contains the function cache if we have that enable to allow for avoiding the eval step on each deserialization, comparison is by md5
	 *
	 * @ignore
	 * @api private
	 */
	var functionCache = BSON.functionCache = {};

	/**
	 * Number BSON Type
	 *
	 * @classconstant BSON_DATA_NUMBER
	 **/
	BSON.BSON_DATA_NUMBER = 1;
	/**
	 * String BSON Type
	 *
	 * @classconstant BSON_DATA_STRING
	 **/
	BSON.BSON_DATA_STRING = 2;
	/**
	 * Object BSON Type
	 *
	 * @classconstant BSON_DATA_OBJECT
	 **/
	BSON.BSON_DATA_OBJECT = 3;
	/**
	 * Array BSON Type
	 *
	 * @classconstant BSON_DATA_ARRAY
	 **/
	BSON.BSON_DATA_ARRAY = 4;
	/**
	 * Binary BSON Type
	 *
	 * @classconstant BSON_DATA_BINARY
	 **/
	BSON.BSON_DATA_BINARY = 5;
	/**
	 * Binary BSON Type
	 *
	 * @classconstant BSON_DATA_UNDEFINED
	 **/
	BSON.BSON_DATA_UNDEFINED = 6;
	/**
	 * ObjectID BSON Type
	 *
	 * @classconstant BSON_DATA_OID
	 **/
	BSON.BSON_DATA_OID = 7;
	/**
	 * Boolean BSON Type
	 *
	 * @classconstant BSON_DATA_BOOLEAN
	 **/
	BSON.BSON_DATA_BOOLEAN = 8;
	/**
	 * Date BSON Type
	 *
	 * @classconstant BSON_DATA_DATE
	 **/
	BSON.BSON_DATA_DATE = 9;
	/**
	 * null BSON Type
	 *
	 * @classconstant BSON_DATA_NULL
	 **/
	BSON.BSON_DATA_NULL = 10;
	/**
	 * RegExp BSON Type
	 *
	 * @classconstant BSON_DATA_REGEXP
	 **/
	BSON.BSON_DATA_REGEXP = 11;
	/**
	 * Code BSON Type
	 *
	 * @classconstant BSON_DATA_DBPOINTER
	 **/
	BSON.BSON_DATA_DBPOINTER = 12;
	/**
	 * Code BSON Type
	 *
	 * @classconstant BSON_DATA_CODE
	 **/
	BSON.BSON_DATA_CODE = 13;
	/**
	 * Symbol BSON Type
	 *
	 * @classconstant BSON_DATA_SYMBOL
	 **/
	BSON.BSON_DATA_SYMBOL = 14;
	/**
	 * Code with Scope BSON Type
	 *
	 * @classconstant BSON_DATA_CODE_W_SCOPE
	 **/
	BSON.BSON_DATA_CODE_W_SCOPE = 15;
	/**
	 * 32 bit Integer BSON Type
	 *
	 * @classconstant BSON_DATA_INT
	 **/
	BSON.BSON_DATA_INT = 16;
	/**
	 * Timestamp BSON Type
	 *
	 * @classconstant BSON_DATA_TIMESTAMP
	 **/
	BSON.BSON_DATA_TIMESTAMP = 17;
	/**
	 * Long BSON Type
	 *
	 * @classconstant BSON_DATA_LONG
	 **/
	BSON.BSON_DATA_LONG = 18;
	/**
	 * Long BSON Type
	 *
	 * @classconstant BSON_DATA_DECIMAL128
	 **/
	BSON.BSON_DATA_DECIMAL128 = 19;
	/**
	 * MinKey BSON Type
	 *
	 * @classconstant BSON_DATA_MIN_KEY
	 **/
	BSON.BSON_DATA_MIN_KEY = 0xff;
	/**
	 * MaxKey BSON Type
	 *
	 * @classconstant BSON_DATA_MAX_KEY
	 **/
	BSON.BSON_DATA_MAX_KEY = 0x7f;

	/**
	 * Binary Default Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_DEFAULT
	 **/
	BSON.BSON_BINARY_SUBTYPE_DEFAULT = 0;
	/**
	 * Binary Function Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_FUNCTION
	 **/
	BSON.BSON_BINARY_SUBTYPE_FUNCTION = 1;
	/**
	 * Binary Byte Array Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_BYTE_ARRAY
	 **/
	BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
	/**
	 * Binary UUID Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_UUID
	 **/
	BSON.BSON_BINARY_SUBTYPE_UUID = 3;
	/**
	 * Binary MD5 Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_MD5
	 **/
	BSON.BSON_BINARY_SUBTYPE_MD5 = 4;
	/**
	 * Binary User Defined Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_USER_DEFINED
	 **/
	BSON.BSON_BINARY_SUBTYPE_USER_DEFINED = 128;

	// BSON MAX VALUES
	BSON.BSON_INT32_MAX = 0x7fffffff;
	BSON.BSON_INT32_MIN = -0x80000000;

	BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
	BSON.BSON_INT64_MIN = -Math.pow(2, 63);

	// JS MAX PRECISE VALUES
	BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
	BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

	// Internal long versions
	var JS_INT_MAX_LONG = Long.fromNumber(0x20000000000000); // Any integer up to 2^53 can be precisely represented by a double.
	var JS_INT_MIN_LONG = Long.fromNumber(-0x20000000000000); // Any integer down to -2^53 can be precisely represented by a double.

	module.exports = deserialize;

/***/ }),
/* 358 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {'use strict';

	var writeIEEE754 = __webpack_require__(359).writeIEEE754,
	    Long = __webpack_require__(335).Long,
	    Map = __webpack_require__(334),
	    Binary = __webpack_require__(356).Binary;

	var normalizedFunctionString = __webpack_require__(344).normalizedFunctionString;

	// try {
	//   var _Buffer = Uint8Array;
	// } catch (e) {
	//   _Buffer = Buffer;
	// }

	var regexp = /\x00/; // eslint-disable-line no-control-regex
	var ignoreKeys = ['$db', '$ref', '$id', '$clusterTime'];

	// To ensure that 0.4 of node works correctly
	var isDate = function isDate(d) {
	  return typeof d === 'object' && Object.prototype.toString.call(d) === '[object Date]';
	};

	var isRegExp = function isRegExp(d) {
	  return Object.prototype.toString.call(d) === '[object RegExp]';
	};

	var serializeString = function (buffer, key, value, index, isArray) {
	  // Encode String type
	  buffer[index++] = BSON.BSON_DATA_STRING;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes + 1;
	  buffer[index - 1] = 0;
	  // Write the string
	  var size = buffer.write(value, index + 4, 'utf8');
	  // Write the size of the string to buffer
	  buffer[index + 3] = size + 1 >> 24 & 0xff;
	  buffer[index + 2] = size + 1 >> 16 & 0xff;
	  buffer[index + 1] = size + 1 >> 8 & 0xff;
	  buffer[index] = size + 1 & 0xff;
	  // Update index
	  index = index + 4 + size;
	  // Write zero
	  buffer[index++] = 0;
	  return index;
	};

	var serializeNumber = function (buffer, key, value, index, isArray) {
	  // We have an integer value
	  if (Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
	    // If the value fits in 32 bits encode as int, if it fits in a double
	    // encode it as a double, otherwise long
	    if (value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) {
	      // Set int type 32 bits or less
	      buffer[index++] = BSON.BSON_DATA_INT;
	      // Number of written bytes
	      var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	      // Encode the name
	      index = index + numberOfWrittenBytes;
	      buffer[index++] = 0;
	      // Write the int value
	      buffer[index++] = value & 0xff;
	      buffer[index++] = value >> 8 & 0xff;
	      buffer[index++] = value >> 16 & 0xff;
	      buffer[index++] = value >> 24 & 0xff;
	    } else if (value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
	      // Encode as double
	      buffer[index++] = BSON.BSON_DATA_NUMBER;
	      // Number of written bytes
	      numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	      // Encode the name
	      index = index + numberOfWrittenBytes;
	      buffer[index++] = 0;
	      // Write float
	      writeIEEE754(buffer, value, index, 'little', 52, 8);
	      // Ajust index
	      index = index + 8;
	    } else {
	      // Set long type
	      buffer[index++] = BSON.BSON_DATA_LONG;
	      // Number of written bytes
	      numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	      // Encode the name
	      index = index + numberOfWrittenBytes;
	      buffer[index++] = 0;
	      var longVal = Long.fromNumber(value);
	      var lowBits = longVal.getLowBits();
	      var highBits = longVal.getHighBits();
	      // Encode low bits
	      buffer[index++] = lowBits & 0xff;
	      buffer[index++] = lowBits >> 8 & 0xff;
	      buffer[index++] = lowBits >> 16 & 0xff;
	      buffer[index++] = lowBits >> 24 & 0xff;
	      // Encode high bits
	      buffer[index++] = highBits & 0xff;
	      buffer[index++] = highBits >> 8 & 0xff;
	      buffer[index++] = highBits >> 16 & 0xff;
	      buffer[index++] = highBits >> 24 & 0xff;
	    }
	  } else {
	    // Encode as double
	    buffer[index++] = BSON.BSON_DATA_NUMBER;
	    // Number of written bytes
	    numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	    // Encode the name
	    index = index + numberOfWrittenBytes;
	    buffer[index++] = 0;
	    // Write float
	    writeIEEE754(buffer, value, index, 'little', 52, 8);
	    // Ajust index
	    index = index + 8;
	  }

	  return index;
	};

	var serializeNull = function (buffer, key, value, index, isArray) {
	  // Set long type
	  buffer[index++] = BSON.BSON_DATA_NULL;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  return index;
	};

	var serializeBoolean = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_BOOLEAN;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Encode the boolean value
	  buffer[index++] = value ? 1 : 0;
	  return index;
	};

	var serializeDate = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_DATE;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;

	  // Write the date
	  var dateInMilis = Long.fromNumber(value.getTime());
	  var lowBits = dateInMilis.getLowBits();
	  var highBits = dateInMilis.getHighBits();
	  // Encode low bits
	  buffer[index++] = lowBits & 0xff;
	  buffer[index++] = lowBits >> 8 & 0xff;
	  buffer[index++] = lowBits >> 16 & 0xff;
	  buffer[index++] = lowBits >> 24 & 0xff;
	  // Encode high bits
	  buffer[index++] = highBits & 0xff;
	  buffer[index++] = highBits >> 8 & 0xff;
	  buffer[index++] = highBits >> 16 & 0xff;
	  buffer[index++] = highBits >> 24 & 0xff;
	  return index;
	};

	var serializeRegExp = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_REGEXP;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  if (value.source && value.source.match(regexp) != null) {
	    throw Error('value ' + value.source + ' must not contain null bytes');
	  }
	  // Adjust the index
	  index = index + buffer.write(value.source, index, 'utf8');
	  // Write zero
	  buffer[index++] = 0x00;
	  // Write the parameters
	  if (value.global) buffer[index++] = 0x73; // s
	  if (value.ignoreCase) buffer[index++] = 0x69; // i
	  if (value.multiline) buffer[index++] = 0x6d; // m
	  // Add ending zero
	  buffer[index++] = 0x00;
	  return index;
	};

	var serializeBSONRegExp = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_REGEXP;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;

	  // Check the pattern for 0 bytes
	  if (value.pattern.match(regexp) != null) {
	    // The BSON spec doesn't allow keys with null bytes because keys are
	    // null-terminated.
	    throw Error('pattern ' + value.pattern + ' must not contain null bytes');
	  }

	  // Adjust the index
	  index = index + buffer.write(value.pattern, index, 'utf8');
	  // Write zero
	  buffer[index++] = 0x00;
	  // Write the options
	  index = index + buffer.write(value.options.split('').sort().join(''), index, 'utf8');
	  // Add ending zero
	  buffer[index++] = 0x00;
	  return index;
	};

	var serializeMinMax = function (buffer, key, value, index, isArray) {
	  // Write the type of either min or max key
	  if (value === null) {
	    buffer[index++] = BSON.BSON_DATA_NULL;
	  } else if (value._bsontype === 'MinKey') {
	    buffer[index++] = BSON.BSON_DATA_MIN_KEY;
	  } else {
	    buffer[index++] = BSON.BSON_DATA_MAX_KEY;
	  }

	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  return index;
	};

	var serializeObjectId = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_OID;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');

	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;

	  // Write the objectId into the shared buffer
	  if (typeof value.id === 'string') {
	    buffer.write(value.id, index, 'binary');
	  } else if (value.id && value.id.copy) {
	    value.id.copy(buffer, index, 0, 12);
	  } else {
	    throw new Error('object [' + JSON.stringify(value) + '] is not a valid ObjectId');
	  }

	  // Ajust index
	  return index + 12;
	};

	var serializeBuffer = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_BINARY;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Get size of the buffer (current write point)
	  var size = value.length;
	  // Write the size of the string to buffer
	  buffer[index++] = size & 0xff;
	  buffer[index++] = size >> 8 & 0xff;
	  buffer[index++] = size >> 16 & 0xff;
	  buffer[index++] = size >> 24 & 0xff;
	  // Write the default subtype
	  buffer[index++] = BSON.BSON_BINARY_SUBTYPE_DEFAULT;
	  // Copy the content form the binary field to the buffer
	  value.copy(buffer, index, 0, size);
	  // Adjust the index
	  index = index + size;
	  return index;
	};

	var serializeObject = function (buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, isArray, path) {
	  for (var i = 0; i < path.length; i++) {
	    if (path[i] === value) throw new Error('cyclic dependency detected');
	  }

	  // Push value to stack
	  path.push(value);
	  // Write the type
	  buffer[index++] = Array.isArray(value) ? BSON.BSON_DATA_ARRAY : BSON.BSON_DATA_OBJECT;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  var endIndex = serializeInto(buffer, value, checkKeys, index, depth + 1, serializeFunctions, ignoreUndefined, path);
	  // Pop stack
	  path.pop();
	  // Write size
	  return endIndex;
	};

	var serializeDecimal128 = function (buffer, key, value, index, isArray) {
	  buffer[index++] = BSON.BSON_DATA_DECIMAL128;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Write the data from the value
	  value.bytes.copy(buffer, index, 0, 16);
	  return index + 16;
	};

	var serializeLong = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = value._bsontype === 'Long' ? BSON.BSON_DATA_LONG : BSON.BSON_DATA_TIMESTAMP;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Write the date
	  var lowBits = value.getLowBits();
	  var highBits = value.getHighBits();
	  // Encode low bits
	  buffer[index++] = lowBits & 0xff;
	  buffer[index++] = lowBits >> 8 & 0xff;
	  buffer[index++] = lowBits >> 16 & 0xff;
	  buffer[index++] = lowBits >> 24 & 0xff;
	  // Encode high bits
	  buffer[index++] = highBits & 0xff;
	  buffer[index++] = highBits >> 8 & 0xff;
	  buffer[index++] = highBits >> 16 & 0xff;
	  buffer[index++] = highBits >> 24 & 0xff;
	  return index;
	};

	var serializeInt32 = function (buffer, key, value, index, isArray) {
	  // Set int type 32 bits or less
	  buffer[index++] = BSON.BSON_DATA_INT;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Write the int value
	  buffer[index++] = value & 0xff;
	  buffer[index++] = value >> 8 & 0xff;
	  buffer[index++] = value >> 16 & 0xff;
	  buffer[index++] = value >> 24 & 0xff;
	  return index;
	};

	var serializeDouble = function (buffer, key, value, index, isArray) {
	  // Encode as double
	  buffer[index++] = BSON.BSON_DATA_NUMBER;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Write float
	  writeIEEE754(buffer, value, index, 'little', 52, 8);
	  // Ajust index
	  index = index + 8;
	  return index;
	};

	var serializeFunction = function (buffer, key, value, index, checkKeys, depth, isArray) {
	  buffer[index++] = BSON.BSON_DATA_CODE;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Function string
	  var functionString = normalizedFunctionString(value);

	  // Write the string
	  var size = buffer.write(functionString, index + 4, 'utf8') + 1;
	  // Write the size of the string to buffer
	  buffer[index] = size & 0xff;
	  buffer[index + 1] = size >> 8 & 0xff;
	  buffer[index + 2] = size >> 16 & 0xff;
	  buffer[index + 3] = size >> 24 & 0xff;
	  // Update index
	  index = index + 4 + size - 1;
	  // Write zero
	  buffer[index++] = 0;
	  return index;
	};

	var serializeCode = function (buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, isArray) {
	  if (value.scope && typeof value.scope === 'object') {
	    // Write the type
	    buffer[index++] = BSON.BSON_DATA_CODE_W_SCOPE;
	    // Number of written bytes
	    var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	    // Encode the name
	    index = index + numberOfWrittenBytes;
	    buffer[index++] = 0;

	    // Starting index
	    var startIndex = index;

	    // Serialize the function
	    // Get the function string
	    var functionString = typeof value.code === 'string' ? value.code : value.code.toString();
	    // Index adjustment
	    index = index + 4;
	    // Write string into buffer
	    var codeSize = buffer.write(functionString, index + 4, 'utf8') + 1;
	    // Write the size of the string to buffer
	    buffer[index] = codeSize & 0xff;
	    buffer[index + 1] = codeSize >> 8 & 0xff;
	    buffer[index + 2] = codeSize >> 16 & 0xff;
	    buffer[index + 3] = codeSize >> 24 & 0xff;
	    // Write end 0
	    buffer[index + 4 + codeSize - 1] = 0;
	    // Write the
	    index = index + codeSize + 4;

	    //
	    // Serialize the scope value
	    var endIndex = serializeInto(buffer, value.scope, checkKeys, index, depth + 1, serializeFunctions, ignoreUndefined);
	    index = endIndex - 1;

	    // Writ the total
	    var totalSize = endIndex - startIndex;

	    // Write the total size of the object
	    buffer[startIndex++] = totalSize & 0xff;
	    buffer[startIndex++] = totalSize >> 8 & 0xff;
	    buffer[startIndex++] = totalSize >> 16 & 0xff;
	    buffer[startIndex++] = totalSize >> 24 & 0xff;
	    // Write trailing zero
	    buffer[index++] = 0;
	  } else {
	    buffer[index++] = BSON.BSON_DATA_CODE;
	    // Number of written bytes
	    numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	    // Encode the name
	    index = index + numberOfWrittenBytes;
	    buffer[index++] = 0;
	    // Function string
	    functionString = value.code.toString();
	    // Write the string
	    var size = buffer.write(functionString, index + 4, 'utf8') + 1;
	    // Write the size of the string to buffer
	    buffer[index] = size & 0xff;
	    buffer[index + 1] = size >> 8 & 0xff;
	    buffer[index + 2] = size >> 16 & 0xff;
	    buffer[index + 3] = size >> 24 & 0xff;
	    // Update index
	    index = index + 4 + size - 1;
	    // Write zero
	    buffer[index++] = 0;
	  }

	  return index;
	};

	var serializeBinary = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_BINARY;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Extract the buffer
	  var data = value.value(true);
	  // Calculate size
	  var size = value.position;
	  // Add the deprecated 02 type 4 bytes of size to total
	  if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) size = size + 4;
	  // Write the size of the string to buffer
	  buffer[index++] = size & 0xff;
	  buffer[index++] = size >> 8 & 0xff;
	  buffer[index++] = size >> 16 & 0xff;
	  buffer[index++] = size >> 24 & 0xff;
	  // Write the subtype to the buffer
	  buffer[index++] = value.sub_type;

	  // If we have binary type 2 the 4 first bytes are the size
	  if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) {
	    size = size - 4;
	    buffer[index++] = size & 0xff;
	    buffer[index++] = size >> 8 & 0xff;
	    buffer[index++] = size >> 16 & 0xff;
	    buffer[index++] = size >> 24 & 0xff;
	  }

	  // Write the data to the object
	  data.copy(buffer, index, 0, value.position);
	  // Adjust the index
	  index = index + value.position;
	  return index;
	};

	var serializeSymbol = function (buffer, key, value, index, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_SYMBOL;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');
	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;
	  // Write the string
	  var size = buffer.write(value.value, index + 4, 'utf8') + 1;
	  // Write the size of the string to buffer
	  buffer[index] = size & 0xff;
	  buffer[index + 1] = size >> 8 & 0xff;
	  buffer[index + 2] = size >> 16 & 0xff;
	  buffer[index + 3] = size >> 24 & 0xff;
	  // Update index
	  index = index + 4 + size - 1;
	  // Write zero
	  buffer[index++] = 0x00;
	  return index;
	};

	var serializeDBRef = function (buffer, key, value, index, depth, serializeFunctions, isArray) {
	  // Write the type
	  buffer[index++] = BSON.BSON_DATA_OBJECT;
	  // Number of written bytes
	  var numberOfWrittenBytes = !isArray ? buffer.write(key, index, 'utf8') : buffer.write(key, index, 'ascii');

	  // Encode the name
	  index = index + numberOfWrittenBytes;
	  buffer[index++] = 0;

	  var startIndex = index;
	  var endIndex;

	  // Serialize object
	  if (null != value.db) {
	    endIndex = serializeInto(buffer, {
	      $ref: value.namespace,
	      $id: value.oid,
	      $db: value.db
	    }, false, index, depth + 1, serializeFunctions);
	  } else {
	    endIndex = serializeInto(buffer, {
	      $ref: value.namespace,
	      $id: value.oid
	    }, false, index, depth + 1, serializeFunctions);
	  }

	  // Calculate object size
	  var size = endIndex - startIndex;
	  // Write the size
	  buffer[startIndex++] = size & 0xff;
	  buffer[startIndex++] = size >> 8 & 0xff;
	  buffer[startIndex++] = size >> 16 & 0xff;
	  buffer[startIndex++] = size >> 24 & 0xff;
	  // Set index
	  return endIndex;
	};

	var serializeInto = function serializeInto(buffer, object, checkKeys, startingIndex, depth, serializeFunctions, ignoreUndefined, path) {
	  startingIndex = startingIndex || 0;
	  path = path || [];

	  // Push the object to the path
	  path.push(object);

	  // Start place to serialize into
	  var index = startingIndex + 4;
	  // var self = this;

	  // Special case isArray
	  if (Array.isArray(object)) {
	    // Get object keys
	    for (var i = 0; i < object.length; i++) {
	      var key = '' + i;
	      var value = object[i];

	      // Is there an override value
	      if (value && value.toBSON) {
	        if (typeof value.toBSON !== 'function') throw new Error('toBSON is not a function');
	        value = value.toBSON();
	      }

	      var type = typeof value;
	      if (type === 'string') {
	        index = serializeString(buffer, key, value, index, true);
	      } else if (type === 'number') {
	        index = serializeNumber(buffer, key, value, index, true);
	      } else if (type === 'boolean') {
	        index = serializeBoolean(buffer, key, value, index, true);
	      } else if (value instanceof Date || isDate(value)) {
	        index = serializeDate(buffer, key, value, index, true);
	      } else if (value === undefined) {
	        index = serializeNull(buffer, key, value, index, true);
	      } else if (value === null) {
	        index = serializeNull(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
	        index = serializeObjectId(buffer, key, value, index, true);
	      } else if (Buffer.isBuffer(value)) {
	        index = serializeBuffer(buffer, key, value, index, true);
	      } else if (value instanceof RegExp || isRegExp(value)) {
	        index = serializeRegExp(buffer, key, value, index, true);
	      } else if (type === 'object' && value['_bsontype'] == null) {
	        index = serializeObject(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, true, path);
	      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
	        index = serializeDecimal128(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
	        index = serializeLong(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'Double') {
	        index = serializeDouble(buffer, key, value, index, true);
	      } else if (typeof value === 'function' && serializeFunctions) {
	        index = serializeFunction(buffer, key, value, index, checkKeys, depth, serializeFunctions, true);
	      } else if (value['_bsontype'] === 'Code') {
	        index = serializeCode(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, true);
	      } else if (value['_bsontype'] === 'Binary') {
	        index = serializeBinary(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'Symbol') {
	        index = serializeSymbol(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'DBRef') {
	        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions, true);
	      } else if (value['_bsontype'] === 'BSONRegExp') {
	        index = serializeBSONRegExp(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'Int32') {
	        index = serializeInt32(buffer, key, value, index, true);
	      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
	        index = serializeMinMax(buffer, key, value, index, true);
	      } else if (typeof value['_bsontype'] !== 'undefined') {
	        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
	      }
	    }
	  } else if (object instanceof Map) {
	    var iterator = object.entries();
	    var done = false;

	    while (!done) {
	      // Unpack the next entry
	      var entry = iterator.next();
	      done = entry.done;
	      // Are we done, then skip and terminate
	      if (done) continue;

	      // Get the entry values
	      key = entry.value[0];
	      value = entry.value[1];

	      // Check the type of the value
	      type = typeof value;

	      // Check the key and throw error if it's illegal
	      if (typeof key === 'string' && ignoreKeys.indexOf(key) === -1) {
	        if (key.match(regexp) != null) {
	          // The BSON spec doesn't allow keys with null bytes because keys are
	          // null-terminated.
	          throw Error('key ' + key + ' must not contain null bytes');
	        }

	        if (checkKeys) {
	          if ('$' === key[0]) {
	            throw Error('key ' + key + " must not start with '$'");
	          } else if (~key.indexOf('.')) {
	            throw Error('key ' + key + " must not contain '.'");
	          }
	        }
	      }

	      if (type === 'string') {
	        index = serializeString(buffer, key, value, index);
	      } else if (type === 'number') {
	        index = serializeNumber(buffer, key, value, index);
	      } else if (type === 'boolean') {
	        index = serializeBoolean(buffer, key, value, index);
	      } else if (value instanceof Date || isDate(value)) {
	        index = serializeDate(buffer, key, value, index);
	        // } else if (value === undefined && ignoreUndefined === true) {
	      } else if (value === null || value === undefined && ignoreUndefined === false) {
	        index = serializeNull(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
	        index = serializeObjectId(buffer, key, value, index);
	      } else if (Buffer.isBuffer(value)) {
	        index = serializeBuffer(buffer, key, value, index);
	      } else if (value instanceof RegExp || isRegExp(value)) {
	        index = serializeRegExp(buffer, key, value, index);
	      } else if (type === 'object' && value['_bsontype'] == null) {
	        index = serializeObject(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, false, path);
	      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
	        index = serializeDecimal128(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
	        index = serializeLong(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Double') {
	        index = serializeDouble(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Code') {
	        index = serializeCode(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined);
	      } else if (typeof value === 'function' && serializeFunctions) {
	        index = serializeFunction(buffer, key, value, index, checkKeys, depth, serializeFunctions);
	      } else if (value['_bsontype'] === 'Binary') {
	        index = serializeBinary(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Symbol') {
	        index = serializeSymbol(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'DBRef') {
	        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions);
	      } else if (value['_bsontype'] === 'BSONRegExp') {
	        index = serializeBSONRegExp(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Int32') {
	        index = serializeInt32(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
	        index = serializeMinMax(buffer, key, value, index);
	      } else if (typeof value['_bsontype'] !== 'undefined') {
	        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
	      }
	    }
	  } else {
	    // Did we provide a custom serialization method
	    if (object.toBSON) {
	      if (typeof object.toBSON !== 'function') throw new Error('toBSON is not a function');
	      object = object.toBSON();
	      if (object != null && typeof object !== 'object') throw new Error('toBSON function did not return an object');
	    }

	    // Iterate over all the keys
	    for (key in object) {
	      value = object[key];
	      // Is there an override value
	      if (value && value.toBSON) {
	        if (typeof value.toBSON !== 'function') throw new Error('toBSON is not a function');
	        value = value.toBSON();
	      }

	      // Check the type of the value
	      type = typeof value;

	      // Check the key and throw error if it's illegal
	      if (typeof key === 'string' && ignoreKeys.indexOf(key) === -1) {
	        if (key.match(regexp) != null) {
	          // The BSON spec doesn't allow keys with null bytes because keys are
	          // null-terminated.
	          throw Error('key ' + key + ' must not contain null bytes');
	        }

	        if (checkKeys) {
	          if ('$' === key[0]) {
	            throw Error('key ' + key + " must not start with '$'");
	          } else if (~key.indexOf('.')) {
	            throw Error('key ' + key + " must not contain '.'");
	          }
	        }
	      }

	      if (type === 'string') {
	        index = serializeString(buffer, key, value, index);
	      } else if (type === 'number') {
	        index = serializeNumber(buffer, key, value, index);
	      } else if (type === 'boolean') {
	        index = serializeBoolean(buffer, key, value, index);
	      } else if (value instanceof Date || isDate(value)) {
	        index = serializeDate(buffer, key, value, index);
	      } else if (value === undefined) {
	        if (ignoreUndefined === false) index = serializeNull(buffer, key, value, index);
	      } else if (value === null) {
	        index = serializeNull(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
	        index = serializeObjectId(buffer, key, value, index);
	      } else if (Buffer.isBuffer(value)) {
	        index = serializeBuffer(buffer, key, value, index);
	      } else if (value instanceof RegExp || isRegExp(value)) {
	        index = serializeRegExp(buffer, key, value, index);
	      } else if (type === 'object' && value['_bsontype'] == null) {
	        index = serializeObject(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined, false, path);
	      } else if (type === 'object' && value['_bsontype'] === 'Decimal128') {
	        index = serializeDecimal128(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Long' || value['_bsontype'] === 'Timestamp') {
	        index = serializeLong(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Double') {
	        index = serializeDouble(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Code') {
	        index = serializeCode(buffer, key, value, index, checkKeys, depth, serializeFunctions, ignoreUndefined);
	      } else if (typeof value === 'function' && serializeFunctions) {
	        index = serializeFunction(buffer, key, value, index, checkKeys, depth, serializeFunctions);
	      } else if (value['_bsontype'] === 'Binary') {
	        index = serializeBinary(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Symbol') {
	        index = serializeSymbol(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'DBRef') {
	        index = serializeDBRef(buffer, key, value, index, depth, serializeFunctions);
	      } else if (value['_bsontype'] === 'BSONRegExp') {
	        index = serializeBSONRegExp(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'Int32') {
	        index = serializeInt32(buffer, key, value, index);
	      } else if (value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
	        index = serializeMinMax(buffer, key, value, index);
	      } else if (typeof value['_bsontype'] !== 'undefined') {
	        throw new TypeError('Unrecognized or invalid _bsontype: ' + value['_bsontype']);
	      }
	    }
	  }

	  // Remove the path
	  path.pop();

	  // Final padding byte for object
	  buffer[index++] = 0x00;

	  // Final size
	  var size = index - startingIndex;
	  // Write the size of the object
	  buffer[startingIndex++] = size & 0xff;
	  buffer[startingIndex++] = size >> 8 & 0xff;
	  buffer[startingIndex++] = size >> 16 & 0xff;
	  buffer[startingIndex++] = size >> 24 & 0xff;
	  return index;
	};

	var BSON = {};

	/**
	 * Contains the function cache if we have that enable to allow for avoiding the eval step on each deserialization, comparison is by md5
	 *
	 * @ignore
	 * @api private
	 */
	// var functionCache = (BSON.functionCache = {});

	/**
	 * Number BSON Type
	 *
	 * @classconstant BSON_DATA_NUMBER
	 **/
	BSON.BSON_DATA_NUMBER = 1;
	/**
	 * String BSON Type
	 *
	 * @classconstant BSON_DATA_STRING
	 **/
	BSON.BSON_DATA_STRING = 2;
	/**
	 * Object BSON Type
	 *
	 * @classconstant BSON_DATA_OBJECT
	 **/
	BSON.BSON_DATA_OBJECT = 3;
	/**
	 * Array BSON Type
	 *
	 * @classconstant BSON_DATA_ARRAY
	 **/
	BSON.BSON_DATA_ARRAY = 4;
	/**
	 * Binary BSON Type
	 *
	 * @classconstant BSON_DATA_BINARY
	 **/
	BSON.BSON_DATA_BINARY = 5;
	/**
	 * ObjectID BSON Type, deprecated
	 *
	 * @classconstant BSON_DATA_UNDEFINED
	 **/
	BSON.BSON_DATA_UNDEFINED = 6;
	/**
	 * ObjectID BSON Type
	 *
	 * @classconstant BSON_DATA_OID
	 **/
	BSON.BSON_DATA_OID = 7;
	/**
	 * Boolean BSON Type
	 *
	 * @classconstant BSON_DATA_BOOLEAN
	 **/
	BSON.BSON_DATA_BOOLEAN = 8;
	/**
	 * Date BSON Type
	 *
	 * @classconstant BSON_DATA_DATE
	 **/
	BSON.BSON_DATA_DATE = 9;
	/**
	 * null BSON Type
	 *
	 * @classconstant BSON_DATA_NULL
	 **/
	BSON.BSON_DATA_NULL = 10;
	/**
	 * RegExp BSON Type
	 *
	 * @classconstant BSON_DATA_REGEXP
	 **/
	BSON.BSON_DATA_REGEXP = 11;
	/**
	 * Code BSON Type
	 *
	 * @classconstant BSON_DATA_CODE
	 **/
	BSON.BSON_DATA_CODE = 13;
	/**
	 * Symbol BSON Type
	 *
	 * @classconstant BSON_DATA_SYMBOL
	 **/
	BSON.BSON_DATA_SYMBOL = 14;
	/**
	 * Code with Scope BSON Type
	 *
	 * @classconstant BSON_DATA_CODE_W_SCOPE
	 **/
	BSON.BSON_DATA_CODE_W_SCOPE = 15;
	/**
	 * 32 bit Integer BSON Type
	 *
	 * @classconstant BSON_DATA_INT
	 **/
	BSON.BSON_DATA_INT = 16;
	/**
	 * Timestamp BSON Type
	 *
	 * @classconstant BSON_DATA_TIMESTAMP
	 **/
	BSON.BSON_DATA_TIMESTAMP = 17;
	/**
	 * Long BSON Type
	 *
	 * @classconstant BSON_DATA_LONG
	 **/
	BSON.BSON_DATA_LONG = 18;
	/**
	 * Long BSON Type
	 *
	 * @classconstant BSON_DATA_DECIMAL128
	 **/
	BSON.BSON_DATA_DECIMAL128 = 19;
	/**
	 * MinKey BSON Type
	 *
	 * @classconstant BSON_DATA_MIN_KEY
	 **/
	BSON.BSON_DATA_MIN_KEY = 0xff;
	/**
	 * MaxKey BSON Type
	 *
	 * @classconstant BSON_DATA_MAX_KEY
	 **/
	BSON.BSON_DATA_MAX_KEY = 0x7f;
	/**
	 * Binary Default Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_DEFAULT
	 **/
	BSON.BSON_BINARY_SUBTYPE_DEFAULT = 0;
	/**
	 * Binary Function Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_FUNCTION
	 **/
	BSON.BSON_BINARY_SUBTYPE_FUNCTION = 1;
	/**
	 * Binary Byte Array Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_BYTE_ARRAY
	 **/
	BSON.BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
	/**
	 * Binary UUID Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_UUID
	 **/
	BSON.BSON_BINARY_SUBTYPE_UUID = 3;
	/**
	 * Binary MD5 Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_MD5
	 **/
	BSON.BSON_BINARY_SUBTYPE_MD5 = 4;
	/**
	 * Binary User Defined Type
	 *
	 * @classconstant BSON_BINARY_SUBTYPE_USER_DEFINED
	 **/
	BSON.BSON_BINARY_SUBTYPE_USER_DEFINED = 128;

	// BSON MAX VALUES
	BSON.BSON_INT32_MAX = 0x7fffffff;
	BSON.BSON_INT32_MIN = -0x80000000;

	BSON.BSON_INT64_MAX = Math.pow(2, 63) - 1;
	BSON.BSON_INT64_MIN = -Math.pow(2, 63);

	// JS MAX PRECISE VALUES
	BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
	BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

	// Internal long versions
	// var JS_INT_MAX_LONG = Long.fromNumber(0x20000000000000); // Any integer up to 2^53 can be precisely represented by a double.
	// var JS_INT_MIN_LONG = Long.fromNumber(-0x20000000000000); // Any integer down to -2^53 can be precisely represented by a double.

	module.exports = serializeInto;
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(339).Buffer))

/***/ }),
/* 359 */
/***/ (function(module, exports) {

	// Copyright (c) 2008, Fair Oaks Labs, Inc.
	// All rights reserved.
	//
	// Redistribution and use in source and binary forms, with or without
	// modification, are permitted provided that the following conditions are met:
	//
	//  * Redistributions of source code must retain the above copyright notice,
	//    this list of conditions and the following disclaimer.
	//
	//  * Redistributions in binary form must reproduce the above copyright notice,
	//    this list of conditions and the following disclaimer in the documentation
	//    and/or other materials provided with the distribution.
	//
	//  * Neither the name of Fair Oaks Labs, Inc. nor the names of its contributors
	//    may be used to endorse or promote products derived from this software
	//    without specific prior written permission.
	//
	// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
	// ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
	// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
	// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
	// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
	// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
	// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
	// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
	// POSSIBILITY OF SUCH DAMAGE.
	//
	//
	// Modifications to writeIEEE754 to support negative zeroes made by Brian White

	var readIEEE754 = function (buffer, offset, endian, mLen, nBytes) {
	  var e,
	      m,
	      bBE = endian === 'big',
	      eLen = nBytes * 8 - mLen - 1,
	      eMax = (1 << eLen) - 1,
	      eBias = eMax >> 1,
	      nBits = -7,
	      i = bBE ? 0 : nBytes - 1,
	      d = bBE ? 1 : -1,
	      s = buffer[offset + i];

	  i += d;

	  e = s & (1 << -nBits) - 1;
	  s >>= -nBits;
	  nBits += eLen;
	  for (; nBits > 0; e = e * 256 + buffer[offset + i], i += d, nBits -= 8);

	  m = e & (1 << -nBits) - 1;
	  e >>= -nBits;
	  nBits += mLen;
	  for (; nBits > 0; m = m * 256 + buffer[offset + i], i += d, nBits -= 8);

	  if (e === 0) {
	    e = 1 - eBias;
	  } else if (e === eMax) {
	    return m ? NaN : (s ? -1 : 1) * Infinity;
	  } else {
	    m = m + Math.pow(2, mLen);
	    e = e - eBias;
	  }
	  return (s ? -1 : 1) * m * Math.pow(2, e - mLen);
	};

	var writeIEEE754 = function (buffer, value, offset, endian, mLen, nBytes) {
	  var e,
	      m,
	      c,
	      bBE = endian === 'big',
	      eLen = nBytes * 8 - mLen - 1,
	      eMax = (1 << eLen) - 1,
	      eBias = eMax >> 1,
	      rt = mLen === 23 ? Math.pow(2, -24) - Math.pow(2, -77) : 0,
	      i = bBE ? nBytes - 1 : 0,
	      d = bBE ? -1 : 1,
	      s = value < 0 || value === 0 && 1 / value < 0 ? 1 : 0;

	  value = Math.abs(value);

	  if (isNaN(value) || value === Infinity) {
	    m = isNaN(value) ? 1 : 0;
	    e = eMax;
	  } else {
	    e = Math.floor(Math.log(value) / Math.LN2);
	    if (value * (c = Math.pow(2, -e)) < 1) {
	      e--;
	      c *= 2;
	    }
	    if (e + eBias >= 1) {
	      value += rt / c;
	    } else {
	      value += rt * Math.pow(2, 1 - eBias);
	    }
	    if (value * c >= 2) {
	      e++;
	      c /= 2;
	    }

	    if (e + eBias >= eMax) {
	      m = 0;
	      e = eMax;
	    } else if (e + eBias >= 1) {
	      m = (value * c - 1) * Math.pow(2, mLen);
	      e = e + eBias;
	    } else {
	      m = value * Math.pow(2, eBias - 1) * Math.pow(2, mLen);
	      e = 0;
	    }
	  }

	  for (; mLen >= 8; buffer[offset + i] = m & 0xff, i += d, m /= 256, mLen -= 8);

	  e = e << mLen | m;
	  eLen += mLen;
	  for (; eLen > 0; buffer[offset + i] = e & 0xff, i += d, e /= 256, eLen -= 8);

	  buffer[offset + i - d] |= s * 128;
	};

	exports.readIEEE754 = readIEEE754;
	exports.writeIEEE754 = writeIEEE754;

/***/ }),
/* 360 */
/***/ (function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(Buffer) {'use strict';

	var Long = __webpack_require__(335).Long,
	    Double = __webpack_require__(336).Double,
	    Timestamp = __webpack_require__(337).Timestamp,
	    ObjectID = __webpack_require__(338).ObjectID,
	    Symbol = __webpack_require__(349).Symbol,
	    BSONRegExp = __webpack_require__(348).BSONRegExp,
	    Code = __webpack_require__(351).Code,
	    Decimal128 = __webpack_require__(352),
	    MinKey = __webpack_require__(353).MinKey,
	    MaxKey = __webpack_require__(354).MaxKey,
	    DBRef = __webpack_require__(355).DBRef,
	    Binary = __webpack_require__(356).Binary;

	var normalizedFunctionString = __webpack_require__(344).normalizedFunctionString;

	// To ensure that 0.4 of node works correctly
	var isDate = function isDate(d) {
	  return typeof d === 'object' && Object.prototype.toString.call(d) === '[object Date]';
	};

	var calculateObjectSize = function calculateObjectSize(object, serializeFunctions, ignoreUndefined) {
	  var totalLength = 4 + 1;

	  if (Array.isArray(object)) {
	    for (var i = 0; i < object.length; i++) {
	      totalLength += calculateElement(i.toString(), object[i], serializeFunctions, true, ignoreUndefined);
	    }
	  } else {
	    // If we have toBSON defined, override the current object
	    if (object.toBSON) {
	      object = object.toBSON();
	    }

	    // Calculate size
	    for (var key in object) {
	      totalLength += calculateElement(key, object[key], serializeFunctions, false, ignoreUndefined);
	    }
	  }

	  return totalLength;
	};

	/**
	 * @ignore
	 * @api private
	 */
	function calculateElement(name, value, serializeFunctions, isArray, ignoreUndefined) {
	  // If we have toBSON defined, override the current object
	  if (value && value.toBSON) {
	    value = value.toBSON();
	  }

	  switch (typeof value) {
	    case 'string':
	      return 1 + Buffer.byteLength(name, 'utf8') + 1 + 4 + Buffer.byteLength(value, 'utf8') + 1;
	    case 'number':
	      if (Math.floor(value) === value && value >= BSON.JS_INT_MIN && value <= BSON.JS_INT_MAX) {
	        if (value >= BSON.BSON_INT32_MIN && value <= BSON.BSON_INT32_MAX) {
	          // 32 bit
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (4 + 1);
	        } else {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
	        }
	      } else {
	        // 64 bit
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
	      }
	    case 'undefined':
	      if (isArray || !ignoreUndefined) return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1;
	      return 0;
	    case 'boolean':
	      return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (1 + 1);
	    case 'object':
	      if (value == null || value instanceof MinKey || value instanceof MaxKey || value['_bsontype'] === 'MinKey' || value['_bsontype'] === 'MaxKey') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1;
	      } else if (value instanceof ObjectID || value['_bsontype'] === 'ObjectID' || value['_bsontype'] === 'ObjectId') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (12 + 1);
	      } else if (value instanceof Date || isDate(value)) {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
	      } else if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (1 + 4 + 1) + value.length;
	      } else if (value instanceof Long || value instanceof Double || value instanceof Timestamp || value['_bsontype'] === 'Long' || value['_bsontype'] === 'Double' || value['_bsontype'] === 'Timestamp') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (8 + 1);
	      } else if (value instanceof Decimal128 || value['_bsontype'] === 'Decimal128') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (16 + 1);
	      } else if (value instanceof Code || value['_bsontype'] === 'Code') {
	        // Calculate size depending on the availability of a scope
	        if (value.scope != null && Object.keys(value.scope).length > 0) {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + 4 + 4 + Buffer.byteLength(value.code.toString(), 'utf8') + 1 + calculateObjectSize(value.scope, serializeFunctions, ignoreUndefined);
	        } else {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + 4 + Buffer.byteLength(value.code.toString(), 'utf8') + 1;
	        }
	      } else if (value instanceof Binary || value['_bsontype'] === 'Binary') {
	        // Check what kind of subtype we have
	        if (value.sub_type === Binary.SUBTYPE_BYTE_ARRAY) {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (value.position + 1 + 4 + 1 + 4);
	        } else {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + (value.position + 1 + 4 + 1);
	        }
	      } else if (value instanceof Symbol || value['_bsontype'] === 'Symbol') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + Buffer.byteLength(value.value, 'utf8') + 4 + 1 + 1;
	      } else if (value instanceof DBRef || value['_bsontype'] === 'DBRef') {
	        // Set up correct object for serialization
	        var ordered_values = {
	          $ref: value.namespace,
	          $id: value.oid
	        };

	        // Add db reference if it exists
	        if (null != value.db) {
	          ordered_values['$db'] = value.db;
	        }

	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + calculateObjectSize(ordered_values, serializeFunctions, ignoreUndefined);
	      } else if (value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + Buffer.byteLength(value.source, 'utf8') + 1 + (value.global ? 1 : 0) + (value.ignoreCase ? 1 : 0) + (value.multiline ? 1 : 0) + 1;
	      } else if (value instanceof BSONRegExp || value['_bsontype'] === 'BSONRegExp') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + Buffer.byteLength(value.pattern, 'utf8') + 1 + Buffer.byteLength(value.options, 'utf8') + 1;
	      } else {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + calculateObjectSize(value, serializeFunctions, ignoreUndefined) + 1;
	      }
	    case 'function':
	      // WTF for 0.4.X where typeof /someregexp/ === 'function'
	      if (value instanceof RegExp || Object.prototype.toString.call(value) === '[object RegExp]' || String.call(value) === '[object RegExp]') {
	        return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + Buffer.byteLength(value.source, 'utf8') + 1 + (value.global ? 1 : 0) + (value.ignoreCase ? 1 : 0) + (value.multiline ? 1 : 0) + 1;
	      } else {
	        if (serializeFunctions && value.scope != null && Object.keys(value.scope).length > 0) {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + 4 + 4 + Buffer.byteLength(normalizedFunctionString(value), 'utf8') + 1 + calculateObjectSize(value.scope, serializeFunctions, ignoreUndefined);
	        } else if (serializeFunctions) {
	          return (name != null ? Buffer.byteLength(name, 'utf8') + 1 : 0) + 1 + 4 + Buffer.byteLength(normalizedFunctionString(value), 'utf8') + 1;
	        }
	      }
	  }

	  return 0;
	}

	var BSON = {};

	// BSON MAX VALUES
	BSON.BSON_INT32_MAX = 0x7fffffff;
	BSON.BSON_INT32_MIN = -0x80000000;

	// JS MAX PRECISE VALUES
	BSON.JS_INT_MAX = 0x20000000000000; // Any integer up to 2^53 can be precisely represented by a double.
	BSON.JS_INT_MIN = -0x20000000000000; // Any integer down to -2^53 can be precisely represented by a double.

	module.exports = calculateObjectSize;
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(339).Buffer))

/***/ })
/******/ ])
});
;
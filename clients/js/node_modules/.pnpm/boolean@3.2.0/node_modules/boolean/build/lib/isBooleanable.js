"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isBooleanable = void 0;
const isBooleanable = function (value) {
    switch (Object.prototype.toString.call(value)) {
        case '[object String]':
            return [
                'true', 't', 'yes', 'y', 'on', '1',
                'false', 'f', 'no', 'n', 'off', '0'
            ].includes(value.trim().toLowerCase());
        case '[object Number]':
            return [0, 1].includes(value.valueOf());
        case '[object Boolean]':
            return true;
        default:
            return false;
    }
};
exports.isBooleanable = isBooleanable;

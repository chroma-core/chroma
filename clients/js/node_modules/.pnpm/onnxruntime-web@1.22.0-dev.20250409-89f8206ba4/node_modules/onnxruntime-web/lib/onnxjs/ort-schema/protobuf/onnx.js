/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
'use strict';

var $protobuf = require('protobufjs/minimal');

// Common aliases
var $Reader = $protobuf.Reader,
  $Writer = $protobuf.Writer,
  $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots['default'] || ($protobuf.roots['default'] = {});

$root.onnx = (function () {
  /**
   * Namespace onnx.
   * @exports onnx
   * @namespace
   */
  var onnx = {};

  /**
   * Version enum.
   * @name onnx.Version
   * @enum {number}
   * @property {number} _START_VERSION=0 _START_VERSION value
   * @property {number} IR_VERSION_2017_10_10=1 IR_VERSION_2017_10_10 value
   * @property {number} IR_VERSION_2017_10_30=2 IR_VERSION_2017_10_30 value
   * @property {number} IR_VERSION_2017_11_3=3 IR_VERSION_2017_11_3 value
   * @property {number} IR_VERSION_2019_1_22=4 IR_VERSION_2019_1_22 value
   * @property {number} IR_VERSION_2019_3_18=5 IR_VERSION_2019_3_18 value
   * @property {number} IR_VERSION_2019_9_19=6 IR_VERSION_2019_9_19 value
   * @property {number} IR_VERSION_2020_5_8=7 IR_VERSION_2020_5_8 value
   * @property {number} IR_VERSION_2021_7_30=8 IR_VERSION_2021_7_30 value
   * @property {number} IR_VERSION=9 IR_VERSION value
   */
  onnx.Version = (function () {
    var valuesById = {},
      values = Object.create(valuesById);
    values[(valuesById[0] = '_START_VERSION')] = 0;
    values[(valuesById[1] = 'IR_VERSION_2017_10_10')] = 1;
    values[(valuesById[2] = 'IR_VERSION_2017_10_30')] = 2;
    values[(valuesById[3] = 'IR_VERSION_2017_11_3')] = 3;
    values[(valuesById[4] = 'IR_VERSION_2019_1_22')] = 4;
    values[(valuesById[5] = 'IR_VERSION_2019_3_18')] = 5;
    values[(valuesById[6] = 'IR_VERSION_2019_9_19')] = 6;
    values[(valuesById[7] = 'IR_VERSION_2020_5_8')] = 7;
    values[(valuesById[8] = 'IR_VERSION_2021_7_30')] = 8;
    values[(valuesById[9] = 'IR_VERSION')] = 9;
    return values;
  })();

  onnx.AttributeProto = (function () {
    /**
     * Properties of an AttributeProto.
     * @memberof onnx
     * @interface IAttributeProto
     * @property {string|null} [name] AttributeProto name
     * @property {string|null} [refAttrName] AttributeProto refAttrName
     * @property {string|null} [docString] AttributeProto docString
     * @property {onnx.AttributeProto.AttributeType|null} [type] AttributeProto type
     * @property {number|null} [f] AttributeProto f
     * @property {number|Long|null} [i] AttributeProto i
     * @property {Uint8Array|null} [s] AttributeProto s
     * @property {onnx.ITensorProto|null} [t] AttributeProto t
     * @property {onnx.IGraphProto|null} [g] AttributeProto g
     * @property {onnx.ISparseTensorProto|null} [sparseTensor] AttributeProto sparseTensor
     * @property {onnx.ITypeProto|null} [tp] AttributeProto tp
     * @property {Array.<number>|null} [floats] AttributeProto floats
     * @property {Array.<number|Long>|null} [ints] AttributeProto ints
     * @property {Array.<Uint8Array>|null} [strings] AttributeProto strings
     * @property {Array.<onnx.ITensorProto>|null} [tensors] AttributeProto tensors
     * @property {Array.<onnx.IGraphProto>|null} [graphs] AttributeProto graphs
     * @property {Array.<onnx.ISparseTensorProto>|null} [sparseTensors] AttributeProto sparseTensors
     * @property {Array.<onnx.ITypeProto>|null} [typeProtos] AttributeProto typeProtos
     */

    /**
     * Constructs a new AttributeProto.
     * @memberof onnx
     * @classdesc Represents an AttributeProto.
     * @implements IAttributeProto
     * @constructor
     * @param {onnx.IAttributeProto=} [properties] Properties to set
     */
    function AttributeProto(properties) {
      this.floats = [];
      this.ints = [];
      this.strings = [];
      this.tensors = [];
      this.graphs = [];
      this.sparseTensors = [];
      this.typeProtos = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * AttributeProto name.
     * @member {string} name
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.name = '';

    /**
     * AttributeProto refAttrName.
     * @member {string} refAttrName
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.refAttrName = '';

    /**
     * AttributeProto docString.
     * @member {string} docString
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.docString = '';

    /**
     * AttributeProto type.
     * @member {onnx.AttributeProto.AttributeType} type
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.type = 0;

    /**
     * AttributeProto f.
     * @member {number} f
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.f = 0;

    /**
     * AttributeProto i.
     * @member {number|Long} i
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.i = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

    /**
     * AttributeProto s.
     * @member {Uint8Array} s
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.s = $util.newBuffer([]);

    /**
     * AttributeProto t.
     * @member {onnx.ITensorProto|null|undefined} t
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.t = null;

    /**
     * AttributeProto g.
     * @member {onnx.IGraphProto|null|undefined} g
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.g = null;

    /**
     * AttributeProto sparseTensor.
     * @member {onnx.ISparseTensorProto|null|undefined} sparseTensor
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.sparseTensor = null;

    /**
     * AttributeProto tp.
     * @member {onnx.ITypeProto|null|undefined} tp
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.tp = null;

    /**
     * AttributeProto floats.
     * @member {Array.<number>} floats
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.floats = $util.emptyArray;

    /**
     * AttributeProto ints.
     * @member {Array.<number|Long>} ints
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.ints = $util.emptyArray;

    /**
     * AttributeProto strings.
     * @member {Array.<Uint8Array>} strings
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.strings = $util.emptyArray;

    /**
     * AttributeProto tensors.
     * @member {Array.<onnx.ITensorProto>} tensors
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.tensors = $util.emptyArray;

    /**
     * AttributeProto graphs.
     * @member {Array.<onnx.IGraphProto>} graphs
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.graphs = $util.emptyArray;

    /**
     * AttributeProto sparseTensors.
     * @member {Array.<onnx.ISparseTensorProto>} sparseTensors
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.sparseTensors = $util.emptyArray;

    /**
     * AttributeProto typeProtos.
     * @member {Array.<onnx.ITypeProto>} typeProtos
     * @memberof onnx.AttributeProto
     * @instance
     */
    AttributeProto.prototype.typeProtos = $util.emptyArray;

    /**
     * Creates a new AttributeProto instance using the specified properties.
     * @function create
     * @memberof onnx.AttributeProto
     * @static
     * @param {onnx.IAttributeProto=} [properties] Properties to set
     * @returns {onnx.AttributeProto} AttributeProto instance
     */
    AttributeProto.create = function create(properties) {
      return new AttributeProto(properties);
    };

    /**
     * Encodes the specified AttributeProto message. Does not implicitly {@link onnx.AttributeProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.AttributeProto
     * @static
     * @param {onnx.IAttributeProto} message AttributeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    AttributeProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.name);
      if (message.f != null && Object.hasOwnProperty.call(message, 'f'))
        writer.uint32(/* id 2, wireType 5 =*/ 21).float(message.f);
      if (message.i != null && Object.hasOwnProperty.call(message, 'i'))
        writer.uint32(/* id 3, wireType 0 =*/ 24).int64(message.i);
      if (message.s != null && Object.hasOwnProperty.call(message, 's'))
        writer.uint32(/* id 4, wireType 2 =*/ 34).bytes(message.s);
      if (message.t != null && Object.hasOwnProperty.call(message, 't'))
        $root.onnx.TensorProto.encode(message.t, writer.uint32(/* id 5, wireType 2 =*/ 42).fork()).ldelim();
      if (message.g != null && Object.hasOwnProperty.call(message, 'g'))
        $root.onnx.GraphProto.encode(message.g, writer.uint32(/* id 6, wireType 2 =*/ 50).fork()).ldelim();
      if (message.floats != null && message.floats.length) {
        writer.uint32(/* id 7, wireType 2 =*/ 58).fork();
        for (var i = 0; i < message.floats.length; ++i) writer.float(message.floats[i]);
        writer.ldelim();
      }
      if (message.ints != null && message.ints.length) {
        writer.uint32(/* id 8, wireType 2 =*/ 66).fork();
        for (var i = 0; i < message.ints.length; ++i) writer.int64(message.ints[i]);
        writer.ldelim();
      }
      if (message.strings != null && message.strings.length)
        for (var i = 0; i < message.strings.length; ++i)
          writer.uint32(/* id 9, wireType 2 =*/ 74).bytes(message.strings[i]);
      if (message.tensors != null && message.tensors.length)
        for (var i = 0; i < message.tensors.length; ++i)
          $root.onnx.TensorProto.encode(message.tensors[i], writer.uint32(/* id 10, wireType 2 =*/ 82).fork()).ldelim();
      if (message.graphs != null && message.graphs.length)
        for (var i = 0; i < message.graphs.length; ++i)
          $root.onnx.GraphProto.encode(message.graphs[i], writer.uint32(/* id 11, wireType 2 =*/ 90).fork()).ldelim();
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 13, wireType 2 =*/ 106).string(message.docString);
      if (message.tp != null && Object.hasOwnProperty.call(message, 'tp'))
        $root.onnx.TypeProto.encode(message.tp, writer.uint32(/* id 14, wireType 2 =*/ 114).fork()).ldelim();
      if (message.typeProtos != null && message.typeProtos.length)
        for (var i = 0; i < message.typeProtos.length; ++i)
          $root.onnx.TypeProto.encode(
            message.typeProtos[i],
            writer.uint32(/* id 15, wireType 2 =*/ 122).fork(),
          ).ldelim();
      if (message.type != null && Object.hasOwnProperty.call(message, 'type'))
        writer.uint32(/* id 20, wireType 0 =*/ 160).int32(message.type);
      if (message.refAttrName != null && Object.hasOwnProperty.call(message, 'refAttrName'))
        writer.uint32(/* id 21, wireType 2 =*/ 170).string(message.refAttrName);
      if (message.sparseTensor != null && Object.hasOwnProperty.call(message, 'sparseTensor'))
        $root.onnx.SparseTensorProto.encode(
          message.sparseTensor,
          writer.uint32(/* id 22, wireType 2 =*/ 178).fork(),
        ).ldelim();
      if (message.sparseTensors != null && message.sparseTensors.length)
        for (var i = 0; i < message.sparseTensors.length; ++i)
          $root.onnx.SparseTensorProto.encode(
            message.sparseTensors[i],
            writer.uint32(/* id 23, wireType 2 =*/ 186).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified AttributeProto message, length delimited. Does not implicitly {@link onnx.AttributeProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.AttributeProto
     * @static
     * @param {onnx.IAttributeProto} message AttributeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    AttributeProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an AttributeProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.AttributeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.AttributeProto} AttributeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    AttributeProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.AttributeProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.name = reader.string();
            break;
          }
          case 21: {
            message.refAttrName = reader.string();
            break;
          }
          case 13: {
            message.docString = reader.string();
            break;
          }
          case 20: {
            message.type = reader.int32();
            break;
          }
          case 2: {
            message.f = reader.float();
            break;
          }
          case 3: {
            message.i = reader.int64();
            break;
          }
          case 4: {
            message.s = reader.bytes();
            break;
          }
          case 5: {
            message.t = $root.onnx.TensorProto.decode(reader, reader.uint32());
            break;
          }
          case 6: {
            message.g = $root.onnx.GraphProto.decode(reader, reader.uint32());
            break;
          }
          case 22: {
            message.sparseTensor = $root.onnx.SparseTensorProto.decode(reader, reader.uint32());
            break;
          }
          case 14: {
            message.tp = $root.onnx.TypeProto.decode(reader, reader.uint32());
            break;
          }
          case 7: {
            if (!(message.floats && message.floats.length)) message.floats = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.floats.push(reader.float());
            } else message.floats.push(reader.float());
            break;
          }
          case 8: {
            if (!(message.ints && message.ints.length)) message.ints = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.ints.push(reader.int64());
            } else message.ints.push(reader.int64());
            break;
          }
          case 9: {
            if (!(message.strings && message.strings.length)) message.strings = [];
            message.strings.push(reader.bytes());
            break;
          }
          case 10: {
            if (!(message.tensors && message.tensors.length)) message.tensors = [];
            message.tensors.push($root.onnx.TensorProto.decode(reader, reader.uint32()));
            break;
          }
          case 11: {
            if (!(message.graphs && message.graphs.length)) message.graphs = [];
            message.graphs.push($root.onnx.GraphProto.decode(reader, reader.uint32()));
            break;
          }
          case 23: {
            if (!(message.sparseTensors && message.sparseTensors.length)) message.sparseTensors = [];
            message.sparseTensors.push($root.onnx.SparseTensorProto.decode(reader, reader.uint32()));
            break;
          }
          case 15: {
            if (!(message.typeProtos && message.typeProtos.length)) message.typeProtos = [];
            message.typeProtos.push($root.onnx.TypeProto.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes an AttributeProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.AttributeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.AttributeProto} AttributeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    AttributeProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an AttributeProto message.
     * @function verify
     * @memberof onnx.AttributeProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    AttributeProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.refAttrName != null && message.hasOwnProperty('refAttrName'))
        if (!$util.isString(message.refAttrName)) return 'refAttrName: string expected';
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      if (message.type != null && message.hasOwnProperty('type'))
        switch (message.type) {
          default:
            return 'type: enum value expected';
          case 0:
          case 1:
          case 2:
          case 3:
          case 4:
          case 5:
          case 11:
          case 13:
          case 6:
          case 7:
          case 8:
          case 9:
          case 10:
          case 12:
          case 14:
            break;
        }
      if (message.f != null && message.hasOwnProperty('f'))
        if (typeof message.f !== 'number') return 'f: number expected';
      if (message.i != null && message.hasOwnProperty('i'))
        if (
          !$util.isInteger(message.i) &&
          !(message.i && $util.isInteger(message.i.low) && $util.isInteger(message.i.high))
        )
          return 'i: integer|Long expected';
      if (message.s != null && message.hasOwnProperty('s'))
        if (!((message.s && typeof message.s.length === 'number') || $util.isString(message.s)))
          return 's: buffer expected';
      if (message.t != null && message.hasOwnProperty('t')) {
        var error = $root.onnx.TensorProto.verify(message.t);
        if (error) return 't.' + error;
      }
      if (message.g != null && message.hasOwnProperty('g')) {
        var error = $root.onnx.GraphProto.verify(message.g);
        if (error) return 'g.' + error;
      }
      if (message.sparseTensor != null && message.hasOwnProperty('sparseTensor')) {
        var error = $root.onnx.SparseTensorProto.verify(message.sparseTensor);
        if (error) return 'sparseTensor.' + error;
      }
      if (message.tp != null && message.hasOwnProperty('tp')) {
        var error = $root.onnx.TypeProto.verify(message.tp);
        if (error) return 'tp.' + error;
      }
      if (message.floats != null && message.hasOwnProperty('floats')) {
        if (!Array.isArray(message.floats)) return 'floats: array expected';
        for (var i = 0; i < message.floats.length; ++i)
          if (typeof message.floats[i] !== 'number') return 'floats: number[] expected';
      }
      if (message.ints != null && message.hasOwnProperty('ints')) {
        if (!Array.isArray(message.ints)) return 'ints: array expected';
        for (var i = 0; i < message.ints.length; ++i)
          if (
            !$util.isInteger(message.ints[i]) &&
            !(message.ints[i] && $util.isInteger(message.ints[i].low) && $util.isInteger(message.ints[i].high))
          )
            return 'ints: integer|Long[] expected';
      }
      if (message.strings != null && message.hasOwnProperty('strings')) {
        if (!Array.isArray(message.strings)) return 'strings: array expected';
        for (var i = 0; i < message.strings.length; ++i)
          if (
            !(
              (message.strings[i] && typeof message.strings[i].length === 'number') ||
              $util.isString(message.strings[i])
            )
          )
            return 'strings: buffer[] expected';
      }
      if (message.tensors != null && message.hasOwnProperty('tensors')) {
        if (!Array.isArray(message.tensors)) return 'tensors: array expected';
        for (var i = 0; i < message.tensors.length; ++i) {
          var error = $root.onnx.TensorProto.verify(message.tensors[i]);
          if (error) return 'tensors.' + error;
        }
      }
      if (message.graphs != null && message.hasOwnProperty('graphs')) {
        if (!Array.isArray(message.graphs)) return 'graphs: array expected';
        for (var i = 0; i < message.graphs.length; ++i) {
          var error = $root.onnx.GraphProto.verify(message.graphs[i]);
          if (error) return 'graphs.' + error;
        }
      }
      if (message.sparseTensors != null && message.hasOwnProperty('sparseTensors')) {
        if (!Array.isArray(message.sparseTensors)) return 'sparseTensors: array expected';
        for (var i = 0; i < message.sparseTensors.length; ++i) {
          var error = $root.onnx.SparseTensorProto.verify(message.sparseTensors[i]);
          if (error) return 'sparseTensors.' + error;
        }
      }
      if (message.typeProtos != null && message.hasOwnProperty('typeProtos')) {
        if (!Array.isArray(message.typeProtos)) return 'typeProtos: array expected';
        for (var i = 0; i < message.typeProtos.length; ++i) {
          var error = $root.onnx.TypeProto.verify(message.typeProtos[i]);
          if (error) return 'typeProtos.' + error;
        }
      }
      return null;
    };

    /**
     * Creates an AttributeProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.AttributeProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.AttributeProto} AttributeProto
     */
    AttributeProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.AttributeProto) return object;
      var message = new $root.onnx.AttributeProto();
      if (object.name != null) message.name = String(object.name);
      if (object.refAttrName != null) message.refAttrName = String(object.refAttrName);
      if (object.docString != null) message.docString = String(object.docString);
      switch (object.type) {
        default:
          if (typeof object.type === 'number') {
            message.type = object.type;
            break;
          }
          break;
        case 'UNDEFINED':
        case 0:
          message.type = 0;
          break;
        case 'FLOAT':
        case 1:
          message.type = 1;
          break;
        case 'INT':
        case 2:
          message.type = 2;
          break;
        case 'STRING':
        case 3:
          message.type = 3;
          break;
        case 'TENSOR':
        case 4:
          message.type = 4;
          break;
        case 'GRAPH':
        case 5:
          message.type = 5;
          break;
        case 'SPARSE_TENSOR':
        case 11:
          message.type = 11;
          break;
        case 'TYPE_PROTO':
        case 13:
          message.type = 13;
          break;
        case 'FLOATS':
        case 6:
          message.type = 6;
          break;
        case 'INTS':
        case 7:
          message.type = 7;
          break;
        case 'STRINGS':
        case 8:
          message.type = 8;
          break;
        case 'TENSORS':
        case 9:
          message.type = 9;
          break;
        case 'GRAPHS':
        case 10:
          message.type = 10;
          break;
        case 'SPARSE_TENSORS':
        case 12:
          message.type = 12;
          break;
        case 'TYPE_PROTOS':
        case 14:
          message.type = 14;
          break;
      }
      if (object.f != null) message.f = Number(object.f);
      if (object.i != null)
        if ($util.Long) (message.i = $util.Long.fromValue(object.i)).unsigned = false;
        else if (typeof object.i === 'string') message.i = parseInt(object.i, 10);
        else if (typeof object.i === 'number') message.i = object.i;
        else if (typeof object.i === 'object')
          message.i = new $util.LongBits(object.i.low >>> 0, object.i.high >>> 0).toNumber();
      if (object.s != null)
        if (typeof object.s === 'string')
          $util.base64.decode(object.s, (message.s = $util.newBuffer($util.base64.length(object.s))), 0);
        else if (object.s.length >= 0) message.s = object.s;
      if (object.t != null) {
        if (typeof object.t !== 'object') throw TypeError('.onnx.AttributeProto.t: object expected');
        message.t = $root.onnx.TensorProto.fromObject(object.t);
      }
      if (object.g != null) {
        if (typeof object.g !== 'object') throw TypeError('.onnx.AttributeProto.g: object expected');
        message.g = $root.onnx.GraphProto.fromObject(object.g);
      }
      if (object.sparseTensor != null) {
        if (typeof object.sparseTensor !== 'object')
          throw TypeError('.onnx.AttributeProto.sparseTensor: object expected');
        message.sparseTensor = $root.onnx.SparseTensorProto.fromObject(object.sparseTensor);
      }
      if (object.tp != null) {
        if (typeof object.tp !== 'object') throw TypeError('.onnx.AttributeProto.tp: object expected');
        message.tp = $root.onnx.TypeProto.fromObject(object.tp);
      }
      if (object.floats) {
        if (!Array.isArray(object.floats)) throw TypeError('.onnx.AttributeProto.floats: array expected');
        message.floats = [];
        for (var i = 0; i < object.floats.length; ++i) message.floats[i] = Number(object.floats[i]);
      }
      if (object.ints) {
        if (!Array.isArray(object.ints)) throw TypeError('.onnx.AttributeProto.ints: array expected');
        message.ints = [];
        for (var i = 0; i < object.ints.length; ++i)
          if ($util.Long) (message.ints[i] = $util.Long.fromValue(object.ints[i])).unsigned = false;
          else if (typeof object.ints[i] === 'string') message.ints[i] = parseInt(object.ints[i], 10);
          else if (typeof object.ints[i] === 'number') message.ints[i] = object.ints[i];
          else if (typeof object.ints[i] === 'object')
            message.ints[i] = new $util.LongBits(object.ints[i].low >>> 0, object.ints[i].high >>> 0).toNumber();
      }
      if (object.strings) {
        if (!Array.isArray(object.strings)) throw TypeError('.onnx.AttributeProto.strings: array expected');
        message.strings = [];
        for (var i = 0; i < object.strings.length; ++i)
          if (typeof object.strings[i] === 'string')
            $util.base64.decode(
              object.strings[i],
              (message.strings[i] = $util.newBuffer($util.base64.length(object.strings[i]))),
              0,
            );
          else if (object.strings[i].length >= 0) message.strings[i] = object.strings[i];
      }
      if (object.tensors) {
        if (!Array.isArray(object.tensors)) throw TypeError('.onnx.AttributeProto.tensors: array expected');
        message.tensors = [];
        for (var i = 0; i < object.tensors.length; ++i) {
          if (typeof object.tensors[i] !== 'object') throw TypeError('.onnx.AttributeProto.tensors: object expected');
          message.tensors[i] = $root.onnx.TensorProto.fromObject(object.tensors[i]);
        }
      }
      if (object.graphs) {
        if (!Array.isArray(object.graphs)) throw TypeError('.onnx.AttributeProto.graphs: array expected');
        message.graphs = [];
        for (var i = 0; i < object.graphs.length; ++i) {
          if (typeof object.graphs[i] !== 'object') throw TypeError('.onnx.AttributeProto.graphs: object expected');
          message.graphs[i] = $root.onnx.GraphProto.fromObject(object.graphs[i]);
        }
      }
      if (object.sparseTensors) {
        if (!Array.isArray(object.sparseTensors)) throw TypeError('.onnx.AttributeProto.sparseTensors: array expected');
        message.sparseTensors = [];
        for (var i = 0; i < object.sparseTensors.length; ++i) {
          if (typeof object.sparseTensors[i] !== 'object')
            throw TypeError('.onnx.AttributeProto.sparseTensors: object expected');
          message.sparseTensors[i] = $root.onnx.SparseTensorProto.fromObject(object.sparseTensors[i]);
        }
      }
      if (object.typeProtos) {
        if (!Array.isArray(object.typeProtos)) throw TypeError('.onnx.AttributeProto.typeProtos: array expected');
        message.typeProtos = [];
        for (var i = 0; i < object.typeProtos.length; ++i) {
          if (typeof object.typeProtos[i] !== 'object')
            throw TypeError('.onnx.AttributeProto.typeProtos: object expected');
          message.typeProtos[i] = $root.onnx.TypeProto.fromObject(object.typeProtos[i]);
        }
      }
      return message;
    };

    /**
     * Creates a plain object from an AttributeProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.AttributeProto
     * @static
     * @param {onnx.AttributeProto} message AttributeProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    AttributeProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.floats = [];
        object.ints = [];
        object.strings = [];
        object.tensors = [];
        object.graphs = [];
        object.typeProtos = [];
        object.sparseTensors = [];
      }
      if (options.defaults) {
        object.name = '';
        object.f = 0;
        if ($util.Long) {
          var long = new $util.Long(0, 0, false);
          object.i = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
        } else object.i = options.longs === String ? '0' : 0;
        if (options.bytes === String) object.s = '';
        else {
          object.s = [];
          if (options.bytes !== Array) object.s = $util.newBuffer(object.s);
        }
        object.t = null;
        object.g = null;
        object.docString = '';
        object.tp = null;
        object.type = options.enums === String ? 'UNDEFINED' : 0;
        object.refAttrName = '';
        object.sparseTensor = null;
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.f != null && message.hasOwnProperty('f'))
        object.f = options.json && !isFinite(message.f) ? String(message.f) : message.f;
      if (message.i != null && message.hasOwnProperty('i'))
        if (typeof message.i === 'number') object.i = options.longs === String ? String(message.i) : message.i;
        else
          object.i =
            options.longs === String
              ? $util.Long.prototype.toString.call(message.i)
              : options.longs === Number
                ? new $util.LongBits(message.i.low >>> 0, message.i.high >>> 0).toNumber()
                : message.i;
      if (message.s != null && message.hasOwnProperty('s'))
        object.s =
          options.bytes === String
            ? $util.base64.encode(message.s, 0, message.s.length)
            : options.bytes === Array
              ? Array.prototype.slice.call(message.s)
              : message.s;
      if (message.t != null && message.hasOwnProperty('t'))
        object.t = $root.onnx.TensorProto.toObject(message.t, options);
      if (message.g != null && message.hasOwnProperty('g'))
        object.g = $root.onnx.GraphProto.toObject(message.g, options);
      if (message.floats && message.floats.length) {
        object.floats = [];
        for (var j = 0; j < message.floats.length; ++j)
          object.floats[j] =
            options.json && !isFinite(message.floats[j]) ? String(message.floats[j]) : message.floats[j];
      }
      if (message.ints && message.ints.length) {
        object.ints = [];
        for (var j = 0; j < message.ints.length; ++j)
          if (typeof message.ints[j] === 'number')
            object.ints[j] = options.longs === String ? String(message.ints[j]) : message.ints[j];
          else
            object.ints[j] =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.ints[j])
                : options.longs === Number
                  ? new $util.LongBits(message.ints[j].low >>> 0, message.ints[j].high >>> 0).toNumber()
                  : message.ints[j];
      }
      if (message.strings && message.strings.length) {
        object.strings = [];
        for (var j = 0; j < message.strings.length; ++j)
          object.strings[j] =
            options.bytes === String
              ? $util.base64.encode(message.strings[j], 0, message.strings[j].length)
              : options.bytes === Array
                ? Array.prototype.slice.call(message.strings[j])
                : message.strings[j];
      }
      if (message.tensors && message.tensors.length) {
        object.tensors = [];
        for (var j = 0; j < message.tensors.length; ++j)
          object.tensors[j] = $root.onnx.TensorProto.toObject(message.tensors[j], options);
      }
      if (message.graphs && message.graphs.length) {
        object.graphs = [];
        for (var j = 0; j < message.graphs.length; ++j)
          object.graphs[j] = $root.onnx.GraphProto.toObject(message.graphs[j], options);
      }
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.tp != null && message.hasOwnProperty('tp'))
        object.tp = $root.onnx.TypeProto.toObject(message.tp, options);
      if (message.typeProtos && message.typeProtos.length) {
        object.typeProtos = [];
        for (var j = 0; j < message.typeProtos.length; ++j)
          object.typeProtos[j] = $root.onnx.TypeProto.toObject(message.typeProtos[j], options);
      }
      if (message.type != null && message.hasOwnProperty('type'))
        object.type =
          options.enums === String
            ? $root.onnx.AttributeProto.AttributeType[message.type] === undefined
              ? message.type
              : $root.onnx.AttributeProto.AttributeType[message.type]
            : message.type;
      if (message.refAttrName != null && message.hasOwnProperty('refAttrName'))
        object.refAttrName = message.refAttrName;
      if (message.sparseTensor != null && message.hasOwnProperty('sparseTensor'))
        object.sparseTensor = $root.onnx.SparseTensorProto.toObject(message.sparseTensor, options);
      if (message.sparseTensors && message.sparseTensors.length) {
        object.sparseTensors = [];
        for (var j = 0; j < message.sparseTensors.length; ++j)
          object.sparseTensors[j] = $root.onnx.SparseTensorProto.toObject(message.sparseTensors[j], options);
      }
      return object;
    };

    /**
     * Converts this AttributeProto to JSON.
     * @function toJSON
     * @memberof onnx.AttributeProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    AttributeProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for AttributeProto
     * @function getTypeUrl
     * @memberof onnx.AttributeProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    AttributeProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.AttributeProto';
    };

    /**
     * AttributeType enum.
     * @name onnx.AttributeProto.AttributeType
     * @enum {number}
     * @property {number} UNDEFINED=0 UNDEFINED value
     * @property {number} FLOAT=1 FLOAT value
     * @property {number} INT=2 INT value
     * @property {number} STRING=3 STRING value
     * @property {number} TENSOR=4 TENSOR value
     * @property {number} GRAPH=5 GRAPH value
     * @property {number} SPARSE_TENSOR=11 SPARSE_TENSOR value
     * @property {number} TYPE_PROTO=13 TYPE_PROTO value
     * @property {number} FLOATS=6 FLOATS value
     * @property {number} INTS=7 INTS value
     * @property {number} STRINGS=8 STRINGS value
     * @property {number} TENSORS=9 TENSORS value
     * @property {number} GRAPHS=10 GRAPHS value
     * @property {number} SPARSE_TENSORS=12 SPARSE_TENSORS value
     * @property {number} TYPE_PROTOS=14 TYPE_PROTOS value
     */
    AttributeProto.AttributeType = (function () {
      var valuesById = {},
        values = Object.create(valuesById);
      values[(valuesById[0] = 'UNDEFINED')] = 0;
      values[(valuesById[1] = 'FLOAT')] = 1;
      values[(valuesById[2] = 'INT')] = 2;
      values[(valuesById[3] = 'STRING')] = 3;
      values[(valuesById[4] = 'TENSOR')] = 4;
      values[(valuesById[5] = 'GRAPH')] = 5;
      values[(valuesById[11] = 'SPARSE_TENSOR')] = 11;
      values[(valuesById[13] = 'TYPE_PROTO')] = 13;
      values[(valuesById[6] = 'FLOATS')] = 6;
      values[(valuesById[7] = 'INTS')] = 7;
      values[(valuesById[8] = 'STRINGS')] = 8;
      values[(valuesById[9] = 'TENSORS')] = 9;
      values[(valuesById[10] = 'GRAPHS')] = 10;
      values[(valuesById[12] = 'SPARSE_TENSORS')] = 12;
      values[(valuesById[14] = 'TYPE_PROTOS')] = 14;
      return values;
    })();

    return AttributeProto;
  })();

  onnx.ValueInfoProto = (function () {
    /**
     * Properties of a ValueInfoProto.
     * @memberof onnx
     * @interface IValueInfoProto
     * @property {string|null} [name] ValueInfoProto name
     * @property {onnx.ITypeProto|null} [type] ValueInfoProto type
     * @property {string|null} [docString] ValueInfoProto docString
     */

    /**
     * Constructs a new ValueInfoProto.
     * @memberof onnx
     * @classdesc Represents a ValueInfoProto.
     * @implements IValueInfoProto
     * @constructor
     * @param {onnx.IValueInfoProto=} [properties] Properties to set
     */
    function ValueInfoProto(properties) {
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * ValueInfoProto name.
     * @member {string} name
     * @memberof onnx.ValueInfoProto
     * @instance
     */
    ValueInfoProto.prototype.name = '';

    /**
     * ValueInfoProto type.
     * @member {onnx.ITypeProto|null|undefined} type
     * @memberof onnx.ValueInfoProto
     * @instance
     */
    ValueInfoProto.prototype.type = null;

    /**
     * ValueInfoProto docString.
     * @member {string} docString
     * @memberof onnx.ValueInfoProto
     * @instance
     */
    ValueInfoProto.prototype.docString = '';

    /**
     * Creates a new ValueInfoProto instance using the specified properties.
     * @function create
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {onnx.IValueInfoProto=} [properties] Properties to set
     * @returns {onnx.ValueInfoProto} ValueInfoProto instance
     */
    ValueInfoProto.create = function create(properties) {
      return new ValueInfoProto(properties);
    };

    /**
     * Encodes the specified ValueInfoProto message. Does not implicitly {@link onnx.ValueInfoProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {onnx.IValueInfoProto} message ValueInfoProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ValueInfoProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.name);
      if (message.type != null && Object.hasOwnProperty.call(message, 'type'))
        $root.onnx.TypeProto.encode(message.type, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.docString);
      return writer;
    };

    /**
     * Encodes the specified ValueInfoProto message, length delimited. Does not implicitly {@link onnx.ValueInfoProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {onnx.IValueInfoProto} message ValueInfoProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ValueInfoProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a ValueInfoProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.ValueInfoProto} ValueInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ValueInfoProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.ValueInfoProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.name = reader.string();
            break;
          }
          case 2: {
            message.type = $root.onnx.TypeProto.decode(reader, reader.uint32());
            break;
          }
          case 3: {
            message.docString = reader.string();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a ValueInfoProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.ValueInfoProto} ValueInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ValueInfoProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a ValueInfoProto message.
     * @function verify
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ValueInfoProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.type != null && message.hasOwnProperty('type')) {
        var error = $root.onnx.TypeProto.verify(message.type);
        if (error) return 'type.' + error;
      }
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      return null;
    };

    /**
     * Creates a ValueInfoProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.ValueInfoProto} ValueInfoProto
     */
    ValueInfoProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.ValueInfoProto) return object;
      var message = new $root.onnx.ValueInfoProto();
      if (object.name != null) message.name = String(object.name);
      if (object.type != null) {
        if (typeof object.type !== 'object') throw TypeError('.onnx.ValueInfoProto.type: object expected');
        message.type = $root.onnx.TypeProto.fromObject(object.type);
      }
      if (object.docString != null) message.docString = String(object.docString);
      return message;
    };

    /**
     * Creates a plain object from a ValueInfoProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {onnx.ValueInfoProto} message ValueInfoProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ValueInfoProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.defaults) {
        object.name = '';
        object.type = null;
        object.docString = '';
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.type != null && message.hasOwnProperty('type'))
        object.type = $root.onnx.TypeProto.toObject(message.type, options);
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      return object;
    };

    /**
     * Converts this ValueInfoProto to JSON.
     * @function toJSON
     * @memberof onnx.ValueInfoProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ValueInfoProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for ValueInfoProto
     * @function getTypeUrl
     * @memberof onnx.ValueInfoProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    ValueInfoProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.ValueInfoProto';
    };

    return ValueInfoProto;
  })();

  onnx.NodeProto = (function () {
    /**
     * Properties of a NodeProto.
     * @memberof onnx
     * @interface INodeProto
     * @property {Array.<string>|null} [input] NodeProto input
     * @property {Array.<string>|null} [output] NodeProto output
     * @property {string|null} [name] NodeProto name
     * @property {string|null} [opType] NodeProto opType
     * @property {string|null} [domain] NodeProto domain
     * @property {Array.<onnx.IAttributeProto>|null} [attribute] NodeProto attribute
     * @property {string|null} [docString] NodeProto docString
     */

    /**
     * Constructs a new NodeProto.
     * @memberof onnx
     * @classdesc Represents a NodeProto.
     * @implements INodeProto
     * @constructor
     * @param {onnx.INodeProto=} [properties] Properties to set
     */
    function NodeProto(properties) {
      this.input = [];
      this.output = [];
      this.attribute = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * NodeProto input.
     * @member {Array.<string>} input
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.input = $util.emptyArray;

    /**
     * NodeProto output.
     * @member {Array.<string>} output
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.output = $util.emptyArray;

    /**
     * NodeProto name.
     * @member {string} name
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.name = '';

    /**
     * NodeProto opType.
     * @member {string} opType
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.opType = '';

    /**
     * NodeProto domain.
     * @member {string} domain
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.domain = '';

    /**
     * NodeProto attribute.
     * @member {Array.<onnx.IAttributeProto>} attribute
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.attribute = $util.emptyArray;

    /**
     * NodeProto docString.
     * @member {string} docString
     * @memberof onnx.NodeProto
     * @instance
     */
    NodeProto.prototype.docString = '';

    /**
     * Creates a new NodeProto instance using the specified properties.
     * @function create
     * @memberof onnx.NodeProto
     * @static
     * @param {onnx.INodeProto=} [properties] Properties to set
     * @returns {onnx.NodeProto} NodeProto instance
     */
    NodeProto.create = function create(properties) {
      return new NodeProto(properties);
    };

    /**
     * Encodes the specified NodeProto message. Does not implicitly {@link onnx.NodeProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.NodeProto
     * @static
     * @param {onnx.INodeProto} message NodeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    NodeProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.input != null && message.input.length)
        for (var i = 0; i < message.input.length; ++i)
          writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.input[i]);
      if (message.output != null && message.output.length)
        for (var i = 0; i < message.output.length; ++i)
          writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.output[i]);
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.name);
      if (message.opType != null && Object.hasOwnProperty.call(message, 'opType'))
        writer.uint32(/* id 4, wireType 2 =*/ 34).string(message.opType);
      if (message.attribute != null && message.attribute.length)
        for (var i = 0; i < message.attribute.length; ++i)
          $root.onnx.AttributeProto.encode(
            message.attribute[i],
            writer.uint32(/* id 5, wireType 2 =*/ 42).fork(),
          ).ldelim();
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.docString);
      if (message.domain != null && Object.hasOwnProperty.call(message, 'domain'))
        writer.uint32(/* id 7, wireType 2 =*/ 58).string(message.domain);
      return writer;
    };

    /**
     * Encodes the specified NodeProto message, length delimited. Does not implicitly {@link onnx.NodeProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.NodeProto
     * @static
     * @param {onnx.INodeProto} message NodeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    NodeProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a NodeProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.NodeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.NodeProto} NodeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    NodeProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.NodeProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            if (!(message.input && message.input.length)) message.input = [];
            message.input.push(reader.string());
            break;
          }
          case 2: {
            if (!(message.output && message.output.length)) message.output = [];
            message.output.push(reader.string());
            break;
          }
          case 3: {
            message.name = reader.string();
            break;
          }
          case 4: {
            message.opType = reader.string();
            break;
          }
          case 7: {
            message.domain = reader.string();
            break;
          }
          case 5: {
            if (!(message.attribute && message.attribute.length)) message.attribute = [];
            message.attribute.push($root.onnx.AttributeProto.decode(reader, reader.uint32()));
            break;
          }
          case 6: {
            message.docString = reader.string();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a NodeProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.NodeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.NodeProto} NodeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    NodeProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a NodeProto message.
     * @function verify
     * @memberof onnx.NodeProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    NodeProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.input != null && message.hasOwnProperty('input')) {
        if (!Array.isArray(message.input)) return 'input: array expected';
        for (var i = 0; i < message.input.length; ++i)
          if (!$util.isString(message.input[i])) return 'input: string[] expected';
      }
      if (message.output != null && message.hasOwnProperty('output')) {
        if (!Array.isArray(message.output)) return 'output: array expected';
        for (var i = 0; i < message.output.length; ++i)
          if (!$util.isString(message.output[i])) return 'output: string[] expected';
      }
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.opType != null && message.hasOwnProperty('opType'))
        if (!$util.isString(message.opType)) return 'opType: string expected';
      if (message.domain != null && message.hasOwnProperty('domain'))
        if (!$util.isString(message.domain)) return 'domain: string expected';
      if (message.attribute != null && message.hasOwnProperty('attribute')) {
        if (!Array.isArray(message.attribute)) return 'attribute: array expected';
        for (var i = 0; i < message.attribute.length; ++i) {
          var error = $root.onnx.AttributeProto.verify(message.attribute[i]);
          if (error) return 'attribute.' + error;
        }
      }
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      return null;
    };

    /**
     * Creates a NodeProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.NodeProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.NodeProto} NodeProto
     */
    NodeProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.NodeProto) return object;
      var message = new $root.onnx.NodeProto();
      if (object.input) {
        if (!Array.isArray(object.input)) throw TypeError('.onnx.NodeProto.input: array expected');
        message.input = [];
        for (var i = 0; i < object.input.length; ++i) message.input[i] = String(object.input[i]);
      }
      if (object.output) {
        if (!Array.isArray(object.output)) throw TypeError('.onnx.NodeProto.output: array expected');
        message.output = [];
        for (var i = 0; i < object.output.length; ++i) message.output[i] = String(object.output[i]);
      }
      if (object.name != null) message.name = String(object.name);
      if (object.opType != null) message.opType = String(object.opType);
      if (object.domain != null) message.domain = String(object.domain);
      if (object.attribute) {
        if (!Array.isArray(object.attribute)) throw TypeError('.onnx.NodeProto.attribute: array expected');
        message.attribute = [];
        for (var i = 0; i < object.attribute.length; ++i) {
          if (typeof object.attribute[i] !== 'object') throw TypeError('.onnx.NodeProto.attribute: object expected');
          message.attribute[i] = $root.onnx.AttributeProto.fromObject(object.attribute[i]);
        }
      }
      if (object.docString != null) message.docString = String(object.docString);
      return message;
    };

    /**
     * Creates a plain object from a NodeProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.NodeProto
     * @static
     * @param {onnx.NodeProto} message NodeProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    NodeProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.input = [];
        object.output = [];
        object.attribute = [];
      }
      if (options.defaults) {
        object.name = '';
        object.opType = '';
        object.docString = '';
        object.domain = '';
      }
      if (message.input && message.input.length) {
        object.input = [];
        for (var j = 0; j < message.input.length; ++j) object.input[j] = message.input[j];
      }
      if (message.output && message.output.length) {
        object.output = [];
        for (var j = 0; j < message.output.length; ++j) object.output[j] = message.output[j];
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.opType != null && message.hasOwnProperty('opType')) object.opType = message.opType;
      if (message.attribute && message.attribute.length) {
        object.attribute = [];
        for (var j = 0; j < message.attribute.length; ++j)
          object.attribute[j] = $root.onnx.AttributeProto.toObject(message.attribute[j], options);
      }
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.domain != null && message.hasOwnProperty('domain')) object.domain = message.domain;
      return object;
    };

    /**
     * Converts this NodeProto to JSON.
     * @function toJSON
     * @memberof onnx.NodeProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    NodeProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for NodeProto
     * @function getTypeUrl
     * @memberof onnx.NodeProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    NodeProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.NodeProto';
    };

    return NodeProto;
  })();

  onnx.TrainingInfoProto = (function () {
    /**
     * Properties of a TrainingInfoProto.
     * @memberof onnx
     * @interface ITrainingInfoProto
     * @property {onnx.IGraphProto|null} [initialization] TrainingInfoProto initialization
     * @property {onnx.IGraphProto|null} [algorithm] TrainingInfoProto algorithm
     * @property {Array.<onnx.IStringStringEntryProto>|null} [initializationBinding] TrainingInfoProto initializationBinding
     * @property {Array.<onnx.IStringStringEntryProto>|null} [updateBinding] TrainingInfoProto updateBinding
     */

    /**
     * Constructs a new TrainingInfoProto.
     * @memberof onnx
     * @classdesc Represents a TrainingInfoProto.
     * @implements ITrainingInfoProto
     * @constructor
     * @param {onnx.ITrainingInfoProto=} [properties] Properties to set
     */
    function TrainingInfoProto(properties) {
      this.initializationBinding = [];
      this.updateBinding = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * TrainingInfoProto initialization.
     * @member {onnx.IGraphProto|null|undefined} initialization
     * @memberof onnx.TrainingInfoProto
     * @instance
     */
    TrainingInfoProto.prototype.initialization = null;

    /**
     * TrainingInfoProto algorithm.
     * @member {onnx.IGraphProto|null|undefined} algorithm
     * @memberof onnx.TrainingInfoProto
     * @instance
     */
    TrainingInfoProto.prototype.algorithm = null;

    /**
     * TrainingInfoProto initializationBinding.
     * @member {Array.<onnx.IStringStringEntryProto>} initializationBinding
     * @memberof onnx.TrainingInfoProto
     * @instance
     */
    TrainingInfoProto.prototype.initializationBinding = $util.emptyArray;

    /**
     * TrainingInfoProto updateBinding.
     * @member {Array.<onnx.IStringStringEntryProto>} updateBinding
     * @memberof onnx.TrainingInfoProto
     * @instance
     */
    TrainingInfoProto.prototype.updateBinding = $util.emptyArray;

    /**
     * Creates a new TrainingInfoProto instance using the specified properties.
     * @function create
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {onnx.ITrainingInfoProto=} [properties] Properties to set
     * @returns {onnx.TrainingInfoProto} TrainingInfoProto instance
     */
    TrainingInfoProto.create = function create(properties) {
      return new TrainingInfoProto(properties);
    };

    /**
     * Encodes the specified TrainingInfoProto message. Does not implicitly {@link onnx.TrainingInfoProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {onnx.ITrainingInfoProto} message TrainingInfoProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TrainingInfoProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.initialization != null && Object.hasOwnProperty.call(message, 'initialization'))
        $root.onnx.GraphProto.encode(message.initialization, writer.uint32(/* id 1, wireType 2 =*/ 10).fork()).ldelim();
      if (message.algorithm != null && Object.hasOwnProperty.call(message, 'algorithm'))
        $root.onnx.GraphProto.encode(message.algorithm, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
      if (message.initializationBinding != null && message.initializationBinding.length)
        for (var i = 0; i < message.initializationBinding.length; ++i)
          $root.onnx.StringStringEntryProto.encode(
            message.initializationBinding[i],
            writer.uint32(/* id 3, wireType 2 =*/ 26).fork(),
          ).ldelim();
      if (message.updateBinding != null && message.updateBinding.length)
        for (var i = 0; i < message.updateBinding.length; ++i)
          $root.onnx.StringStringEntryProto.encode(
            message.updateBinding[i],
            writer.uint32(/* id 4, wireType 2 =*/ 34).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified TrainingInfoProto message, length delimited. Does not implicitly {@link onnx.TrainingInfoProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {onnx.ITrainingInfoProto} message TrainingInfoProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TrainingInfoProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a TrainingInfoProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.TrainingInfoProto} TrainingInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TrainingInfoProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.TrainingInfoProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.initialization = $root.onnx.GraphProto.decode(reader, reader.uint32());
            break;
          }
          case 2: {
            message.algorithm = $root.onnx.GraphProto.decode(reader, reader.uint32());
            break;
          }
          case 3: {
            if (!(message.initializationBinding && message.initializationBinding.length))
              message.initializationBinding = [];
            message.initializationBinding.push($root.onnx.StringStringEntryProto.decode(reader, reader.uint32()));
            break;
          }
          case 4: {
            if (!(message.updateBinding && message.updateBinding.length)) message.updateBinding = [];
            message.updateBinding.push($root.onnx.StringStringEntryProto.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a TrainingInfoProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.TrainingInfoProto} TrainingInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TrainingInfoProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a TrainingInfoProto message.
     * @function verify
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    TrainingInfoProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.initialization != null && message.hasOwnProperty('initialization')) {
        var error = $root.onnx.GraphProto.verify(message.initialization);
        if (error) return 'initialization.' + error;
      }
      if (message.algorithm != null && message.hasOwnProperty('algorithm')) {
        var error = $root.onnx.GraphProto.verify(message.algorithm);
        if (error) return 'algorithm.' + error;
      }
      if (message.initializationBinding != null && message.hasOwnProperty('initializationBinding')) {
        if (!Array.isArray(message.initializationBinding)) return 'initializationBinding: array expected';
        for (var i = 0; i < message.initializationBinding.length; ++i) {
          var error = $root.onnx.StringStringEntryProto.verify(message.initializationBinding[i]);
          if (error) return 'initializationBinding.' + error;
        }
      }
      if (message.updateBinding != null && message.hasOwnProperty('updateBinding')) {
        if (!Array.isArray(message.updateBinding)) return 'updateBinding: array expected';
        for (var i = 0; i < message.updateBinding.length; ++i) {
          var error = $root.onnx.StringStringEntryProto.verify(message.updateBinding[i]);
          if (error) return 'updateBinding.' + error;
        }
      }
      return null;
    };

    /**
     * Creates a TrainingInfoProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.TrainingInfoProto} TrainingInfoProto
     */
    TrainingInfoProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.TrainingInfoProto) return object;
      var message = new $root.onnx.TrainingInfoProto();
      if (object.initialization != null) {
        if (typeof object.initialization !== 'object')
          throw TypeError('.onnx.TrainingInfoProto.initialization: object expected');
        message.initialization = $root.onnx.GraphProto.fromObject(object.initialization);
      }
      if (object.algorithm != null) {
        if (typeof object.algorithm !== 'object') throw TypeError('.onnx.TrainingInfoProto.algorithm: object expected');
        message.algorithm = $root.onnx.GraphProto.fromObject(object.algorithm);
      }
      if (object.initializationBinding) {
        if (!Array.isArray(object.initializationBinding))
          throw TypeError('.onnx.TrainingInfoProto.initializationBinding: array expected');
        message.initializationBinding = [];
        for (var i = 0; i < object.initializationBinding.length; ++i) {
          if (typeof object.initializationBinding[i] !== 'object')
            throw TypeError('.onnx.TrainingInfoProto.initializationBinding: object expected');
          message.initializationBinding[i] = $root.onnx.StringStringEntryProto.fromObject(
            object.initializationBinding[i],
          );
        }
      }
      if (object.updateBinding) {
        if (!Array.isArray(object.updateBinding))
          throw TypeError('.onnx.TrainingInfoProto.updateBinding: array expected');
        message.updateBinding = [];
        for (var i = 0; i < object.updateBinding.length; ++i) {
          if (typeof object.updateBinding[i] !== 'object')
            throw TypeError('.onnx.TrainingInfoProto.updateBinding: object expected');
          message.updateBinding[i] = $root.onnx.StringStringEntryProto.fromObject(object.updateBinding[i]);
        }
      }
      return message;
    };

    /**
     * Creates a plain object from a TrainingInfoProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {onnx.TrainingInfoProto} message TrainingInfoProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    TrainingInfoProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.initializationBinding = [];
        object.updateBinding = [];
      }
      if (options.defaults) {
        object.initialization = null;
        object.algorithm = null;
      }
      if (message.initialization != null && message.hasOwnProperty('initialization'))
        object.initialization = $root.onnx.GraphProto.toObject(message.initialization, options);
      if (message.algorithm != null && message.hasOwnProperty('algorithm'))
        object.algorithm = $root.onnx.GraphProto.toObject(message.algorithm, options);
      if (message.initializationBinding && message.initializationBinding.length) {
        object.initializationBinding = [];
        for (var j = 0; j < message.initializationBinding.length; ++j)
          object.initializationBinding[j] = $root.onnx.StringStringEntryProto.toObject(
            message.initializationBinding[j],
            options,
          );
      }
      if (message.updateBinding && message.updateBinding.length) {
        object.updateBinding = [];
        for (var j = 0; j < message.updateBinding.length; ++j)
          object.updateBinding[j] = $root.onnx.StringStringEntryProto.toObject(message.updateBinding[j], options);
      }
      return object;
    };

    /**
     * Converts this TrainingInfoProto to JSON.
     * @function toJSON
     * @memberof onnx.TrainingInfoProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    TrainingInfoProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for TrainingInfoProto
     * @function getTypeUrl
     * @memberof onnx.TrainingInfoProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    TrainingInfoProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.TrainingInfoProto';
    };

    return TrainingInfoProto;
  })();

  onnx.ModelProto = (function () {
    /**
     * Properties of a ModelProto.
     * @memberof onnx
     * @interface IModelProto
     * @property {number|Long|null} [irVersion] ModelProto irVersion
     * @property {Array.<onnx.IOperatorSetIdProto>|null} [opsetImport] ModelProto opsetImport
     * @property {string|null} [producerName] ModelProto producerName
     * @property {string|null} [producerVersion] ModelProto producerVersion
     * @property {string|null} [domain] ModelProto domain
     * @property {number|Long|null} [modelVersion] ModelProto modelVersion
     * @property {string|null} [docString] ModelProto docString
     * @property {onnx.IGraphProto|null} [graph] ModelProto graph
     * @property {Array.<onnx.IStringStringEntryProto>|null} [metadataProps] ModelProto metadataProps
     * @property {Array.<onnx.ITrainingInfoProto>|null} [trainingInfo] ModelProto trainingInfo
     * @property {Array.<onnx.IFunctionProto>|null} [functions] ModelProto functions
     */

    /**
     * Constructs a new ModelProto.
     * @memberof onnx
     * @classdesc Represents a ModelProto.
     * @implements IModelProto
     * @constructor
     * @param {onnx.IModelProto=} [properties] Properties to set
     */
    function ModelProto(properties) {
      this.opsetImport = [];
      this.metadataProps = [];
      this.trainingInfo = [];
      this.functions = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * ModelProto irVersion.
     * @member {number|Long} irVersion
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.irVersion = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

    /**
     * ModelProto opsetImport.
     * @member {Array.<onnx.IOperatorSetIdProto>} opsetImport
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.opsetImport = $util.emptyArray;

    /**
     * ModelProto producerName.
     * @member {string} producerName
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.producerName = '';

    /**
     * ModelProto producerVersion.
     * @member {string} producerVersion
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.producerVersion = '';

    /**
     * ModelProto domain.
     * @member {string} domain
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.domain = '';

    /**
     * ModelProto modelVersion.
     * @member {number|Long} modelVersion
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.modelVersion = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

    /**
     * ModelProto docString.
     * @member {string} docString
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.docString = '';

    /**
     * ModelProto graph.
     * @member {onnx.IGraphProto|null|undefined} graph
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.graph = null;

    /**
     * ModelProto metadataProps.
     * @member {Array.<onnx.IStringStringEntryProto>} metadataProps
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.metadataProps = $util.emptyArray;

    /**
     * ModelProto trainingInfo.
     * @member {Array.<onnx.ITrainingInfoProto>} trainingInfo
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.trainingInfo = $util.emptyArray;

    /**
     * ModelProto functions.
     * @member {Array.<onnx.IFunctionProto>} functions
     * @memberof onnx.ModelProto
     * @instance
     */
    ModelProto.prototype.functions = $util.emptyArray;

    /**
     * Creates a new ModelProto instance using the specified properties.
     * @function create
     * @memberof onnx.ModelProto
     * @static
     * @param {onnx.IModelProto=} [properties] Properties to set
     * @returns {onnx.ModelProto} ModelProto instance
     */
    ModelProto.create = function create(properties) {
      return new ModelProto(properties);
    };

    /**
     * Encodes the specified ModelProto message. Does not implicitly {@link onnx.ModelProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.ModelProto
     * @static
     * @param {onnx.IModelProto} message ModelProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ModelProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.irVersion != null && Object.hasOwnProperty.call(message, 'irVersion'))
        writer.uint32(/* id 1, wireType 0 =*/ 8).int64(message.irVersion);
      if (message.producerName != null && Object.hasOwnProperty.call(message, 'producerName'))
        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.producerName);
      if (message.producerVersion != null && Object.hasOwnProperty.call(message, 'producerVersion'))
        writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.producerVersion);
      if (message.domain != null && Object.hasOwnProperty.call(message, 'domain'))
        writer.uint32(/* id 4, wireType 2 =*/ 34).string(message.domain);
      if (message.modelVersion != null && Object.hasOwnProperty.call(message, 'modelVersion'))
        writer.uint32(/* id 5, wireType 0 =*/ 40).int64(message.modelVersion);
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.docString);
      if (message.graph != null && Object.hasOwnProperty.call(message, 'graph'))
        $root.onnx.GraphProto.encode(message.graph, writer.uint32(/* id 7, wireType 2 =*/ 58).fork()).ldelim();
      if (message.opsetImport != null && message.opsetImport.length)
        for (var i = 0; i < message.opsetImport.length; ++i)
          $root.onnx.OperatorSetIdProto.encode(
            message.opsetImport[i],
            writer.uint32(/* id 8, wireType 2 =*/ 66).fork(),
          ).ldelim();
      if (message.metadataProps != null && message.metadataProps.length)
        for (var i = 0; i < message.metadataProps.length; ++i)
          $root.onnx.StringStringEntryProto.encode(
            message.metadataProps[i],
            writer.uint32(/* id 14, wireType 2 =*/ 114).fork(),
          ).ldelim();
      if (message.trainingInfo != null && message.trainingInfo.length)
        for (var i = 0; i < message.trainingInfo.length; ++i)
          $root.onnx.TrainingInfoProto.encode(
            message.trainingInfo[i],
            writer.uint32(/* id 20, wireType 2 =*/ 162).fork(),
          ).ldelim();
      if (message.functions != null && message.functions.length)
        for (var i = 0; i < message.functions.length; ++i)
          $root.onnx.FunctionProto.encode(
            message.functions[i],
            writer.uint32(/* id 25, wireType 2 =*/ 202).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified ModelProto message, length delimited. Does not implicitly {@link onnx.ModelProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.ModelProto
     * @static
     * @param {onnx.IModelProto} message ModelProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ModelProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a ModelProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.ModelProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.ModelProto} ModelProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ModelProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.ModelProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.irVersion = reader.int64();
            break;
          }
          case 8: {
            if (!(message.opsetImport && message.opsetImport.length)) message.opsetImport = [];
            message.opsetImport.push($root.onnx.OperatorSetIdProto.decode(reader, reader.uint32()));
            break;
          }
          case 2: {
            message.producerName = reader.string();
            break;
          }
          case 3: {
            message.producerVersion = reader.string();
            break;
          }
          case 4: {
            message.domain = reader.string();
            break;
          }
          case 5: {
            message.modelVersion = reader.int64();
            break;
          }
          case 6: {
            message.docString = reader.string();
            break;
          }
          case 7: {
            message.graph = $root.onnx.GraphProto.decode(reader, reader.uint32());
            break;
          }
          case 14: {
            if (!(message.metadataProps && message.metadataProps.length)) message.metadataProps = [];
            message.metadataProps.push($root.onnx.StringStringEntryProto.decode(reader, reader.uint32()));
            break;
          }
          case 20: {
            if (!(message.trainingInfo && message.trainingInfo.length)) message.trainingInfo = [];
            message.trainingInfo.push($root.onnx.TrainingInfoProto.decode(reader, reader.uint32()));
            break;
          }
          case 25: {
            if (!(message.functions && message.functions.length)) message.functions = [];
            message.functions.push($root.onnx.FunctionProto.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a ModelProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.ModelProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.ModelProto} ModelProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ModelProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a ModelProto message.
     * @function verify
     * @memberof onnx.ModelProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ModelProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.irVersion != null && message.hasOwnProperty('irVersion'))
        if (
          !$util.isInteger(message.irVersion) &&
          !(message.irVersion && $util.isInteger(message.irVersion.low) && $util.isInteger(message.irVersion.high))
        )
          return 'irVersion: integer|Long expected';
      if (message.opsetImport != null && message.hasOwnProperty('opsetImport')) {
        if (!Array.isArray(message.opsetImport)) return 'opsetImport: array expected';
        for (var i = 0; i < message.opsetImport.length; ++i) {
          var error = $root.onnx.OperatorSetIdProto.verify(message.opsetImport[i]);
          if (error) return 'opsetImport.' + error;
        }
      }
      if (message.producerName != null && message.hasOwnProperty('producerName'))
        if (!$util.isString(message.producerName)) return 'producerName: string expected';
      if (message.producerVersion != null && message.hasOwnProperty('producerVersion'))
        if (!$util.isString(message.producerVersion)) return 'producerVersion: string expected';
      if (message.domain != null && message.hasOwnProperty('domain'))
        if (!$util.isString(message.domain)) return 'domain: string expected';
      if (message.modelVersion != null && message.hasOwnProperty('modelVersion'))
        if (
          !$util.isInteger(message.modelVersion) &&
          !(
            message.modelVersion &&
            $util.isInteger(message.modelVersion.low) &&
            $util.isInteger(message.modelVersion.high)
          )
        )
          return 'modelVersion: integer|Long expected';
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      if (message.graph != null && message.hasOwnProperty('graph')) {
        var error = $root.onnx.GraphProto.verify(message.graph);
        if (error) return 'graph.' + error;
      }
      if (message.metadataProps != null && message.hasOwnProperty('metadataProps')) {
        if (!Array.isArray(message.metadataProps)) return 'metadataProps: array expected';
        for (var i = 0; i < message.metadataProps.length; ++i) {
          var error = $root.onnx.StringStringEntryProto.verify(message.metadataProps[i]);
          if (error) return 'metadataProps.' + error;
        }
      }
      if (message.trainingInfo != null && message.hasOwnProperty('trainingInfo')) {
        if (!Array.isArray(message.trainingInfo)) return 'trainingInfo: array expected';
        for (var i = 0; i < message.trainingInfo.length; ++i) {
          var error = $root.onnx.TrainingInfoProto.verify(message.trainingInfo[i]);
          if (error) return 'trainingInfo.' + error;
        }
      }
      if (message.functions != null && message.hasOwnProperty('functions')) {
        if (!Array.isArray(message.functions)) return 'functions: array expected';
        for (var i = 0; i < message.functions.length; ++i) {
          var error = $root.onnx.FunctionProto.verify(message.functions[i]);
          if (error) return 'functions.' + error;
        }
      }
      return null;
    };

    /**
     * Creates a ModelProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.ModelProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.ModelProto} ModelProto
     */
    ModelProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.ModelProto) return object;
      var message = new $root.onnx.ModelProto();
      if (object.irVersion != null)
        if ($util.Long) (message.irVersion = $util.Long.fromValue(object.irVersion)).unsigned = false;
        else if (typeof object.irVersion === 'string') message.irVersion = parseInt(object.irVersion, 10);
        else if (typeof object.irVersion === 'number') message.irVersion = object.irVersion;
        else if (typeof object.irVersion === 'object')
          message.irVersion = new $util.LongBits(object.irVersion.low >>> 0, object.irVersion.high >>> 0).toNumber();
      if (object.opsetImport) {
        if (!Array.isArray(object.opsetImport)) throw TypeError('.onnx.ModelProto.opsetImport: array expected');
        message.opsetImport = [];
        for (var i = 0; i < object.opsetImport.length; ++i) {
          if (typeof object.opsetImport[i] !== 'object')
            throw TypeError('.onnx.ModelProto.opsetImport: object expected');
          message.opsetImport[i] = $root.onnx.OperatorSetIdProto.fromObject(object.opsetImport[i]);
        }
      }
      if (object.producerName != null) message.producerName = String(object.producerName);
      if (object.producerVersion != null) message.producerVersion = String(object.producerVersion);
      if (object.domain != null) message.domain = String(object.domain);
      if (object.modelVersion != null)
        if ($util.Long) (message.modelVersion = $util.Long.fromValue(object.modelVersion)).unsigned = false;
        else if (typeof object.modelVersion === 'string') message.modelVersion = parseInt(object.modelVersion, 10);
        else if (typeof object.modelVersion === 'number') message.modelVersion = object.modelVersion;
        else if (typeof object.modelVersion === 'object')
          message.modelVersion = new $util.LongBits(
            object.modelVersion.low >>> 0,
            object.modelVersion.high >>> 0,
          ).toNumber();
      if (object.docString != null) message.docString = String(object.docString);
      if (object.graph != null) {
        if (typeof object.graph !== 'object') throw TypeError('.onnx.ModelProto.graph: object expected');
        message.graph = $root.onnx.GraphProto.fromObject(object.graph);
      }
      if (object.metadataProps) {
        if (!Array.isArray(object.metadataProps)) throw TypeError('.onnx.ModelProto.metadataProps: array expected');
        message.metadataProps = [];
        for (var i = 0; i < object.metadataProps.length; ++i) {
          if (typeof object.metadataProps[i] !== 'object')
            throw TypeError('.onnx.ModelProto.metadataProps: object expected');
          message.metadataProps[i] = $root.onnx.StringStringEntryProto.fromObject(object.metadataProps[i]);
        }
      }
      if (object.trainingInfo) {
        if (!Array.isArray(object.trainingInfo)) throw TypeError('.onnx.ModelProto.trainingInfo: array expected');
        message.trainingInfo = [];
        for (var i = 0; i < object.trainingInfo.length; ++i) {
          if (typeof object.trainingInfo[i] !== 'object')
            throw TypeError('.onnx.ModelProto.trainingInfo: object expected');
          message.trainingInfo[i] = $root.onnx.TrainingInfoProto.fromObject(object.trainingInfo[i]);
        }
      }
      if (object.functions) {
        if (!Array.isArray(object.functions)) throw TypeError('.onnx.ModelProto.functions: array expected');
        message.functions = [];
        for (var i = 0; i < object.functions.length; ++i) {
          if (typeof object.functions[i] !== 'object') throw TypeError('.onnx.ModelProto.functions: object expected');
          message.functions[i] = $root.onnx.FunctionProto.fromObject(object.functions[i]);
        }
      }
      return message;
    };

    /**
     * Creates a plain object from a ModelProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.ModelProto
     * @static
     * @param {onnx.ModelProto} message ModelProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ModelProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.opsetImport = [];
        object.metadataProps = [];
        object.trainingInfo = [];
        object.functions = [];
      }
      if (options.defaults) {
        if ($util.Long) {
          var long = new $util.Long(0, 0, false);
          object.irVersion =
            options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
        } else object.irVersion = options.longs === String ? '0' : 0;
        object.producerName = '';
        object.producerVersion = '';
        object.domain = '';
        if ($util.Long) {
          var long = new $util.Long(0, 0, false);
          object.modelVersion =
            options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
        } else object.modelVersion = options.longs === String ? '0' : 0;
        object.docString = '';
        object.graph = null;
      }
      if (message.irVersion != null && message.hasOwnProperty('irVersion'))
        if (typeof message.irVersion === 'number')
          object.irVersion = options.longs === String ? String(message.irVersion) : message.irVersion;
        else
          object.irVersion =
            options.longs === String
              ? $util.Long.prototype.toString.call(message.irVersion)
              : options.longs === Number
                ? new $util.LongBits(message.irVersion.low >>> 0, message.irVersion.high >>> 0).toNumber()
                : message.irVersion;
      if (message.producerName != null && message.hasOwnProperty('producerName'))
        object.producerName = message.producerName;
      if (message.producerVersion != null && message.hasOwnProperty('producerVersion'))
        object.producerVersion = message.producerVersion;
      if (message.domain != null && message.hasOwnProperty('domain')) object.domain = message.domain;
      if (message.modelVersion != null && message.hasOwnProperty('modelVersion'))
        if (typeof message.modelVersion === 'number')
          object.modelVersion = options.longs === String ? String(message.modelVersion) : message.modelVersion;
        else
          object.modelVersion =
            options.longs === String
              ? $util.Long.prototype.toString.call(message.modelVersion)
              : options.longs === Number
                ? new $util.LongBits(message.modelVersion.low >>> 0, message.modelVersion.high >>> 0).toNumber()
                : message.modelVersion;
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.graph != null && message.hasOwnProperty('graph'))
        object.graph = $root.onnx.GraphProto.toObject(message.graph, options);
      if (message.opsetImport && message.opsetImport.length) {
        object.opsetImport = [];
        for (var j = 0; j < message.opsetImport.length; ++j)
          object.opsetImport[j] = $root.onnx.OperatorSetIdProto.toObject(message.opsetImport[j], options);
      }
      if (message.metadataProps && message.metadataProps.length) {
        object.metadataProps = [];
        for (var j = 0; j < message.metadataProps.length; ++j)
          object.metadataProps[j] = $root.onnx.StringStringEntryProto.toObject(message.metadataProps[j], options);
      }
      if (message.trainingInfo && message.trainingInfo.length) {
        object.trainingInfo = [];
        for (var j = 0; j < message.trainingInfo.length; ++j)
          object.trainingInfo[j] = $root.onnx.TrainingInfoProto.toObject(message.trainingInfo[j], options);
      }
      if (message.functions && message.functions.length) {
        object.functions = [];
        for (var j = 0; j < message.functions.length; ++j)
          object.functions[j] = $root.onnx.FunctionProto.toObject(message.functions[j], options);
      }
      return object;
    };

    /**
     * Converts this ModelProto to JSON.
     * @function toJSON
     * @memberof onnx.ModelProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ModelProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for ModelProto
     * @function getTypeUrl
     * @memberof onnx.ModelProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    ModelProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.ModelProto';
    };

    return ModelProto;
  })();

  onnx.StringStringEntryProto = (function () {
    /**
     * Properties of a StringStringEntryProto.
     * @memberof onnx
     * @interface IStringStringEntryProto
     * @property {string|null} [key] StringStringEntryProto key
     * @property {string|null} [value] StringStringEntryProto value
     */

    /**
     * Constructs a new StringStringEntryProto.
     * @memberof onnx
     * @classdesc Represents a StringStringEntryProto.
     * @implements IStringStringEntryProto
     * @constructor
     * @param {onnx.IStringStringEntryProto=} [properties] Properties to set
     */
    function StringStringEntryProto(properties) {
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * StringStringEntryProto key.
     * @member {string} key
     * @memberof onnx.StringStringEntryProto
     * @instance
     */
    StringStringEntryProto.prototype.key = '';

    /**
     * StringStringEntryProto value.
     * @member {string} value
     * @memberof onnx.StringStringEntryProto
     * @instance
     */
    StringStringEntryProto.prototype.value = '';

    /**
     * Creates a new StringStringEntryProto instance using the specified properties.
     * @function create
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {onnx.IStringStringEntryProto=} [properties] Properties to set
     * @returns {onnx.StringStringEntryProto} StringStringEntryProto instance
     */
    StringStringEntryProto.create = function create(properties) {
      return new StringStringEntryProto(properties);
    };

    /**
     * Encodes the specified StringStringEntryProto message. Does not implicitly {@link onnx.StringStringEntryProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {onnx.IStringStringEntryProto} message StringStringEntryProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    StringStringEntryProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.key != null && Object.hasOwnProperty.call(message, 'key'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.key);
      if (message.value != null && Object.hasOwnProperty.call(message, 'value'))
        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.value);
      return writer;
    };

    /**
     * Encodes the specified StringStringEntryProto message, length delimited. Does not implicitly {@link onnx.StringStringEntryProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {onnx.IStringStringEntryProto} message StringStringEntryProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    StringStringEntryProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a StringStringEntryProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.StringStringEntryProto} StringStringEntryProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    StringStringEntryProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.StringStringEntryProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.key = reader.string();
            break;
          }
          case 2: {
            message.value = reader.string();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a StringStringEntryProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.StringStringEntryProto} StringStringEntryProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    StringStringEntryProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a StringStringEntryProto message.
     * @function verify
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    StringStringEntryProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.key != null && message.hasOwnProperty('key'))
        if (!$util.isString(message.key)) return 'key: string expected';
      if (message.value != null && message.hasOwnProperty('value'))
        if (!$util.isString(message.value)) return 'value: string expected';
      return null;
    };

    /**
     * Creates a StringStringEntryProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.StringStringEntryProto} StringStringEntryProto
     */
    StringStringEntryProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.StringStringEntryProto) return object;
      var message = new $root.onnx.StringStringEntryProto();
      if (object.key != null) message.key = String(object.key);
      if (object.value != null) message.value = String(object.value);
      return message;
    };

    /**
     * Creates a plain object from a StringStringEntryProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {onnx.StringStringEntryProto} message StringStringEntryProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    StringStringEntryProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.defaults) {
        object.key = '';
        object.value = '';
      }
      if (message.key != null && message.hasOwnProperty('key')) object.key = message.key;
      if (message.value != null && message.hasOwnProperty('value')) object.value = message.value;
      return object;
    };

    /**
     * Converts this StringStringEntryProto to JSON.
     * @function toJSON
     * @memberof onnx.StringStringEntryProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    StringStringEntryProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for StringStringEntryProto
     * @function getTypeUrl
     * @memberof onnx.StringStringEntryProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    StringStringEntryProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.StringStringEntryProto';
    };

    return StringStringEntryProto;
  })();

  onnx.TensorAnnotation = (function () {
    /**
     * Properties of a TensorAnnotation.
     * @memberof onnx
     * @interface ITensorAnnotation
     * @property {string|null} [tensorName] TensorAnnotation tensorName
     * @property {Array.<onnx.IStringStringEntryProto>|null} [quantParameterTensorNames] TensorAnnotation quantParameterTensorNames
     */

    /**
     * Constructs a new TensorAnnotation.
     * @memberof onnx
     * @classdesc Represents a TensorAnnotation.
     * @implements ITensorAnnotation
     * @constructor
     * @param {onnx.ITensorAnnotation=} [properties] Properties to set
     */
    function TensorAnnotation(properties) {
      this.quantParameterTensorNames = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * TensorAnnotation tensorName.
     * @member {string} tensorName
     * @memberof onnx.TensorAnnotation
     * @instance
     */
    TensorAnnotation.prototype.tensorName = '';

    /**
     * TensorAnnotation quantParameterTensorNames.
     * @member {Array.<onnx.IStringStringEntryProto>} quantParameterTensorNames
     * @memberof onnx.TensorAnnotation
     * @instance
     */
    TensorAnnotation.prototype.quantParameterTensorNames = $util.emptyArray;

    /**
     * Creates a new TensorAnnotation instance using the specified properties.
     * @function create
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {onnx.ITensorAnnotation=} [properties] Properties to set
     * @returns {onnx.TensorAnnotation} TensorAnnotation instance
     */
    TensorAnnotation.create = function create(properties) {
      return new TensorAnnotation(properties);
    };

    /**
     * Encodes the specified TensorAnnotation message. Does not implicitly {@link onnx.TensorAnnotation.verify|verify} messages.
     * @function encode
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {onnx.ITensorAnnotation} message TensorAnnotation message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorAnnotation.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.tensorName != null && Object.hasOwnProperty.call(message, 'tensorName'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.tensorName);
      if (message.quantParameterTensorNames != null && message.quantParameterTensorNames.length)
        for (var i = 0; i < message.quantParameterTensorNames.length; ++i)
          $root.onnx.StringStringEntryProto.encode(
            message.quantParameterTensorNames[i],
            writer.uint32(/* id 2, wireType 2 =*/ 18).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified TensorAnnotation message, length delimited. Does not implicitly {@link onnx.TensorAnnotation.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {onnx.ITensorAnnotation} message TensorAnnotation message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorAnnotation.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a TensorAnnotation message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.TensorAnnotation} TensorAnnotation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorAnnotation.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.TensorAnnotation();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.tensorName = reader.string();
            break;
          }
          case 2: {
            if (!(message.quantParameterTensorNames && message.quantParameterTensorNames.length))
              message.quantParameterTensorNames = [];
            message.quantParameterTensorNames.push($root.onnx.StringStringEntryProto.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a TensorAnnotation message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.TensorAnnotation} TensorAnnotation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorAnnotation.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a TensorAnnotation message.
     * @function verify
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    TensorAnnotation.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.tensorName != null && message.hasOwnProperty('tensorName'))
        if (!$util.isString(message.tensorName)) return 'tensorName: string expected';
      if (message.quantParameterTensorNames != null && message.hasOwnProperty('quantParameterTensorNames')) {
        if (!Array.isArray(message.quantParameterTensorNames)) return 'quantParameterTensorNames: array expected';
        for (var i = 0; i < message.quantParameterTensorNames.length; ++i) {
          var error = $root.onnx.StringStringEntryProto.verify(message.quantParameterTensorNames[i]);
          if (error) return 'quantParameterTensorNames.' + error;
        }
      }
      return null;
    };

    /**
     * Creates a TensorAnnotation message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.TensorAnnotation} TensorAnnotation
     */
    TensorAnnotation.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.TensorAnnotation) return object;
      var message = new $root.onnx.TensorAnnotation();
      if (object.tensorName != null) message.tensorName = String(object.tensorName);
      if (object.quantParameterTensorNames) {
        if (!Array.isArray(object.quantParameterTensorNames))
          throw TypeError('.onnx.TensorAnnotation.quantParameterTensorNames: array expected');
        message.quantParameterTensorNames = [];
        for (var i = 0; i < object.quantParameterTensorNames.length; ++i) {
          if (typeof object.quantParameterTensorNames[i] !== 'object')
            throw TypeError('.onnx.TensorAnnotation.quantParameterTensorNames: object expected');
          message.quantParameterTensorNames[i] = $root.onnx.StringStringEntryProto.fromObject(
            object.quantParameterTensorNames[i],
          );
        }
      }
      return message;
    };

    /**
     * Creates a plain object from a TensorAnnotation message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {onnx.TensorAnnotation} message TensorAnnotation
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    TensorAnnotation.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) object.quantParameterTensorNames = [];
      if (options.defaults) object.tensorName = '';
      if (message.tensorName != null && message.hasOwnProperty('tensorName')) object.tensorName = message.tensorName;
      if (message.quantParameterTensorNames && message.quantParameterTensorNames.length) {
        object.quantParameterTensorNames = [];
        for (var j = 0; j < message.quantParameterTensorNames.length; ++j)
          object.quantParameterTensorNames[j] = $root.onnx.StringStringEntryProto.toObject(
            message.quantParameterTensorNames[j],
            options,
          );
      }
      return object;
    };

    /**
     * Converts this TensorAnnotation to JSON.
     * @function toJSON
     * @memberof onnx.TensorAnnotation
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    TensorAnnotation.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for TensorAnnotation
     * @function getTypeUrl
     * @memberof onnx.TensorAnnotation
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    TensorAnnotation.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.TensorAnnotation';
    };

    return TensorAnnotation;
  })();

  onnx.GraphProto = (function () {
    /**
     * Properties of a GraphProto.
     * @memberof onnx
     * @interface IGraphProto
     * @property {Array.<onnx.INodeProto>|null} [node] GraphProto node
     * @property {string|null} [name] GraphProto name
     * @property {Array.<onnx.ITensorProto>|null} [initializer] GraphProto initializer
     * @property {Array.<onnx.ISparseTensorProto>|null} [sparseInitializer] GraphProto sparseInitializer
     * @property {string|null} [docString] GraphProto docString
     * @property {Array.<onnx.IValueInfoProto>|null} [input] GraphProto input
     * @property {Array.<onnx.IValueInfoProto>|null} [output] GraphProto output
     * @property {Array.<onnx.IValueInfoProto>|null} [valueInfo] GraphProto valueInfo
     * @property {Array.<onnx.ITensorAnnotation>|null} [quantizationAnnotation] GraphProto quantizationAnnotation
     */

    /**
     * Constructs a new GraphProto.
     * @memberof onnx
     * @classdesc Represents a GraphProto.
     * @implements IGraphProto
     * @constructor
     * @param {onnx.IGraphProto=} [properties] Properties to set
     */
    function GraphProto(properties) {
      this.node = [];
      this.initializer = [];
      this.sparseInitializer = [];
      this.input = [];
      this.output = [];
      this.valueInfo = [];
      this.quantizationAnnotation = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * GraphProto node.
     * @member {Array.<onnx.INodeProto>} node
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.node = $util.emptyArray;

    /**
     * GraphProto name.
     * @member {string} name
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.name = '';

    /**
     * GraphProto initializer.
     * @member {Array.<onnx.ITensorProto>} initializer
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.initializer = $util.emptyArray;

    /**
     * GraphProto sparseInitializer.
     * @member {Array.<onnx.ISparseTensorProto>} sparseInitializer
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.sparseInitializer = $util.emptyArray;

    /**
     * GraphProto docString.
     * @member {string} docString
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.docString = '';

    /**
     * GraphProto input.
     * @member {Array.<onnx.IValueInfoProto>} input
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.input = $util.emptyArray;

    /**
     * GraphProto output.
     * @member {Array.<onnx.IValueInfoProto>} output
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.output = $util.emptyArray;

    /**
     * GraphProto valueInfo.
     * @member {Array.<onnx.IValueInfoProto>} valueInfo
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.valueInfo = $util.emptyArray;

    /**
     * GraphProto quantizationAnnotation.
     * @member {Array.<onnx.ITensorAnnotation>} quantizationAnnotation
     * @memberof onnx.GraphProto
     * @instance
     */
    GraphProto.prototype.quantizationAnnotation = $util.emptyArray;

    /**
     * Creates a new GraphProto instance using the specified properties.
     * @function create
     * @memberof onnx.GraphProto
     * @static
     * @param {onnx.IGraphProto=} [properties] Properties to set
     * @returns {onnx.GraphProto} GraphProto instance
     */
    GraphProto.create = function create(properties) {
      return new GraphProto(properties);
    };

    /**
     * Encodes the specified GraphProto message. Does not implicitly {@link onnx.GraphProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.GraphProto
     * @static
     * @param {onnx.IGraphProto} message GraphProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    GraphProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.node != null && message.node.length)
        for (var i = 0; i < message.node.length; ++i)
          $root.onnx.NodeProto.encode(message.node[i], writer.uint32(/* id 1, wireType 2 =*/ 10).fork()).ldelim();
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.name);
      if (message.initializer != null && message.initializer.length)
        for (var i = 0; i < message.initializer.length; ++i)
          $root.onnx.TensorProto.encode(
            message.initializer[i],
            writer.uint32(/* id 5, wireType 2 =*/ 42).fork(),
          ).ldelim();
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 10, wireType 2 =*/ 82).string(message.docString);
      if (message.input != null && message.input.length)
        for (var i = 0; i < message.input.length; ++i)
          $root.onnx.ValueInfoProto.encode(
            message.input[i],
            writer.uint32(/* id 11, wireType 2 =*/ 90).fork(),
          ).ldelim();
      if (message.output != null && message.output.length)
        for (var i = 0; i < message.output.length; ++i)
          $root.onnx.ValueInfoProto.encode(
            message.output[i],
            writer.uint32(/* id 12, wireType 2 =*/ 98).fork(),
          ).ldelim();
      if (message.valueInfo != null && message.valueInfo.length)
        for (var i = 0; i < message.valueInfo.length; ++i)
          $root.onnx.ValueInfoProto.encode(
            message.valueInfo[i],
            writer.uint32(/* id 13, wireType 2 =*/ 106).fork(),
          ).ldelim();
      if (message.quantizationAnnotation != null && message.quantizationAnnotation.length)
        for (var i = 0; i < message.quantizationAnnotation.length; ++i)
          $root.onnx.TensorAnnotation.encode(
            message.quantizationAnnotation[i],
            writer.uint32(/* id 14, wireType 2 =*/ 114).fork(),
          ).ldelim();
      if (message.sparseInitializer != null && message.sparseInitializer.length)
        for (var i = 0; i < message.sparseInitializer.length; ++i)
          $root.onnx.SparseTensorProto.encode(
            message.sparseInitializer[i],
            writer.uint32(/* id 15, wireType 2 =*/ 122).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified GraphProto message, length delimited. Does not implicitly {@link onnx.GraphProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.GraphProto
     * @static
     * @param {onnx.IGraphProto} message GraphProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    GraphProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a GraphProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.GraphProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.GraphProto} GraphProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    GraphProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.GraphProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            if (!(message.node && message.node.length)) message.node = [];
            message.node.push($root.onnx.NodeProto.decode(reader, reader.uint32()));
            break;
          }
          case 2: {
            message.name = reader.string();
            break;
          }
          case 5: {
            if (!(message.initializer && message.initializer.length)) message.initializer = [];
            message.initializer.push($root.onnx.TensorProto.decode(reader, reader.uint32()));
            break;
          }
          case 15: {
            if (!(message.sparseInitializer && message.sparseInitializer.length)) message.sparseInitializer = [];
            message.sparseInitializer.push($root.onnx.SparseTensorProto.decode(reader, reader.uint32()));
            break;
          }
          case 10: {
            message.docString = reader.string();
            break;
          }
          case 11: {
            if (!(message.input && message.input.length)) message.input = [];
            message.input.push($root.onnx.ValueInfoProto.decode(reader, reader.uint32()));
            break;
          }
          case 12: {
            if (!(message.output && message.output.length)) message.output = [];
            message.output.push($root.onnx.ValueInfoProto.decode(reader, reader.uint32()));
            break;
          }
          case 13: {
            if (!(message.valueInfo && message.valueInfo.length)) message.valueInfo = [];
            message.valueInfo.push($root.onnx.ValueInfoProto.decode(reader, reader.uint32()));
            break;
          }
          case 14: {
            if (!(message.quantizationAnnotation && message.quantizationAnnotation.length))
              message.quantizationAnnotation = [];
            message.quantizationAnnotation.push($root.onnx.TensorAnnotation.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a GraphProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.GraphProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.GraphProto} GraphProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    GraphProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a GraphProto message.
     * @function verify
     * @memberof onnx.GraphProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    GraphProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.node != null && message.hasOwnProperty('node')) {
        if (!Array.isArray(message.node)) return 'node: array expected';
        for (var i = 0; i < message.node.length; ++i) {
          var error = $root.onnx.NodeProto.verify(message.node[i]);
          if (error) return 'node.' + error;
        }
      }
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.initializer != null && message.hasOwnProperty('initializer')) {
        if (!Array.isArray(message.initializer)) return 'initializer: array expected';
        for (var i = 0; i < message.initializer.length; ++i) {
          var error = $root.onnx.TensorProto.verify(message.initializer[i]);
          if (error) return 'initializer.' + error;
        }
      }
      if (message.sparseInitializer != null && message.hasOwnProperty('sparseInitializer')) {
        if (!Array.isArray(message.sparseInitializer)) return 'sparseInitializer: array expected';
        for (var i = 0; i < message.sparseInitializer.length; ++i) {
          var error = $root.onnx.SparseTensorProto.verify(message.sparseInitializer[i]);
          if (error) return 'sparseInitializer.' + error;
        }
      }
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      if (message.input != null && message.hasOwnProperty('input')) {
        if (!Array.isArray(message.input)) return 'input: array expected';
        for (var i = 0; i < message.input.length; ++i) {
          var error = $root.onnx.ValueInfoProto.verify(message.input[i]);
          if (error) return 'input.' + error;
        }
      }
      if (message.output != null && message.hasOwnProperty('output')) {
        if (!Array.isArray(message.output)) return 'output: array expected';
        for (var i = 0; i < message.output.length; ++i) {
          var error = $root.onnx.ValueInfoProto.verify(message.output[i]);
          if (error) return 'output.' + error;
        }
      }
      if (message.valueInfo != null && message.hasOwnProperty('valueInfo')) {
        if (!Array.isArray(message.valueInfo)) return 'valueInfo: array expected';
        for (var i = 0; i < message.valueInfo.length; ++i) {
          var error = $root.onnx.ValueInfoProto.verify(message.valueInfo[i]);
          if (error) return 'valueInfo.' + error;
        }
      }
      if (message.quantizationAnnotation != null && message.hasOwnProperty('quantizationAnnotation')) {
        if (!Array.isArray(message.quantizationAnnotation)) return 'quantizationAnnotation: array expected';
        for (var i = 0; i < message.quantizationAnnotation.length; ++i) {
          var error = $root.onnx.TensorAnnotation.verify(message.quantizationAnnotation[i]);
          if (error) return 'quantizationAnnotation.' + error;
        }
      }
      return null;
    };

    /**
     * Creates a GraphProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.GraphProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.GraphProto} GraphProto
     */
    GraphProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.GraphProto) return object;
      var message = new $root.onnx.GraphProto();
      if (object.node) {
        if (!Array.isArray(object.node)) throw TypeError('.onnx.GraphProto.node: array expected');
        message.node = [];
        for (var i = 0; i < object.node.length; ++i) {
          if (typeof object.node[i] !== 'object') throw TypeError('.onnx.GraphProto.node: object expected');
          message.node[i] = $root.onnx.NodeProto.fromObject(object.node[i]);
        }
      }
      if (object.name != null) message.name = String(object.name);
      if (object.initializer) {
        if (!Array.isArray(object.initializer)) throw TypeError('.onnx.GraphProto.initializer: array expected');
        message.initializer = [];
        for (var i = 0; i < object.initializer.length; ++i) {
          if (typeof object.initializer[i] !== 'object')
            throw TypeError('.onnx.GraphProto.initializer: object expected');
          message.initializer[i] = $root.onnx.TensorProto.fromObject(object.initializer[i]);
        }
      }
      if (object.sparseInitializer) {
        if (!Array.isArray(object.sparseInitializer))
          throw TypeError('.onnx.GraphProto.sparseInitializer: array expected');
        message.sparseInitializer = [];
        for (var i = 0; i < object.sparseInitializer.length; ++i) {
          if (typeof object.sparseInitializer[i] !== 'object')
            throw TypeError('.onnx.GraphProto.sparseInitializer: object expected');
          message.sparseInitializer[i] = $root.onnx.SparseTensorProto.fromObject(object.sparseInitializer[i]);
        }
      }
      if (object.docString != null) message.docString = String(object.docString);
      if (object.input) {
        if (!Array.isArray(object.input)) throw TypeError('.onnx.GraphProto.input: array expected');
        message.input = [];
        for (var i = 0; i < object.input.length; ++i) {
          if (typeof object.input[i] !== 'object') throw TypeError('.onnx.GraphProto.input: object expected');
          message.input[i] = $root.onnx.ValueInfoProto.fromObject(object.input[i]);
        }
      }
      if (object.output) {
        if (!Array.isArray(object.output)) throw TypeError('.onnx.GraphProto.output: array expected');
        message.output = [];
        for (var i = 0; i < object.output.length; ++i) {
          if (typeof object.output[i] !== 'object') throw TypeError('.onnx.GraphProto.output: object expected');
          message.output[i] = $root.onnx.ValueInfoProto.fromObject(object.output[i]);
        }
      }
      if (object.valueInfo) {
        if (!Array.isArray(object.valueInfo)) throw TypeError('.onnx.GraphProto.valueInfo: array expected');
        message.valueInfo = [];
        for (var i = 0; i < object.valueInfo.length; ++i) {
          if (typeof object.valueInfo[i] !== 'object') throw TypeError('.onnx.GraphProto.valueInfo: object expected');
          message.valueInfo[i] = $root.onnx.ValueInfoProto.fromObject(object.valueInfo[i]);
        }
      }
      if (object.quantizationAnnotation) {
        if (!Array.isArray(object.quantizationAnnotation))
          throw TypeError('.onnx.GraphProto.quantizationAnnotation: array expected');
        message.quantizationAnnotation = [];
        for (var i = 0; i < object.quantizationAnnotation.length; ++i) {
          if (typeof object.quantizationAnnotation[i] !== 'object')
            throw TypeError('.onnx.GraphProto.quantizationAnnotation: object expected');
          message.quantizationAnnotation[i] = $root.onnx.TensorAnnotation.fromObject(object.quantizationAnnotation[i]);
        }
      }
      return message;
    };

    /**
     * Creates a plain object from a GraphProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.GraphProto
     * @static
     * @param {onnx.GraphProto} message GraphProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    GraphProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.node = [];
        object.initializer = [];
        object.input = [];
        object.output = [];
        object.valueInfo = [];
        object.quantizationAnnotation = [];
        object.sparseInitializer = [];
      }
      if (options.defaults) {
        object.name = '';
        object.docString = '';
      }
      if (message.node && message.node.length) {
        object.node = [];
        for (var j = 0; j < message.node.length; ++j)
          object.node[j] = $root.onnx.NodeProto.toObject(message.node[j], options);
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.initializer && message.initializer.length) {
        object.initializer = [];
        for (var j = 0; j < message.initializer.length; ++j)
          object.initializer[j] = $root.onnx.TensorProto.toObject(message.initializer[j], options);
      }
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.input && message.input.length) {
        object.input = [];
        for (var j = 0; j < message.input.length; ++j)
          object.input[j] = $root.onnx.ValueInfoProto.toObject(message.input[j], options);
      }
      if (message.output && message.output.length) {
        object.output = [];
        for (var j = 0; j < message.output.length; ++j)
          object.output[j] = $root.onnx.ValueInfoProto.toObject(message.output[j], options);
      }
      if (message.valueInfo && message.valueInfo.length) {
        object.valueInfo = [];
        for (var j = 0; j < message.valueInfo.length; ++j)
          object.valueInfo[j] = $root.onnx.ValueInfoProto.toObject(message.valueInfo[j], options);
      }
      if (message.quantizationAnnotation && message.quantizationAnnotation.length) {
        object.quantizationAnnotation = [];
        for (var j = 0; j < message.quantizationAnnotation.length; ++j)
          object.quantizationAnnotation[j] = $root.onnx.TensorAnnotation.toObject(
            message.quantizationAnnotation[j],
            options,
          );
      }
      if (message.sparseInitializer && message.sparseInitializer.length) {
        object.sparseInitializer = [];
        for (var j = 0; j < message.sparseInitializer.length; ++j)
          object.sparseInitializer[j] = $root.onnx.SparseTensorProto.toObject(message.sparseInitializer[j], options);
      }
      return object;
    };

    /**
     * Converts this GraphProto to JSON.
     * @function toJSON
     * @memberof onnx.GraphProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    GraphProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for GraphProto
     * @function getTypeUrl
     * @memberof onnx.GraphProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    GraphProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.GraphProto';
    };

    return GraphProto;
  })();

  onnx.TensorProto = (function () {
    /**
     * Properties of a TensorProto.
     * @memberof onnx
     * @interface ITensorProto
     * @property {Array.<number|Long>|null} [dims] TensorProto dims
     * @property {number|null} [dataType] TensorProto dataType
     * @property {onnx.TensorProto.ISegment|null} [segment] TensorProto segment
     * @property {Array.<number>|null} [floatData] TensorProto floatData
     * @property {Array.<number>|null} [int32Data] TensorProto int32Data
     * @property {Array.<Uint8Array>|null} [stringData] TensorProto stringData
     * @property {Array.<number|Long>|null} [int64Data] TensorProto int64Data
     * @property {string|null} [name] TensorProto name
     * @property {string|null} [docString] TensorProto docString
     * @property {Uint8Array|null} [rawData] TensorProto rawData
     * @property {Array.<onnx.IStringStringEntryProto>|null} [externalData] TensorProto externalData
     * @property {onnx.TensorProto.DataLocation|null} [dataLocation] TensorProto dataLocation
     * @property {Array.<number>|null} [doubleData] TensorProto doubleData
     * @property {Array.<number|Long>|null} [uint64Data] TensorProto uint64Data
     */

    /**
     * Constructs a new TensorProto.
     * @memberof onnx
     * @classdesc Represents a TensorProto.
     * @implements ITensorProto
     * @constructor
     * @param {onnx.ITensorProto=} [properties] Properties to set
     */
    function TensorProto(properties) {
      this.dims = [];
      this.floatData = [];
      this.int32Data = [];
      this.stringData = [];
      this.int64Data = [];
      this.externalData = [];
      this.doubleData = [];
      this.uint64Data = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * TensorProto dims.
     * @member {Array.<number|Long>} dims
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.dims = $util.emptyArray;

    /**
     * TensorProto dataType.
     * @member {number} dataType
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.dataType = 0;

    /**
     * TensorProto segment.
     * @member {onnx.TensorProto.ISegment|null|undefined} segment
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.segment = null;

    /**
     * TensorProto floatData.
     * @member {Array.<number>} floatData
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.floatData = $util.emptyArray;

    /**
     * TensorProto int32Data.
     * @member {Array.<number>} int32Data
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.int32Data = $util.emptyArray;

    /**
     * TensorProto stringData.
     * @member {Array.<Uint8Array>} stringData
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.stringData = $util.emptyArray;

    /**
     * TensorProto int64Data.
     * @member {Array.<number|Long>} int64Data
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.int64Data = $util.emptyArray;

    /**
     * TensorProto name.
     * @member {string} name
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.name = '';

    /**
     * TensorProto docString.
     * @member {string} docString
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.docString = '';

    /**
     * TensorProto rawData.
     * @member {Uint8Array} rawData
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.rawData = $util.newBuffer([]);

    /**
     * TensorProto externalData.
     * @member {Array.<onnx.IStringStringEntryProto>} externalData
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.externalData = $util.emptyArray;

    /**
     * TensorProto dataLocation.
     * @member {onnx.TensorProto.DataLocation} dataLocation
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.dataLocation = 0;

    /**
     * TensorProto doubleData.
     * @member {Array.<number>} doubleData
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.doubleData = $util.emptyArray;

    /**
     * TensorProto uint64Data.
     * @member {Array.<number|Long>} uint64Data
     * @memberof onnx.TensorProto
     * @instance
     */
    TensorProto.prototype.uint64Data = $util.emptyArray;

    /**
     * Creates a new TensorProto instance using the specified properties.
     * @function create
     * @memberof onnx.TensorProto
     * @static
     * @param {onnx.ITensorProto=} [properties] Properties to set
     * @returns {onnx.TensorProto} TensorProto instance
     */
    TensorProto.create = function create(properties) {
      return new TensorProto(properties);
    };

    /**
     * Encodes the specified TensorProto message. Does not implicitly {@link onnx.TensorProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.TensorProto
     * @static
     * @param {onnx.ITensorProto} message TensorProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.dims != null && message.dims.length) {
        writer.uint32(/* id 1, wireType 2 =*/ 10).fork();
        for (var i = 0; i < message.dims.length; ++i) writer.int64(message.dims[i]);
        writer.ldelim();
      }
      if (message.dataType != null && Object.hasOwnProperty.call(message, 'dataType'))
        writer.uint32(/* id 2, wireType 0 =*/ 16).int32(message.dataType);
      if (message.segment != null && Object.hasOwnProperty.call(message, 'segment'))
        $root.onnx.TensorProto.Segment.encode(
          message.segment,
          writer.uint32(/* id 3, wireType 2 =*/ 26).fork(),
        ).ldelim();
      if (message.floatData != null && message.floatData.length) {
        writer.uint32(/* id 4, wireType 2 =*/ 34).fork();
        for (var i = 0; i < message.floatData.length; ++i) writer.float(message.floatData[i]);
        writer.ldelim();
      }
      if (message.int32Data != null && message.int32Data.length) {
        writer.uint32(/* id 5, wireType 2 =*/ 42).fork();
        for (var i = 0; i < message.int32Data.length; ++i) writer.int32(message.int32Data[i]);
        writer.ldelim();
      }
      if (message.stringData != null && message.stringData.length)
        for (var i = 0; i < message.stringData.length; ++i)
          writer.uint32(/* id 6, wireType 2 =*/ 50).bytes(message.stringData[i]);
      if (message.int64Data != null && message.int64Data.length) {
        writer.uint32(/* id 7, wireType 2 =*/ 58).fork();
        for (var i = 0; i < message.int64Data.length; ++i) writer.int64(message.int64Data[i]);
        writer.ldelim();
      }
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 8, wireType 2 =*/ 66).string(message.name);
      if (message.rawData != null && Object.hasOwnProperty.call(message, 'rawData'))
        writer.uint32(/* id 9, wireType 2 =*/ 74).bytes(message.rawData);
      if (message.doubleData != null && message.doubleData.length) {
        writer.uint32(/* id 10, wireType 2 =*/ 82).fork();
        for (var i = 0; i < message.doubleData.length; ++i) writer.double(message.doubleData[i]);
        writer.ldelim();
      }
      if (message.uint64Data != null && message.uint64Data.length) {
        writer.uint32(/* id 11, wireType 2 =*/ 90).fork();
        for (var i = 0; i < message.uint64Data.length; ++i) writer.uint64(message.uint64Data[i]);
        writer.ldelim();
      }
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 12, wireType 2 =*/ 98).string(message.docString);
      if (message.externalData != null && message.externalData.length)
        for (var i = 0; i < message.externalData.length; ++i)
          $root.onnx.StringStringEntryProto.encode(
            message.externalData[i],
            writer.uint32(/* id 13, wireType 2 =*/ 106).fork(),
          ).ldelim();
      if (message.dataLocation != null && Object.hasOwnProperty.call(message, 'dataLocation'))
        writer.uint32(/* id 14, wireType 0 =*/ 112).int32(message.dataLocation);
      return writer;
    };

    /**
     * Encodes the specified TensorProto message, length delimited. Does not implicitly {@link onnx.TensorProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.TensorProto
     * @static
     * @param {onnx.ITensorProto} message TensorProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a TensorProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.TensorProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.TensorProto} TensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.TensorProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            if (!(message.dims && message.dims.length)) message.dims = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.dims.push(reader.int64());
            } else message.dims.push(reader.int64());
            break;
          }
          case 2: {
            message.dataType = reader.int32();
            break;
          }
          case 3: {
            message.segment = $root.onnx.TensorProto.Segment.decode(reader, reader.uint32());
            break;
          }
          case 4: {
            if (!(message.floatData && message.floatData.length)) message.floatData = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.floatData.push(reader.float());
            } else message.floatData.push(reader.float());
            break;
          }
          case 5: {
            if (!(message.int32Data && message.int32Data.length)) message.int32Data = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.int32Data.push(reader.int32());
            } else message.int32Data.push(reader.int32());
            break;
          }
          case 6: {
            if (!(message.stringData && message.stringData.length)) message.stringData = [];
            message.stringData.push(reader.bytes());
            break;
          }
          case 7: {
            if (!(message.int64Data && message.int64Data.length)) message.int64Data = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.int64Data.push(reader.int64());
            } else message.int64Data.push(reader.int64());
            break;
          }
          case 8: {
            message.name = reader.string();
            break;
          }
          case 12: {
            message.docString = reader.string();
            break;
          }
          case 9: {
            message.rawData = reader.bytes();
            break;
          }
          case 13: {
            if (!(message.externalData && message.externalData.length)) message.externalData = [];
            message.externalData.push($root.onnx.StringStringEntryProto.decode(reader, reader.uint32()));
            break;
          }
          case 14: {
            message.dataLocation = reader.int32();
            break;
          }
          case 10: {
            if (!(message.doubleData && message.doubleData.length)) message.doubleData = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.doubleData.push(reader.double());
            } else message.doubleData.push(reader.double());
            break;
          }
          case 11: {
            if (!(message.uint64Data && message.uint64Data.length)) message.uint64Data = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.uint64Data.push(reader.uint64());
            } else message.uint64Data.push(reader.uint64());
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a TensorProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.TensorProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.TensorProto} TensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a TensorProto message.
     * @function verify
     * @memberof onnx.TensorProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    TensorProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.dims != null && message.hasOwnProperty('dims')) {
        if (!Array.isArray(message.dims)) return 'dims: array expected';
        for (var i = 0; i < message.dims.length; ++i)
          if (
            !$util.isInteger(message.dims[i]) &&
            !(message.dims[i] && $util.isInteger(message.dims[i].low) && $util.isInteger(message.dims[i].high))
          )
            return 'dims: integer|Long[] expected';
      }
      if (message.dataType != null && message.hasOwnProperty('dataType'))
        if (!$util.isInteger(message.dataType)) return 'dataType: integer expected';
      if (message.segment != null && message.hasOwnProperty('segment')) {
        var error = $root.onnx.TensorProto.Segment.verify(message.segment);
        if (error) return 'segment.' + error;
      }
      if (message.floatData != null && message.hasOwnProperty('floatData')) {
        if (!Array.isArray(message.floatData)) return 'floatData: array expected';
        for (var i = 0; i < message.floatData.length; ++i)
          if (typeof message.floatData[i] !== 'number') return 'floatData: number[] expected';
      }
      if (message.int32Data != null && message.hasOwnProperty('int32Data')) {
        if (!Array.isArray(message.int32Data)) return 'int32Data: array expected';
        for (var i = 0; i < message.int32Data.length; ++i)
          if (!$util.isInteger(message.int32Data[i])) return 'int32Data: integer[] expected';
      }
      if (message.stringData != null && message.hasOwnProperty('stringData')) {
        if (!Array.isArray(message.stringData)) return 'stringData: array expected';
        for (var i = 0; i < message.stringData.length; ++i)
          if (
            !(
              (message.stringData[i] && typeof message.stringData[i].length === 'number') ||
              $util.isString(message.stringData[i])
            )
          )
            return 'stringData: buffer[] expected';
      }
      if (message.int64Data != null && message.hasOwnProperty('int64Data')) {
        if (!Array.isArray(message.int64Data)) return 'int64Data: array expected';
        for (var i = 0; i < message.int64Data.length; ++i)
          if (
            !$util.isInteger(message.int64Data[i]) &&
            !(
              message.int64Data[i] &&
              $util.isInteger(message.int64Data[i].low) &&
              $util.isInteger(message.int64Data[i].high)
            )
          )
            return 'int64Data: integer|Long[] expected';
      }
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      if (message.rawData != null && message.hasOwnProperty('rawData'))
        if (!((message.rawData && typeof message.rawData.length === 'number') || $util.isString(message.rawData)))
          return 'rawData: buffer expected';
      if (message.externalData != null && message.hasOwnProperty('externalData')) {
        if (!Array.isArray(message.externalData)) return 'externalData: array expected';
        for (var i = 0; i < message.externalData.length; ++i) {
          var error = $root.onnx.StringStringEntryProto.verify(message.externalData[i]);
          if (error) return 'externalData.' + error;
        }
      }
      if (message.dataLocation != null && message.hasOwnProperty('dataLocation'))
        switch (message.dataLocation) {
          default:
            return 'dataLocation: enum value expected';
          case 0:
          case 1:
            break;
        }
      if (message.doubleData != null && message.hasOwnProperty('doubleData')) {
        if (!Array.isArray(message.doubleData)) return 'doubleData: array expected';
        for (var i = 0; i < message.doubleData.length; ++i)
          if (typeof message.doubleData[i] !== 'number') return 'doubleData: number[] expected';
      }
      if (message.uint64Data != null && message.hasOwnProperty('uint64Data')) {
        if (!Array.isArray(message.uint64Data)) return 'uint64Data: array expected';
        for (var i = 0; i < message.uint64Data.length; ++i)
          if (
            !$util.isInteger(message.uint64Data[i]) &&
            !(
              message.uint64Data[i] &&
              $util.isInteger(message.uint64Data[i].low) &&
              $util.isInteger(message.uint64Data[i].high)
            )
          )
            return 'uint64Data: integer|Long[] expected';
      }
      return null;
    };

    /**
     * Creates a TensorProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.TensorProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.TensorProto} TensorProto
     */
    TensorProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.TensorProto) return object;
      var message = new $root.onnx.TensorProto();
      if (object.dims) {
        if (!Array.isArray(object.dims)) throw TypeError('.onnx.TensorProto.dims: array expected');
        message.dims = [];
        for (var i = 0; i < object.dims.length; ++i)
          if ($util.Long) (message.dims[i] = $util.Long.fromValue(object.dims[i])).unsigned = false;
          else if (typeof object.dims[i] === 'string') message.dims[i] = parseInt(object.dims[i], 10);
          else if (typeof object.dims[i] === 'number') message.dims[i] = object.dims[i];
          else if (typeof object.dims[i] === 'object')
            message.dims[i] = new $util.LongBits(object.dims[i].low >>> 0, object.dims[i].high >>> 0).toNumber();
      }
      if (object.dataType != null) message.dataType = object.dataType | 0;
      if (object.segment != null) {
        if (typeof object.segment !== 'object') throw TypeError('.onnx.TensorProto.segment: object expected');
        message.segment = $root.onnx.TensorProto.Segment.fromObject(object.segment);
      }
      if (object.floatData) {
        if (!Array.isArray(object.floatData)) throw TypeError('.onnx.TensorProto.floatData: array expected');
        message.floatData = [];
        for (var i = 0; i < object.floatData.length; ++i) message.floatData[i] = Number(object.floatData[i]);
      }
      if (object.int32Data) {
        if (!Array.isArray(object.int32Data)) throw TypeError('.onnx.TensorProto.int32Data: array expected');
        message.int32Data = [];
        for (var i = 0; i < object.int32Data.length; ++i) message.int32Data[i] = object.int32Data[i] | 0;
      }
      if (object.stringData) {
        if (!Array.isArray(object.stringData)) throw TypeError('.onnx.TensorProto.stringData: array expected');
        message.stringData = [];
        for (var i = 0; i < object.stringData.length; ++i)
          if (typeof object.stringData[i] === 'string')
            $util.base64.decode(
              object.stringData[i],
              (message.stringData[i] = $util.newBuffer($util.base64.length(object.stringData[i]))),
              0,
            );
          else if (object.stringData[i].length >= 0) message.stringData[i] = object.stringData[i];
      }
      if (object.int64Data) {
        if (!Array.isArray(object.int64Data)) throw TypeError('.onnx.TensorProto.int64Data: array expected');
        message.int64Data = [];
        for (var i = 0; i < object.int64Data.length; ++i)
          if ($util.Long) (message.int64Data[i] = $util.Long.fromValue(object.int64Data[i])).unsigned = false;
          else if (typeof object.int64Data[i] === 'string') message.int64Data[i] = parseInt(object.int64Data[i], 10);
          else if (typeof object.int64Data[i] === 'number') message.int64Data[i] = object.int64Data[i];
          else if (typeof object.int64Data[i] === 'object')
            message.int64Data[i] = new $util.LongBits(
              object.int64Data[i].low >>> 0,
              object.int64Data[i].high >>> 0,
            ).toNumber();
      }
      if (object.name != null) message.name = String(object.name);
      if (object.docString != null) message.docString = String(object.docString);
      if (object.rawData != null)
        if (typeof object.rawData === 'string')
          $util.base64.decode(
            object.rawData,
            (message.rawData = $util.newBuffer($util.base64.length(object.rawData))),
            0,
          );
        else if (object.rawData.length >= 0) message.rawData = object.rawData;
      if (object.externalData) {
        if (!Array.isArray(object.externalData)) throw TypeError('.onnx.TensorProto.externalData: array expected');
        message.externalData = [];
        for (var i = 0; i < object.externalData.length; ++i) {
          if (typeof object.externalData[i] !== 'object')
            throw TypeError('.onnx.TensorProto.externalData: object expected');
          message.externalData[i] = $root.onnx.StringStringEntryProto.fromObject(object.externalData[i]);
        }
      }
      switch (object.dataLocation) {
        default:
          if (typeof object.dataLocation === 'number') {
            message.dataLocation = object.dataLocation;
            break;
          }
          break;
        case 'DEFAULT':
        case 0:
          message.dataLocation = 0;
          break;
        case 'EXTERNAL':
        case 1:
          message.dataLocation = 1;
          break;
      }
      if (object.doubleData) {
        if (!Array.isArray(object.doubleData)) throw TypeError('.onnx.TensorProto.doubleData: array expected');
        message.doubleData = [];
        for (var i = 0; i < object.doubleData.length; ++i) message.doubleData[i] = Number(object.doubleData[i]);
      }
      if (object.uint64Data) {
        if (!Array.isArray(object.uint64Data)) throw TypeError('.onnx.TensorProto.uint64Data: array expected');
        message.uint64Data = [];
        for (var i = 0; i < object.uint64Data.length; ++i)
          if ($util.Long) (message.uint64Data[i] = $util.Long.fromValue(object.uint64Data[i])).unsigned = true;
          else if (typeof object.uint64Data[i] === 'string') message.uint64Data[i] = parseInt(object.uint64Data[i], 10);
          else if (typeof object.uint64Data[i] === 'number') message.uint64Data[i] = object.uint64Data[i];
          else if (typeof object.uint64Data[i] === 'object')
            message.uint64Data[i] = new $util.LongBits(
              object.uint64Data[i].low >>> 0,
              object.uint64Data[i].high >>> 0,
            ).toNumber(true);
      }
      return message;
    };

    /**
     * Creates a plain object from a TensorProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.TensorProto
     * @static
     * @param {onnx.TensorProto} message TensorProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    TensorProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.dims = [];
        object.floatData = [];
        object.int32Data = [];
        object.stringData = [];
        object.int64Data = [];
        object.doubleData = [];
        object.uint64Data = [];
        object.externalData = [];
      }
      if (options.defaults) {
        object.dataType = 0;
        object.segment = null;
        object.name = '';
        if (options.bytes === String) object.rawData = '';
        else {
          object.rawData = [];
          if (options.bytes !== Array) object.rawData = $util.newBuffer(object.rawData);
        }
        object.docString = '';
        object.dataLocation = options.enums === String ? 'DEFAULT' : 0;
      }
      if (message.dims && message.dims.length) {
        object.dims = [];
        for (var j = 0; j < message.dims.length; ++j)
          if (typeof message.dims[j] === 'number')
            object.dims[j] = options.longs === String ? String(message.dims[j]) : message.dims[j];
          else
            object.dims[j] =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.dims[j])
                : options.longs === Number
                  ? new $util.LongBits(message.dims[j].low >>> 0, message.dims[j].high >>> 0).toNumber()
                  : message.dims[j];
      }
      if (message.dataType != null && message.hasOwnProperty('dataType')) object.dataType = message.dataType;
      if (message.segment != null && message.hasOwnProperty('segment'))
        object.segment = $root.onnx.TensorProto.Segment.toObject(message.segment, options);
      if (message.floatData && message.floatData.length) {
        object.floatData = [];
        for (var j = 0; j < message.floatData.length; ++j)
          object.floatData[j] =
            options.json && !isFinite(message.floatData[j]) ? String(message.floatData[j]) : message.floatData[j];
      }
      if (message.int32Data && message.int32Data.length) {
        object.int32Data = [];
        for (var j = 0; j < message.int32Data.length; ++j) object.int32Data[j] = message.int32Data[j];
      }
      if (message.stringData && message.stringData.length) {
        object.stringData = [];
        for (var j = 0; j < message.stringData.length; ++j)
          object.stringData[j] =
            options.bytes === String
              ? $util.base64.encode(message.stringData[j], 0, message.stringData[j].length)
              : options.bytes === Array
                ? Array.prototype.slice.call(message.stringData[j])
                : message.stringData[j];
      }
      if (message.int64Data && message.int64Data.length) {
        object.int64Data = [];
        for (var j = 0; j < message.int64Data.length; ++j)
          if (typeof message.int64Data[j] === 'number')
            object.int64Data[j] = options.longs === String ? String(message.int64Data[j]) : message.int64Data[j];
          else
            object.int64Data[j] =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.int64Data[j])
                : options.longs === Number
                  ? new $util.LongBits(message.int64Data[j].low >>> 0, message.int64Data[j].high >>> 0).toNumber()
                  : message.int64Data[j];
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.rawData != null && message.hasOwnProperty('rawData'))
        object.rawData =
          options.bytes === String
            ? $util.base64.encode(message.rawData, 0, message.rawData.length)
            : options.bytes === Array
              ? Array.prototype.slice.call(message.rawData)
              : message.rawData;
      if (message.doubleData && message.doubleData.length) {
        object.doubleData = [];
        for (var j = 0; j < message.doubleData.length; ++j)
          object.doubleData[j] =
            options.json && !isFinite(message.doubleData[j]) ? String(message.doubleData[j]) : message.doubleData[j];
      }
      if (message.uint64Data && message.uint64Data.length) {
        object.uint64Data = [];
        for (var j = 0; j < message.uint64Data.length; ++j)
          if (typeof message.uint64Data[j] === 'number')
            object.uint64Data[j] = options.longs === String ? String(message.uint64Data[j]) : message.uint64Data[j];
          else
            object.uint64Data[j] =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.uint64Data[j])
                : options.longs === Number
                  ? new $util.LongBits(message.uint64Data[j].low >>> 0, message.uint64Data[j].high >>> 0).toNumber(true)
                  : message.uint64Data[j];
      }
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.externalData && message.externalData.length) {
        object.externalData = [];
        for (var j = 0; j < message.externalData.length; ++j)
          object.externalData[j] = $root.onnx.StringStringEntryProto.toObject(message.externalData[j], options);
      }
      if (message.dataLocation != null && message.hasOwnProperty('dataLocation'))
        object.dataLocation =
          options.enums === String
            ? $root.onnx.TensorProto.DataLocation[message.dataLocation] === undefined
              ? message.dataLocation
              : $root.onnx.TensorProto.DataLocation[message.dataLocation]
            : message.dataLocation;
      return object;
    };

    /**
     * Converts this TensorProto to JSON.
     * @function toJSON
     * @memberof onnx.TensorProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    TensorProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for TensorProto
     * @function getTypeUrl
     * @memberof onnx.TensorProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    TensorProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.TensorProto';
    };

    /**
     * DataType enum.
     * @name onnx.TensorProto.DataType
     * @enum {number}
     * @property {number} UNDEFINED=0 UNDEFINED value
     * @property {number} FLOAT=1 FLOAT value
     * @property {number} UINT8=2 UINT8 value
     * @property {number} INT8=3 INT8 value
     * @property {number} UINT16=4 UINT16 value
     * @property {number} INT16=5 INT16 value
     * @property {number} INT32=6 INT32 value
     * @property {number} INT64=7 INT64 value
     * @property {number} STRING=8 STRING value
     * @property {number} BOOL=9 BOOL value
     * @property {number} FLOAT16=10 FLOAT16 value
     * @property {number} DOUBLE=11 DOUBLE value
     * @property {number} UINT32=12 UINT32 value
     * @property {number} UINT64=13 UINT64 value
     * @property {number} COMPLEX64=14 COMPLEX64 value
     * @property {number} COMPLEX128=15 COMPLEX128 value
     * @property {number} BFLOAT16=16 BFLOAT16 value
     * @property {number} FLOAT8E4M3FN=17 FLOAT8E4M3FN value
     * @property {number} FLOAT8E4M3FNUZ=18 FLOAT8E4M3FNUZ value
     * @property {number} FLOAT8E5M2=19 FLOAT8E5M2 value
     * @property {number} FLOAT8E5M2FNUZ=20 FLOAT8E5M2FNUZ value
     */
    TensorProto.DataType = (function () {
      var valuesById = {},
        values = Object.create(valuesById);
      values[(valuesById[0] = 'UNDEFINED')] = 0;
      values[(valuesById[1] = 'FLOAT')] = 1;
      values[(valuesById[2] = 'UINT8')] = 2;
      values[(valuesById[3] = 'INT8')] = 3;
      values[(valuesById[4] = 'UINT16')] = 4;
      values[(valuesById[5] = 'INT16')] = 5;
      values[(valuesById[6] = 'INT32')] = 6;
      values[(valuesById[7] = 'INT64')] = 7;
      values[(valuesById[8] = 'STRING')] = 8;
      values[(valuesById[9] = 'BOOL')] = 9;
      values[(valuesById[10] = 'FLOAT16')] = 10;
      values[(valuesById[11] = 'DOUBLE')] = 11;
      values[(valuesById[12] = 'UINT32')] = 12;
      values[(valuesById[13] = 'UINT64')] = 13;
      values[(valuesById[14] = 'COMPLEX64')] = 14;
      values[(valuesById[15] = 'COMPLEX128')] = 15;
      values[(valuesById[16] = 'BFLOAT16')] = 16;
      values[(valuesById[17] = 'FLOAT8E4M3FN')] = 17;
      values[(valuesById[18] = 'FLOAT8E4M3FNUZ')] = 18;
      values[(valuesById[19] = 'FLOAT8E5M2')] = 19;
      values[(valuesById[20] = 'FLOAT8E5M2FNUZ')] = 20;
      return values;
    })();

    TensorProto.Segment = (function () {
      /**
       * Properties of a Segment.
       * @memberof onnx.TensorProto
       * @interface ISegment
       * @property {number|Long|null} [begin] Segment begin
       * @property {number|Long|null} [end] Segment end
       */

      /**
       * Constructs a new Segment.
       * @memberof onnx.TensorProto
       * @classdesc Represents a Segment.
       * @implements ISegment
       * @constructor
       * @param {onnx.TensorProto.ISegment=} [properties] Properties to set
       */
      function Segment(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Segment begin.
       * @member {number|Long} begin
       * @memberof onnx.TensorProto.Segment
       * @instance
       */
      Segment.prototype.begin = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

      /**
       * Segment end.
       * @member {number|Long} end
       * @memberof onnx.TensorProto.Segment
       * @instance
       */
      Segment.prototype.end = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

      /**
       * Creates a new Segment instance using the specified properties.
       * @function create
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {onnx.TensorProto.ISegment=} [properties] Properties to set
       * @returns {onnx.TensorProto.Segment} Segment instance
       */
      Segment.create = function create(properties) {
        return new Segment(properties);
      };

      /**
       * Encodes the specified Segment message. Does not implicitly {@link onnx.TensorProto.Segment.verify|verify} messages.
       * @function encode
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {onnx.TensorProto.ISegment} message Segment message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Segment.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.begin != null && Object.hasOwnProperty.call(message, 'begin'))
          writer.uint32(/* id 1, wireType 0 =*/ 8).int64(message.begin);
        if (message.end != null && Object.hasOwnProperty.call(message, 'end'))
          writer.uint32(/* id 2, wireType 0 =*/ 16).int64(message.end);
        return writer;
      };

      /**
       * Encodes the specified Segment message, length delimited. Does not implicitly {@link onnx.TensorProto.Segment.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {onnx.TensorProto.ISegment} message Segment message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Segment.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a Segment message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TensorProto.Segment} Segment
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Segment.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TensorProto.Segment();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.begin = reader.int64();
              break;
            }
            case 2: {
              message.end = reader.int64();
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a Segment message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TensorProto.Segment} Segment
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Segment.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a Segment message.
       * @function verify
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Segment.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.begin != null && message.hasOwnProperty('begin'))
          if (
            !$util.isInteger(message.begin) &&
            !(message.begin && $util.isInteger(message.begin.low) && $util.isInteger(message.begin.high))
          )
            return 'begin: integer|Long expected';
        if (message.end != null && message.hasOwnProperty('end'))
          if (
            !$util.isInteger(message.end) &&
            !(message.end && $util.isInteger(message.end.low) && $util.isInteger(message.end.high))
          )
            return 'end: integer|Long expected';
        return null;
      };

      /**
       * Creates a Segment message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TensorProto.Segment} Segment
       */
      Segment.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TensorProto.Segment) return object;
        var message = new $root.onnx.TensorProto.Segment();
        if (object.begin != null)
          if ($util.Long) (message.begin = $util.Long.fromValue(object.begin)).unsigned = false;
          else if (typeof object.begin === 'string') message.begin = parseInt(object.begin, 10);
          else if (typeof object.begin === 'number') message.begin = object.begin;
          else if (typeof object.begin === 'object')
            message.begin = new $util.LongBits(object.begin.low >>> 0, object.begin.high >>> 0).toNumber();
        if (object.end != null)
          if ($util.Long) (message.end = $util.Long.fromValue(object.end)).unsigned = false;
          else if (typeof object.end === 'string') message.end = parseInt(object.end, 10);
          else if (typeof object.end === 'number') message.end = object.end;
          else if (typeof object.end === 'object')
            message.end = new $util.LongBits(object.end.low >>> 0, object.end.high >>> 0).toNumber();
        return message;
      };

      /**
       * Creates a plain object from a Segment message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {onnx.TensorProto.Segment} message Segment
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Segment.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) {
          if ($util.Long) {
            var long = new $util.Long(0, 0, false);
            object.begin =
              options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
          } else object.begin = options.longs === String ? '0' : 0;
          if ($util.Long) {
            var long = new $util.Long(0, 0, false);
            object.end = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
          } else object.end = options.longs === String ? '0' : 0;
        }
        if (message.begin != null && message.hasOwnProperty('begin'))
          if (typeof message.begin === 'number')
            object.begin = options.longs === String ? String(message.begin) : message.begin;
          else
            object.begin =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.begin)
                : options.longs === Number
                  ? new $util.LongBits(message.begin.low >>> 0, message.begin.high >>> 0).toNumber()
                  : message.begin;
        if (message.end != null && message.hasOwnProperty('end'))
          if (typeof message.end === 'number')
            object.end = options.longs === String ? String(message.end) : message.end;
          else
            object.end =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.end)
                : options.longs === Number
                  ? new $util.LongBits(message.end.low >>> 0, message.end.high >>> 0).toNumber()
                  : message.end;
        return object;
      };

      /**
       * Converts this Segment to JSON.
       * @function toJSON
       * @memberof onnx.TensorProto.Segment
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Segment.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Segment
       * @function getTypeUrl
       * @memberof onnx.TensorProto.Segment
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Segment.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TensorProto.Segment';
      };

      return Segment;
    })();

    /**
     * DataLocation enum.
     * @name onnx.TensorProto.DataLocation
     * @enum {number}
     * @property {number} DEFAULT=0 DEFAULT value
     * @property {number} EXTERNAL=1 EXTERNAL value
     */
    TensorProto.DataLocation = (function () {
      var valuesById = {},
        values = Object.create(valuesById);
      values[(valuesById[0] = 'DEFAULT')] = 0;
      values[(valuesById[1] = 'EXTERNAL')] = 1;
      return values;
    })();

    return TensorProto;
  })();

  onnx.SparseTensorProto = (function () {
    /**
     * Properties of a SparseTensorProto.
     * @memberof onnx
     * @interface ISparseTensorProto
     * @property {onnx.ITensorProto|null} [values] SparseTensorProto values
     * @property {onnx.ITensorProto|null} [indices] SparseTensorProto indices
     * @property {Array.<number|Long>|null} [dims] SparseTensorProto dims
     */

    /**
     * Constructs a new SparseTensorProto.
     * @memberof onnx
     * @classdesc Represents a SparseTensorProto.
     * @implements ISparseTensorProto
     * @constructor
     * @param {onnx.ISparseTensorProto=} [properties] Properties to set
     */
    function SparseTensorProto(properties) {
      this.dims = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * SparseTensorProto values.
     * @member {onnx.ITensorProto|null|undefined} values
     * @memberof onnx.SparseTensorProto
     * @instance
     */
    SparseTensorProto.prototype.values = null;

    /**
     * SparseTensorProto indices.
     * @member {onnx.ITensorProto|null|undefined} indices
     * @memberof onnx.SparseTensorProto
     * @instance
     */
    SparseTensorProto.prototype.indices = null;

    /**
     * SparseTensorProto dims.
     * @member {Array.<number|Long>} dims
     * @memberof onnx.SparseTensorProto
     * @instance
     */
    SparseTensorProto.prototype.dims = $util.emptyArray;

    /**
     * Creates a new SparseTensorProto instance using the specified properties.
     * @function create
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {onnx.ISparseTensorProto=} [properties] Properties to set
     * @returns {onnx.SparseTensorProto} SparseTensorProto instance
     */
    SparseTensorProto.create = function create(properties) {
      return new SparseTensorProto(properties);
    };

    /**
     * Encodes the specified SparseTensorProto message. Does not implicitly {@link onnx.SparseTensorProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {onnx.ISparseTensorProto} message SparseTensorProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SparseTensorProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.values != null && Object.hasOwnProperty.call(message, 'values'))
        $root.onnx.TensorProto.encode(message.values, writer.uint32(/* id 1, wireType 2 =*/ 10).fork()).ldelim();
      if (message.indices != null && Object.hasOwnProperty.call(message, 'indices'))
        $root.onnx.TensorProto.encode(message.indices, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
      if (message.dims != null && message.dims.length) {
        writer.uint32(/* id 3, wireType 2 =*/ 26).fork();
        for (var i = 0; i < message.dims.length; ++i) writer.int64(message.dims[i]);
        writer.ldelim();
      }
      return writer;
    };

    /**
     * Encodes the specified SparseTensorProto message, length delimited. Does not implicitly {@link onnx.SparseTensorProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {onnx.ISparseTensorProto} message SparseTensorProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    SparseTensorProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a SparseTensorProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.SparseTensorProto} SparseTensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SparseTensorProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.SparseTensorProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.values = $root.onnx.TensorProto.decode(reader, reader.uint32());
            break;
          }
          case 2: {
            message.indices = $root.onnx.TensorProto.decode(reader, reader.uint32());
            break;
          }
          case 3: {
            if (!(message.dims && message.dims.length)) message.dims = [];
            if ((tag & 7) === 2) {
              var end2 = reader.uint32() + reader.pos;
              while (reader.pos < end2) message.dims.push(reader.int64());
            } else message.dims.push(reader.int64());
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a SparseTensorProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.SparseTensorProto} SparseTensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    SparseTensorProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a SparseTensorProto message.
     * @function verify
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    SparseTensorProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.values != null && message.hasOwnProperty('values')) {
        var error = $root.onnx.TensorProto.verify(message.values);
        if (error) return 'values.' + error;
      }
      if (message.indices != null && message.hasOwnProperty('indices')) {
        var error = $root.onnx.TensorProto.verify(message.indices);
        if (error) return 'indices.' + error;
      }
      if (message.dims != null && message.hasOwnProperty('dims')) {
        if (!Array.isArray(message.dims)) return 'dims: array expected';
        for (var i = 0; i < message.dims.length; ++i)
          if (
            !$util.isInteger(message.dims[i]) &&
            !(message.dims[i] && $util.isInteger(message.dims[i].low) && $util.isInteger(message.dims[i].high))
          )
            return 'dims: integer|Long[] expected';
      }
      return null;
    };

    /**
     * Creates a SparseTensorProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.SparseTensorProto} SparseTensorProto
     */
    SparseTensorProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.SparseTensorProto) return object;
      var message = new $root.onnx.SparseTensorProto();
      if (object.values != null) {
        if (typeof object.values !== 'object') throw TypeError('.onnx.SparseTensorProto.values: object expected');
        message.values = $root.onnx.TensorProto.fromObject(object.values);
      }
      if (object.indices != null) {
        if (typeof object.indices !== 'object') throw TypeError('.onnx.SparseTensorProto.indices: object expected');
        message.indices = $root.onnx.TensorProto.fromObject(object.indices);
      }
      if (object.dims) {
        if (!Array.isArray(object.dims)) throw TypeError('.onnx.SparseTensorProto.dims: array expected');
        message.dims = [];
        for (var i = 0; i < object.dims.length; ++i)
          if ($util.Long) (message.dims[i] = $util.Long.fromValue(object.dims[i])).unsigned = false;
          else if (typeof object.dims[i] === 'string') message.dims[i] = parseInt(object.dims[i], 10);
          else if (typeof object.dims[i] === 'number') message.dims[i] = object.dims[i];
          else if (typeof object.dims[i] === 'object')
            message.dims[i] = new $util.LongBits(object.dims[i].low >>> 0, object.dims[i].high >>> 0).toNumber();
      }
      return message;
    };

    /**
     * Creates a plain object from a SparseTensorProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {onnx.SparseTensorProto} message SparseTensorProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    SparseTensorProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) object.dims = [];
      if (options.defaults) {
        object.values = null;
        object.indices = null;
      }
      if (message.values != null && message.hasOwnProperty('values'))
        object.values = $root.onnx.TensorProto.toObject(message.values, options);
      if (message.indices != null && message.hasOwnProperty('indices'))
        object.indices = $root.onnx.TensorProto.toObject(message.indices, options);
      if (message.dims && message.dims.length) {
        object.dims = [];
        for (var j = 0; j < message.dims.length; ++j)
          if (typeof message.dims[j] === 'number')
            object.dims[j] = options.longs === String ? String(message.dims[j]) : message.dims[j];
          else
            object.dims[j] =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.dims[j])
                : options.longs === Number
                  ? new $util.LongBits(message.dims[j].low >>> 0, message.dims[j].high >>> 0).toNumber()
                  : message.dims[j];
      }
      return object;
    };

    /**
     * Converts this SparseTensorProto to JSON.
     * @function toJSON
     * @memberof onnx.SparseTensorProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    SparseTensorProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for SparseTensorProto
     * @function getTypeUrl
     * @memberof onnx.SparseTensorProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    SparseTensorProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.SparseTensorProto';
    };

    return SparseTensorProto;
  })();

  onnx.TensorShapeProto = (function () {
    /**
     * Properties of a TensorShapeProto.
     * @memberof onnx
     * @interface ITensorShapeProto
     * @property {Array.<onnx.TensorShapeProto.IDimension>|null} [dim] TensorShapeProto dim
     */

    /**
     * Constructs a new TensorShapeProto.
     * @memberof onnx
     * @classdesc Represents a TensorShapeProto.
     * @implements ITensorShapeProto
     * @constructor
     * @param {onnx.ITensorShapeProto=} [properties] Properties to set
     */
    function TensorShapeProto(properties) {
      this.dim = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * TensorShapeProto dim.
     * @member {Array.<onnx.TensorShapeProto.IDimension>} dim
     * @memberof onnx.TensorShapeProto
     * @instance
     */
    TensorShapeProto.prototype.dim = $util.emptyArray;

    /**
     * Creates a new TensorShapeProto instance using the specified properties.
     * @function create
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {onnx.ITensorShapeProto=} [properties] Properties to set
     * @returns {onnx.TensorShapeProto} TensorShapeProto instance
     */
    TensorShapeProto.create = function create(properties) {
      return new TensorShapeProto(properties);
    };

    /**
     * Encodes the specified TensorShapeProto message. Does not implicitly {@link onnx.TensorShapeProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {onnx.ITensorShapeProto} message TensorShapeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorShapeProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.dim != null && message.dim.length)
        for (var i = 0; i < message.dim.length; ++i)
          $root.onnx.TensorShapeProto.Dimension.encode(
            message.dim[i],
            writer.uint32(/* id 1, wireType 2 =*/ 10).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified TensorShapeProto message, length delimited. Does not implicitly {@link onnx.TensorShapeProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {onnx.ITensorShapeProto} message TensorShapeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TensorShapeProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a TensorShapeProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.TensorShapeProto} TensorShapeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorShapeProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.TensorShapeProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            if (!(message.dim && message.dim.length)) message.dim = [];
            message.dim.push($root.onnx.TensorShapeProto.Dimension.decode(reader, reader.uint32()));
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a TensorShapeProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.TensorShapeProto} TensorShapeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TensorShapeProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a TensorShapeProto message.
     * @function verify
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    TensorShapeProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.dim != null && message.hasOwnProperty('dim')) {
        if (!Array.isArray(message.dim)) return 'dim: array expected';
        for (var i = 0; i < message.dim.length; ++i) {
          var error = $root.onnx.TensorShapeProto.Dimension.verify(message.dim[i]);
          if (error) return 'dim.' + error;
        }
      }
      return null;
    };

    /**
     * Creates a TensorShapeProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.TensorShapeProto} TensorShapeProto
     */
    TensorShapeProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.TensorShapeProto) return object;
      var message = new $root.onnx.TensorShapeProto();
      if (object.dim) {
        if (!Array.isArray(object.dim)) throw TypeError('.onnx.TensorShapeProto.dim: array expected');
        message.dim = [];
        for (var i = 0; i < object.dim.length; ++i) {
          if (typeof object.dim[i] !== 'object') throw TypeError('.onnx.TensorShapeProto.dim: object expected');
          message.dim[i] = $root.onnx.TensorShapeProto.Dimension.fromObject(object.dim[i]);
        }
      }
      return message;
    };

    /**
     * Creates a plain object from a TensorShapeProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {onnx.TensorShapeProto} message TensorShapeProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    TensorShapeProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) object.dim = [];
      if (message.dim && message.dim.length) {
        object.dim = [];
        for (var j = 0; j < message.dim.length; ++j)
          object.dim[j] = $root.onnx.TensorShapeProto.Dimension.toObject(message.dim[j], options);
      }
      return object;
    };

    /**
     * Converts this TensorShapeProto to JSON.
     * @function toJSON
     * @memberof onnx.TensorShapeProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    TensorShapeProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for TensorShapeProto
     * @function getTypeUrl
     * @memberof onnx.TensorShapeProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    TensorShapeProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.TensorShapeProto';
    };

    TensorShapeProto.Dimension = (function () {
      /**
       * Properties of a Dimension.
       * @memberof onnx.TensorShapeProto
       * @interface IDimension
       * @property {number|Long|null} [dimValue] Dimension dimValue
       * @property {string|null} [dimParam] Dimension dimParam
       * @property {string|null} [denotation] Dimension denotation
       */

      /**
       * Constructs a new Dimension.
       * @memberof onnx.TensorShapeProto
       * @classdesc Represents a Dimension.
       * @implements IDimension
       * @constructor
       * @param {onnx.TensorShapeProto.IDimension=} [properties] Properties to set
       */
      function Dimension(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Dimension dimValue.
       * @member {number|Long|null|undefined} dimValue
       * @memberof onnx.TensorShapeProto.Dimension
       * @instance
       */
      Dimension.prototype.dimValue = null;

      /**
       * Dimension dimParam.
       * @member {string|null|undefined} dimParam
       * @memberof onnx.TensorShapeProto.Dimension
       * @instance
       */
      Dimension.prototype.dimParam = null;

      /**
       * Dimension denotation.
       * @member {string} denotation
       * @memberof onnx.TensorShapeProto.Dimension
       * @instance
       */
      Dimension.prototype.denotation = '';

      // OneOf field names bound to virtual getters and setters
      var $oneOfFields;

      /**
       * Dimension value.
       * @member {"dimValue"|"dimParam"|undefined} value
       * @memberof onnx.TensorShapeProto.Dimension
       * @instance
       */
      Object.defineProperty(Dimension.prototype, 'value', {
        get: $util.oneOfGetter(($oneOfFields = ['dimValue', 'dimParam'])),
        set: $util.oneOfSetter($oneOfFields),
      });

      /**
       * Creates a new Dimension instance using the specified properties.
       * @function create
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {onnx.TensorShapeProto.IDimension=} [properties] Properties to set
       * @returns {onnx.TensorShapeProto.Dimension} Dimension instance
       */
      Dimension.create = function create(properties) {
        return new Dimension(properties);
      };

      /**
       * Encodes the specified Dimension message. Does not implicitly {@link onnx.TensorShapeProto.Dimension.verify|verify} messages.
       * @function encode
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {onnx.TensorShapeProto.IDimension} message Dimension message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Dimension.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.dimValue != null && Object.hasOwnProperty.call(message, 'dimValue'))
          writer.uint32(/* id 1, wireType 0 =*/ 8).int64(message.dimValue);
        if (message.dimParam != null && Object.hasOwnProperty.call(message, 'dimParam'))
          writer.uint32(/* id 2, wireType 2 =*/ 18).string(message.dimParam);
        if (message.denotation != null && Object.hasOwnProperty.call(message, 'denotation'))
          writer.uint32(/* id 3, wireType 2 =*/ 26).string(message.denotation);
        return writer;
      };

      /**
       * Encodes the specified Dimension message, length delimited. Does not implicitly {@link onnx.TensorShapeProto.Dimension.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {onnx.TensorShapeProto.IDimension} message Dimension message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Dimension.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a Dimension message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TensorShapeProto.Dimension} Dimension
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Dimension.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TensorShapeProto.Dimension();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.dimValue = reader.int64();
              break;
            }
            case 2: {
              message.dimParam = reader.string();
              break;
            }
            case 3: {
              message.denotation = reader.string();
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a Dimension message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TensorShapeProto.Dimension} Dimension
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Dimension.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a Dimension message.
       * @function verify
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Dimension.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        var properties = {};
        if (message.dimValue != null && message.hasOwnProperty('dimValue')) {
          properties.value = 1;
          if (
            !$util.isInteger(message.dimValue) &&
            !(message.dimValue && $util.isInteger(message.dimValue.low) && $util.isInteger(message.dimValue.high))
          )
            return 'dimValue: integer|Long expected';
        }
        if (message.dimParam != null && message.hasOwnProperty('dimParam')) {
          if (properties.value === 1) return 'value: multiple values';
          properties.value = 1;
          if (!$util.isString(message.dimParam)) return 'dimParam: string expected';
        }
        if (message.denotation != null && message.hasOwnProperty('denotation'))
          if (!$util.isString(message.denotation)) return 'denotation: string expected';
        return null;
      };

      /**
       * Creates a Dimension message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TensorShapeProto.Dimension} Dimension
       */
      Dimension.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TensorShapeProto.Dimension) return object;
        var message = new $root.onnx.TensorShapeProto.Dimension();
        if (object.dimValue != null)
          if ($util.Long) (message.dimValue = $util.Long.fromValue(object.dimValue)).unsigned = false;
          else if (typeof object.dimValue === 'string') message.dimValue = parseInt(object.dimValue, 10);
          else if (typeof object.dimValue === 'number') message.dimValue = object.dimValue;
          else if (typeof object.dimValue === 'object')
            message.dimValue = new $util.LongBits(object.dimValue.low >>> 0, object.dimValue.high >>> 0).toNumber();
        if (object.dimParam != null) message.dimParam = String(object.dimParam);
        if (object.denotation != null) message.denotation = String(object.denotation);
        return message;
      };

      /**
       * Creates a plain object from a Dimension message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {onnx.TensorShapeProto.Dimension} message Dimension
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Dimension.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) object.denotation = '';
        if (message.dimValue != null && message.hasOwnProperty('dimValue')) {
          if (typeof message.dimValue === 'number')
            object.dimValue = options.longs === String ? String(message.dimValue) : message.dimValue;
          else
            object.dimValue =
              options.longs === String
                ? $util.Long.prototype.toString.call(message.dimValue)
                : options.longs === Number
                  ? new $util.LongBits(message.dimValue.low >>> 0, message.dimValue.high >>> 0).toNumber()
                  : message.dimValue;
          if (options.oneofs) object.value = 'dimValue';
        }
        if (message.dimParam != null && message.hasOwnProperty('dimParam')) {
          object.dimParam = message.dimParam;
          if (options.oneofs) object.value = 'dimParam';
        }
        if (message.denotation != null && message.hasOwnProperty('denotation')) object.denotation = message.denotation;
        return object;
      };

      /**
       * Converts this Dimension to JSON.
       * @function toJSON
       * @memberof onnx.TensorShapeProto.Dimension
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Dimension.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Dimension
       * @function getTypeUrl
       * @memberof onnx.TensorShapeProto.Dimension
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Dimension.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TensorShapeProto.Dimension';
      };

      return Dimension;
    })();

    return TensorShapeProto;
  })();

  onnx.TypeProto = (function () {
    /**
     * Properties of a TypeProto.
     * @memberof onnx
     * @interface ITypeProto
     * @property {onnx.TypeProto.ITensor|null} [tensorType] TypeProto tensorType
     * @property {onnx.TypeProto.ISequence|null} [sequenceType] TypeProto sequenceType
     * @property {onnx.TypeProto.IMap|null} [mapType] TypeProto mapType
     * @property {onnx.TypeProto.IOptional|null} [optionalType] TypeProto optionalType
     * @property {onnx.TypeProto.ISparseTensor|null} [sparseTensorType] TypeProto sparseTensorType
     * @property {string|null} [denotation] TypeProto denotation
     */

    /**
     * Constructs a new TypeProto.
     * @memberof onnx
     * @classdesc Represents a TypeProto.
     * @implements ITypeProto
     * @constructor
     * @param {onnx.ITypeProto=} [properties] Properties to set
     */
    function TypeProto(properties) {
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * TypeProto tensorType.
     * @member {onnx.TypeProto.ITensor|null|undefined} tensorType
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.tensorType = null;

    /**
     * TypeProto sequenceType.
     * @member {onnx.TypeProto.ISequence|null|undefined} sequenceType
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.sequenceType = null;

    /**
     * TypeProto mapType.
     * @member {onnx.TypeProto.IMap|null|undefined} mapType
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.mapType = null;

    /**
     * TypeProto optionalType.
     * @member {onnx.TypeProto.IOptional|null|undefined} optionalType
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.optionalType = null;

    /**
     * TypeProto sparseTensorType.
     * @member {onnx.TypeProto.ISparseTensor|null|undefined} sparseTensorType
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.sparseTensorType = null;

    /**
     * TypeProto denotation.
     * @member {string} denotation
     * @memberof onnx.TypeProto
     * @instance
     */
    TypeProto.prototype.denotation = '';

    // OneOf field names bound to virtual getters and setters
    var $oneOfFields;

    /**
     * TypeProto value.
     * @member {"tensorType"|"sequenceType"|"mapType"|"optionalType"|"sparseTensorType"|undefined} value
     * @memberof onnx.TypeProto
     * @instance
     */
    Object.defineProperty(TypeProto.prototype, 'value', {
      get: $util.oneOfGetter(
        ($oneOfFields = ['tensorType', 'sequenceType', 'mapType', 'optionalType', 'sparseTensorType']),
      ),
      set: $util.oneOfSetter($oneOfFields),
    });

    /**
     * Creates a new TypeProto instance using the specified properties.
     * @function create
     * @memberof onnx.TypeProto
     * @static
     * @param {onnx.ITypeProto=} [properties] Properties to set
     * @returns {onnx.TypeProto} TypeProto instance
     */
    TypeProto.create = function create(properties) {
      return new TypeProto(properties);
    };

    /**
     * Encodes the specified TypeProto message. Does not implicitly {@link onnx.TypeProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.TypeProto
     * @static
     * @param {onnx.ITypeProto} message TypeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TypeProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.tensorType != null && Object.hasOwnProperty.call(message, 'tensorType'))
        $root.onnx.TypeProto.Tensor.encode(
          message.tensorType,
          writer.uint32(/* id 1, wireType 2 =*/ 10).fork(),
        ).ldelim();
      if (message.sequenceType != null && Object.hasOwnProperty.call(message, 'sequenceType'))
        $root.onnx.TypeProto.Sequence.encode(
          message.sequenceType,
          writer.uint32(/* id 4, wireType 2 =*/ 34).fork(),
        ).ldelim();
      if (message.mapType != null && Object.hasOwnProperty.call(message, 'mapType'))
        $root.onnx.TypeProto.Map.encode(message.mapType, writer.uint32(/* id 5, wireType 2 =*/ 42).fork()).ldelim();
      if (message.denotation != null && Object.hasOwnProperty.call(message, 'denotation'))
        writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.denotation);
      if (message.sparseTensorType != null && Object.hasOwnProperty.call(message, 'sparseTensorType'))
        $root.onnx.TypeProto.SparseTensor.encode(
          message.sparseTensorType,
          writer.uint32(/* id 8, wireType 2 =*/ 66).fork(),
        ).ldelim();
      if (message.optionalType != null && Object.hasOwnProperty.call(message, 'optionalType'))
        $root.onnx.TypeProto.Optional.encode(
          message.optionalType,
          writer.uint32(/* id 9, wireType 2 =*/ 74).fork(),
        ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified TypeProto message, length delimited. Does not implicitly {@link onnx.TypeProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.TypeProto
     * @static
     * @param {onnx.ITypeProto} message TypeProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    TypeProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a TypeProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.TypeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.TypeProto} TypeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TypeProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.TypeProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.tensorType = $root.onnx.TypeProto.Tensor.decode(reader, reader.uint32());
            break;
          }
          case 4: {
            message.sequenceType = $root.onnx.TypeProto.Sequence.decode(reader, reader.uint32());
            break;
          }
          case 5: {
            message.mapType = $root.onnx.TypeProto.Map.decode(reader, reader.uint32());
            break;
          }
          case 9: {
            message.optionalType = $root.onnx.TypeProto.Optional.decode(reader, reader.uint32());
            break;
          }
          case 8: {
            message.sparseTensorType = $root.onnx.TypeProto.SparseTensor.decode(reader, reader.uint32());
            break;
          }
          case 6: {
            message.denotation = reader.string();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a TypeProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.TypeProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.TypeProto} TypeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    TypeProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a TypeProto message.
     * @function verify
     * @memberof onnx.TypeProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    TypeProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      var properties = {};
      if (message.tensorType != null && message.hasOwnProperty('tensorType')) {
        properties.value = 1;
        {
          var error = $root.onnx.TypeProto.Tensor.verify(message.tensorType);
          if (error) return 'tensorType.' + error;
        }
      }
      if (message.sequenceType != null && message.hasOwnProperty('sequenceType')) {
        if (properties.value === 1) return 'value: multiple values';
        properties.value = 1;
        {
          var error = $root.onnx.TypeProto.Sequence.verify(message.sequenceType);
          if (error) return 'sequenceType.' + error;
        }
      }
      if (message.mapType != null && message.hasOwnProperty('mapType')) {
        if (properties.value === 1) return 'value: multiple values';
        properties.value = 1;
        {
          var error = $root.onnx.TypeProto.Map.verify(message.mapType);
          if (error) return 'mapType.' + error;
        }
      }
      if (message.optionalType != null && message.hasOwnProperty('optionalType')) {
        if (properties.value === 1) return 'value: multiple values';
        properties.value = 1;
        {
          var error = $root.onnx.TypeProto.Optional.verify(message.optionalType);
          if (error) return 'optionalType.' + error;
        }
      }
      if (message.sparseTensorType != null && message.hasOwnProperty('sparseTensorType')) {
        if (properties.value === 1) return 'value: multiple values';
        properties.value = 1;
        {
          var error = $root.onnx.TypeProto.SparseTensor.verify(message.sparseTensorType);
          if (error) return 'sparseTensorType.' + error;
        }
      }
      if (message.denotation != null && message.hasOwnProperty('denotation'))
        if (!$util.isString(message.denotation)) return 'denotation: string expected';
      return null;
    };

    /**
     * Creates a TypeProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.TypeProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.TypeProto} TypeProto
     */
    TypeProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.TypeProto) return object;
      var message = new $root.onnx.TypeProto();
      if (object.tensorType != null) {
        if (typeof object.tensorType !== 'object') throw TypeError('.onnx.TypeProto.tensorType: object expected');
        message.tensorType = $root.onnx.TypeProto.Tensor.fromObject(object.tensorType);
      }
      if (object.sequenceType != null) {
        if (typeof object.sequenceType !== 'object') throw TypeError('.onnx.TypeProto.sequenceType: object expected');
        message.sequenceType = $root.onnx.TypeProto.Sequence.fromObject(object.sequenceType);
      }
      if (object.mapType != null) {
        if (typeof object.mapType !== 'object') throw TypeError('.onnx.TypeProto.mapType: object expected');
        message.mapType = $root.onnx.TypeProto.Map.fromObject(object.mapType);
      }
      if (object.optionalType != null) {
        if (typeof object.optionalType !== 'object') throw TypeError('.onnx.TypeProto.optionalType: object expected');
        message.optionalType = $root.onnx.TypeProto.Optional.fromObject(object.optionalType);
      }
      if (object.sparseTensorType != null) {
        if (typeof object.sparseTensorType !== 'object')
          throw TypeError('.onnx.TypeProto.sparseTensorType: object expected');
        message.sparseTensorType = $root.onnx.TypeProto.SparseTensor.fromObject(object.sparseTensorType);
      }
      if (object.denotation != null) message.denotation = String(object.denotation);
      return message;
    };

    /**
     * Creates a plain object from a TypeProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.TypeProto
     * @static
     * @param {onnx.TypeProto} message TypeProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    TypeProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.defaults) object.denotation = '';
      if (message.tensorType != null && message.hasOwnProperty('tensorType')) {
        object.tensorType = $root.onnx.TypeProto.Tensor.toObject(message.tensorType, options);
        if (options.oneofs) object.value = 'tensorType';
      }
      if (message.sequenceType != null && message.hasOwnProperty('sequenceType')) {
        object.sequenceType = $root.onnx.TypeProto.Sequence.toObject(message.sequenceType, options);
        if (options.oneofs) object.value = 'sequenceType';
      }
      if (message.mapType != null && message.hasOwnProperty('mapType')) {
        object.mapType = $root.onnx.TypeProto.Map.toObject(message.mapType, options);
        if (options.oneofs) object.value = 'mapType';
      }
      if (message.denotation != null && message.hasOwnProperty('denotation')) object.denotation = message.denotation;
      if (message.sparseTensorType != null && message.hasOwnProperty('sparseTensorType')) {
        object.sparseTensorType = $root.onnx.TypeProto.SparseTensor.toObject(message.sparseTensorType, options);
        if (options.oneofs) object.value = 'sparseTensorType';
      }
      if (message.optionalType != null && message.hasOwnProperty('optionalType')) {
        object.optionalType = $root.onnx.TypeProto.Optional.toObject(message.optionalType, options);
        if (options.oneofs) object.value = 'optionalType';
      }
      return object;
    };

    /**
     * Converts this TypeProto to JSON.
     * @function toJSON
     * @memberof onnx.TypeProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    TypeProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for TypeProto
     * @function getTypeUrl
     * @memberof onnx.TypeProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    TypeProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.TypeProto';
    };

    TypeProto.Tensor = (function () {
      /**
       * Properties of a Tensor.
       * @memberof onnx.TypeProto
       * @interface ITensor
       * @property {number|null} [elemType] Tensor elemType
       * @property {onnx.ITensorShapeProto|null} [shape] Tensor shape
       */

      /**
       * Constructs a new Tensor.
       * @memberof onnx.TypeProto
       * @classdesc Represents a Tensor.
       * @implements ITensor
       * @constructor
       * @param {onnx.TypeProto.ITensor=} [properties] Properties to set
       */
      function Tensor(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Tensor elemType.
       * @member {number} elemType
       * @memberof onnx.TypeProto.Tensor
       * @instance
       */
      Tensor.prototype.elemType = 0;

      /**
       * Tensor shape.
       * @member {onnx.ITensorShapeProto|null|undefined} shape
       * @memberof onnx.TypeProto.Tensor
       * @instance
       */
      Tensor.prototype.shape = null;

      /**
       * Creates a new Tensor instance using the specified properties.
       * @function create
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {onnx.TypeProto.ITensor=} [properties] Properties to set
       * @returns {onnx.TypeProto.Tensor} Tensor instance
       */
      Tensor.create = function create(properties) {
        return new Tensor(properties);
      };

      /**
       * Encodes the specified Tensor message. Does not implicitly {@link onnx.TypeProto.Tensor.verify|verify} messages.
       * @function encode
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {onnx.TypeProto.ITensor} message Tensor message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Tensor.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.elemType != null && Object.hasOwnProperty.call(message, 'elemType'))
          writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.elemType);
        if (message.shape != null && Object.hasOwnProperty.call(message, 'shape'))
          $root.onnx.TensorShapeProto.encode(message.shape, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
        return writer;
      };

      /**
       * Encodes the specified Tensor message, length delimited. Does not implicitly {@link onnx.TypeProto.Tensor.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {onnx.TypeProto.ITensor} message Tensor message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Tensor.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a Tensor message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TypeProto.Tensor} Tensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Tensor.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TypeProto.Tensor();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.elemType = reader.int32();
              break;
            }
            case 2: {
              message.shape = $root.onnx.TensorShapeProto.decode(reader, reader.uint32());
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a Tensor message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TypeProto.Tensor} Tensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Tensor.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a Tensor message.
       * @function verify
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Tensor.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.elemType != null && message.hasOwnProperty('elemType'))
          if (!$util.isInteger(message.elemType)) return 'elemType: integer expected';
        if (message.shape != null && message.hasOwnProperty('shape')) {
          var error = $root.onnx.TensorShapeProto.verify(message.shape);
          if (error) return 'shape.' + error;
        }
        return null;
      };

      /**
       * Creates a Tensor message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TypeProto.Tensor} Tensor
       */
      Tensor.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TypeProto.Tensor) return object;
        var message = new $root.onnx.TypeProto.Tensor();
        if (object.elemType != null) message.elemType = object.elemType | 0;
        if (object.shape != null) {
          if (typeof object.shape !== 'object') throw TypeError('.onnx.TypeProto.Tensor.shape: object expected');
          message.shape = $root.onnx.TensorShapeProto.fromObject(object.shape);
        }
        return message;
      };

      /**
       * Creates a plain object from a Tensor message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {onnx.TypeProto.Tensor} message Tensor
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Tensor.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) {
          object.elemType = 0;
          object.shape = null;
        }
        if (message.elemType != null && message.hasOwnProperty('elemType')) object.elemType = message.elemType;
        if (message.shape != null && message.hasOwnProperty('shape'))
          object.shape = $root.onnx.TensorShapeProto.toObject(message.shape, options);
        return object;
      };

      /**
       * Converts this Tensor to JSON.
       * @function toJSON
       * @memberof onnx.TypeProto.Tensor
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Tensor.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Tensor
       * @function getTypeUrl
       * @memberof onnx.TypeProto.Tensor
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Tensor.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TypeProto.Tensor';
      };

      return Tensor;
    })();

    TypeProto.Sequence = (function () {
      /**
       * Properties of a Sequence.
       * @memberof onnx.TypeProto
       * @interface ISequence
       * @property {onnx.ITypeProto|null} [elemType] Sequence elemType
       */

      /**
       * Constructs a new Sequence.
       * @memberof onnx.TypeProto
       * @classdesc Represents a Sequence.
       * @implements ISequence
       * @constructor
       * @param {onnx.TypeProto.ISequence=} [properties] Properties to set
       */
      function Sequence(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Sequence elemType.
       * @member {onnx.ITypeProto|null|undefined} elemType
       * @memberof onnx.TypeProto.Sequence
       * @instance
       */
      Sequence.prototype.elemType = null;

      /**
       * Creates a new Sequence instance using the specified properties.
       * @function create
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {onnx.TypeProto.ISequence=} [properties] Properties to set
       * @returns {onnx.TypeProto.Sequence} Sequence instance
       */
      Sequence.create = function create(properties) {
        return new Sequence(properties);
      };

      /**
       * Encodes the specified Sequence message. Does not implicitly {@link onnx.TypeProto.Sequence.verify|verify} messages.
       * @function encode
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {onnx.TypeProto.ISequence} message Sequence message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Sequence.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.elemType != null && Object.hasOwnProperty.call(message, 'elemType'))
          $root.onnx.TypeProto.encode(message.elemType, writer.uint32(/* id 1, wireType 2 =*/ 10).fork()).ldelim();
        return writer;
      };

      /**
       * Encodes the specified Sequence message, length delimited. Does not implicitly {@link onnx.TypeProto.Sequence.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {onnx.TypeProto.ISequence} message Sequence message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Sequence.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a Sequence message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TypeProto.Sequence} Sequence
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Sequence.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TypeProto.Sequence();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.elemType = $root.onnx.TypeProto.decode(reader, reader.uint32());
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a Sequence message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TypeProto.Sequence} Sequence
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Sequence.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a Sequence message.
       * @function verify
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Sequence.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.elemType != null && message.hasOwnProperty('elemType')) {
          var error = $root.onnx.TypeProto.verify(message.elemType);
          if (error) return 'elemType.' + error;
        }
        return null;
      };

      /**
       * Creates a Sequence message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TypeProto.Sequence} Sequence
       */
      Sequence.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TypeProto.Sequence) return object;
        var message = new $root.onnx.TypeProto.Sequence();
        if (object.elemType != null) {
          if (typeof object.elemType !== 'object')
            throw TypeError('.onnx.TypeProto.Sequence.elemType: object expected');
          message.elemType = $root.onnx.TypeProto.fromObject(object.elemType);
        }
        return message;
      };

      /**
       * Creates a plain object from a Sequence message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {onnx.TypeProto.Sequence} message Sequence
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Sequence.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) object.elemType = null;
        if (message.elemType != null && message.hasOwnProperty('elemType'))
          object.elemType = $root.onnx.TypeProto.toObject(message.elemType, options);
        return object;
      };

      /**
       * Converts this Sequence to JSON.
       * @function toJSON
       * @memberof onnx.TypeProto.Sequence
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Sequence.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Sequence
       * @function getTypeUrl
       * @memberof onnx.TypeProto.Sequence
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Sequence.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TypeProto.Sequence';
      };

      return Sequence;
    })();

    TypeProto.Map = (function () {
      /**
       * Properties of a Map.
       * @memberof onnx.TypeProto
       * @interface IMap
       * @property {number|null} [keyType] Map keyType
       * @property {onnx.ITypeProto|null} [valueType] Map valueType
       */

      /**
       * Constructs a new Map.
       * @memberof onnx.TypeProto
       * @classdesc Represents a Map.
       * @implements IMap
       * @constructor
       * @param {onnx.TypeProto.IMap=} [properties] Properties to set
       */
      function Map(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Map keyType.
       * @member {number} keyType
       * @memberof onnx.TypeProto.Map
       * @instance
       */
      Map.prototype.keyType = 0;

      /**
       * Map valueType.
       * @member {onnx.ITypeProto|null|undefined} valueType
       * @memberof onnx.TypeProto.Map
       * @instance
       */
      Map.prototype.valueType = null;

      /**
       * Creates a new Map instance using the specified properties.
       * @function create
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {onnx.TypeProto.IMap=} [properties] Properties to set
       * @returns {onnx.TypeProto.Map} Map instance
       */
      Map.create = function create(properties) {
        return new Map(properties);
      };

      /**
       * Encodes the specified Map message. Does not implicitly {@link onnx.TypeProto.Map.verify|verify} messages.
       * @function encode
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {onnx.TypeProto.IMap} message Map message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Map.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.keyType != null && Object.hasOwnProperty.call(message, 'keyType'))
          writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.keyType);
        if (message.valueType != null && Object.hasOwnProperty.call(message, 'valueType'))
          $root.onnx.TypeProto.encode(message.valueType, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
        return writer;
      };

      /**
       * Encodes the specified Map message, length delimited. Does not implicitly {@link onnx.TypeProto.Map.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {onnx.TypeProto.IMap} message Map message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Map.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a Map message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TypeProto.Map} Map
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Map.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TypeProto.Map();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.keyType = reader.int32();
              break;
            }
            case 2: {
              message.valueType = $root.onnx.TypeProto.decode(reader, reader.uint32());
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a Map message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TypeProto.Map} Map
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Map.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a Map message.
       * @function verify
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Map.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.keyType != null && message.hasOwnProperty('keyType'))
          if (!$util.isInteger(message.keyType)) return 'keyType: integer expected';
        if (message.valueType != null && message.hasOwnProperty('valueType')) {
          var error = $root.onnx.TypeProto.verify(message.valueType);
          if (error) return 'valueType.' + error;
        }
        return null;
      };

      /**
       * Creates a Map message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TypeProto.Map} Map
       */
      Map.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TypeProto.Map) return object;
        var message = new $root.onnx.TypeProto.Map();
        if (object.keyType != null) message.keyType = object.keyType | 0;
        if (object.valueType != null) {
          if (typeof object.valueType !== 'object') throw TypeError('.onnx.TypeProto.Map.valueType: object expected');
          message.valueType = $root.onnx.TypeProto.fromObject(object.valueType);
        }
        return message;
      };

      /**
       * Creates a plain object from a Map message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {onnx.TypeProto.Map} message Map
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Map.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) {
          object.keyType = 0;
          object.valueType = null;
        }
        if (message.keyType != null && message.hasOwnProperty('keyType')) object.keyType = message.keyType;
        if (message.valueType != null && message.hasOwnProperty('valueType'))
          object.valueType = $root.onnx.TypeProto.toObject(message.valueType, options);
        return object;
      };

      /**
       * Converts this Map to JSON.
       * @function toJSON
       * @memberof onnx.TypeProto.Map
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Map.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Map
       * @function getTypeUrl
       * @memberof onnx.TypeProto.Map
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Map.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TypeProto.Map';
      };

      return Map;
    })();

    TypeProto.Optional = (function () {
      /**
       * Properties of an Optional.
       * @memberof onnx.TypeProto
       * @interface IOptional
       * @property {onnx.ITypeProto|null} [elemType] Optional elemType
       */

      /**
       * Constructs a new Optional.
       * @memberof onnx.TypeProto
       * @classdesc Represents an Optional.
       * @implements IOptional
       * @constructor
       * @param {onnx.TypeProto.IOptional=} [properties] Properties to set
       */
      function Optional(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * Optional elemType.
       * @member {onnx.ITypeProto|null|undefined} elemType
       * @memberof onnx.TypeProto.Optional
       * @instance
       */
      Optional.prototype.elemType = null;

      /**
       * Creates a new Optional instance using the specified properties.
       * @function create
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {onnx.TypeProto.IOptional=} [properties] Properties to set
       * @returns {onnx.TypeProto.Optional} Optional instance
       */
      Optional.create = function create(properties) {
        return new Optional(properties);
      };

      /**
       * Encodes the specified Optional message. Does not implicitly {@link onnx.TypeProto.Optional.verify|verify} messages.
       * @function encode
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {onnx.TypeProto.IOptional} message Optional message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Optional.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.elemType != null && Object.hasOwnProperty.call(message, 'elemType'))
          $root.onnx.TypeProto.encode(message.elemType, writer.uint32(/* id 1, wireType 2 =*/ 10).fork()).ldelim();
        return writer;
      };

      /**
       * Encodes the specified Optional message, length delimited. Does not implicitly {@link onnx.TypeProto.Optional.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {onnx.TypeProto.IOptional} message Optional message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      Optional.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes an Optional message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TypeProto.Optional} Optional
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Optional.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TypeProto.Optional();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.elemType = $root.onnx.TypeProto.decode(reader, reader.uint32());
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes an Optional message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TypeProto.Optional} Optional
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      Optional.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies an Optional message.
       * @function verify
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      Optional.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.elemType != null && message.hasOwnProperty('elemType')) {
          var error = $root.onnx.TypeProto.verify(message.elemType);
          if (error) return 'elemType.' + error;
        }
        return null;
      };

      /**
       * Creates an Optional message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TypeProto.Optional} Optional
       */
      Optional.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TypeProto.Optional) return object;
        var message = new $root.onnx.TypeProto.Optional();
        if (object.elemType != null) {
          if (typeof object.elemType !== 'object')
            throw TypeError('.onnx.TypeProto.Optional.elemType: object expected');
          message.elemType = $root.onnx.TypeProto.fromObject(object.elemType);
        }
        return message;
      };

      /**
       * Creates a plain object from an Optional message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {onnx.TypeProto.Optional} message Optional
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      Optional.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) object.elemType = null;
        if (message.elemType != null && message.hasOwnProperty('elemType'))
          object.elemType = $root.onnx.TypeProto.toObject(message.elemType, options);
        return object;
      };

      /**
       * Converts this Optional to JSON.
       * @function toJSON
       * @memberof onnx.TypeProto.Optional
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      Optional.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for Optional
       * @function getTypeUrl
       * @memberof onnx.TypeProto.Optional
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      Optional.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TypeProto.Optional';
      };

      return Optional;
    })();

    TypeProto.SparseTensor = (function () {
      /**
       * Properties of a SparseTensor.
       * @memberof onnx.TypeProto
       * @interface ISparseTensor
       * @property {number|null} [elemType] SparseTensor elemType
       * @property {onnx.ITensorShapeProto|null} [shape] SparseTensor shape
       */

      /**
       * Constructs a new SparseTensor.
       * @memberof onnx.TypeProto
       * @classdesc Represents a SparseTensor.
       * @implements ISparseTensor
       * @constructor
       * @param {onnx.TypeProto.ISparseTensor=} [properties] Properties to set
       */
      function SparseTensor(properties) {
        if (properties)
          for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
            if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
      }

      /**
       * SparseTensor elemType.
       * @member {number} elemType
       * @memberof onnx.TypeProto.SparseTensor
       * @instance
       */
      SparseTensor.prototype.elemType = 0;

      /**
       * SparseTensor shape.
       * @member {onnx.ITensorShapeProto|null|undefined} shape
       * @memberof onnx.TypeProto.SparseTensor
       * @instance
       */
      SparseTensor.prototype.shape = null;

      /**
       * Creates a new SparseTensor instance using the specified properties.
       * @function create
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {onnx.TypeProto.ISparseTensor=} [properties] Properties to set
       * @returns {onnx.TypeProto.SparseTensor} SparseTensor instance
       */
      SparseTensor.create = function create(properties) {
        return new SparseTensor(properties);
      };

      /**
       * Encodes the specified SparseTensor message. Does not implicitly {@link onnx.TypeProto.SparseTensor.verify|verify} messages.
       * @function encode
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {onnx.TypeProto.ISparseTensor} message SparseTensor message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      SparseTensor.encode = function encode(message, writer) {
        if (!writer) writer = $Writer.create();
        if (message.elemType != null && Object.hasOwnProperty.call(message, 'elemType'))
          writer.uint32(/* id 1, wireType 0 =*/ 8).int32(message.elemType);
        if (message.shape != null && Object.hasOwnProperty.call(message, 'shape'))
          $root.onnx.TensorShapeProto.encode(message.shape, writer.uint32(/* id 2, wireType 2 =*/ 18).fork()).ldelim();
        return writer;
      };

      /**
       * Encodes the specified SparseTensor message, length delimited. Does not implicitly {@link onnx.TypeProto.SparseTensor.verify|verify} messages.
       * @function encodeDelimited
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {onnx.TypeProto.ISparseTensor} message SparseTensor message or plain object to encode
       * @param {$protobuf.Writer} [writer] Writer to encode to
       * @returns {$protobuf.Writer} Writer
       */
      SparseTensor.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
      };

      /**
       * Decodes a SparseTensor message from the specified reader or buffer.
       * @function decode
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @param {number} [length] Message length if known beforehand
       * @returns {onnx.TypeProto.SparseTensor} SparseTensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      SparseTensor.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length,
          message = new $root.onnx.TypeProto.SparseTensor();
        while (reader.pos < end) {
          var tag = reader.uint32();
          switch (tag >>> 3) {
            case 1: {
              message.elemType = reader.int32();
              break;
            }
            case 2: {
              message.shape = $root.onnx.TensorShapeProto.decode(reader, reader.uint32());
              break;
            }
            default:
              reader.skipType(tag & 7);
              break;
          }
        }
        return message;
      };

      /**
       * Decodes a SparseTensor message from the specified reader or buffer, length delimited.
       * @function decodeDelimited
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
       * @returns {onnx.TypeProto.SparseTensor} SparseTensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      SparseTensor.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader)) reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
      };

      /**
       * Verifies a SparseTensor message.
       * @function verify
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {Object.<string,*>} message Plain object to verify
       * @returns {string|null} `null` if valid, otherwise the reason why it is not
       */
      SparseTensor.verify = function verify(message) {
        if (typeof message !== 'object' || message === null) return 'object expected';
        if (message.elemType != null && message.hasOwnProperty('elemType'))
          if (!$util.isInteger(message.elemType)) return 'elemType: integer expected';
        if (message.shape != null && message.hasOwnProperty('shape')) {
          var error = $root.onnx.TensorShapeProto.verify(message.shape);
          if (error) return 'shape.' + error;
        }
        return null;
      };

      /**
       * Creates a SparseTensor message from a plain object. Also converts values to their respective internal types.
       * @function fromObject
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {Object.<string,*>} object Plain object
       * @returns {onnx.TypeProto.SparseTensor} SparseTensor
       */
      SparseTensor.fromObject = function fromObject(object) {
        if (object instanceof $root.onnx.TypeProto.SparseTensor) return object;
        var message = new $root.onnx.TypeProto.SparseTensor();
        if (object.elemType != null) message.elemType = object.elemType | 0;
        if (object.shape != null) {
          if (typeof object.shape !== 'object') throw TypeError('.onnx.TypeProto.SparseTensor.shape: object expected');
          message.shape = $root.onnx.TensorShapeProto.fromObject(object.shape);
        }
        return message;
      };

      /**
       * Creates a plain object from a SparseTensor message. Also converts values to other types if specified.
       * @function toObject
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {onnx.TypeProto.SparseTensor} message SparseTensor
       * @param {$protobuf.IConversionOptions} [options] Conversion options
       * @returns {Object.<string,*>} Plain object
       */
      SparseTensor.toObject = function toObject(message, options) {
        if (!options) options = {};
        var object = {};
        if (options.defaults) {
          object.elemType = 0;
          object.shape = null;
        }
        if (message.elemType != null && message.hasOwnProperty('elemType')) object.elemType = message.elemType;
        if (message.shape != null && message.hasOwnProperty('shape'))
          object.shape = $root.onnx.TensorShapeProto.toObject(message.shape, options);
        return object;
      };

      /**
       * Converts this SparseTensor to JSON.
       * @function toJSON
       * @memberof onnx.TypeProto.SparseTensor
       * @instance
       * @returns {Object.<string,*>} JSON object
       */
      SparseTensor.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
      };

      /**
       * Gets the default type url for SparseTensor
       * @function getTypeUrl
       * @memberof onnx.TypeProto.SparseTensor
       * @static
       * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns {string} The default type url
       */
      SparseTensor.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
        if (typeUrlPrefix === undefined) {
          typeUrlPrefix = 'type.googleapis.com';
        }
        return typeUrlPrefix + '/onnx.TypeProto.SparseTensor';
      };

      return SparseTensor;
    })();

    return TypeProto;
  })();

  onnx.OperatorSetIdProto = (function () {
    /**
     * Properties of an OperatorSetIdProto.
     * @memberof onnx
     * @interface IOperatorSetIdProto
     * @property {string|null} [domain] OperatorSetIdProto domain
     * @property {number|Long|null} [version] OperatorSetIdProto version
     */

    /**
     * Constructs a new OperatorSetIdProto.
     * @memberof onnx
     * @classdesc Represents an OperatorSetIdProto.
     * @implements IOperatorSetIdProto
     * @constructor
     * @param {onnx.IOperatorSetIdProto=} [properties] Properties to set
     */
    function OperatorSetIdProto(properties) {
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * OperatorSetIdProto domain.
     * @member {string} domain
     * @memberof onnx.OperatorSetIdProto
     * @instance
     */
    OperatorSetIdProto.prototype.domain = '';

    /**
     * OperatorSetIdProto version.
     * @member {number|Long} version
     * @memberof onnx.OperatorSetIdProto
     * @instance
     */
    OperatorSetIdProto.prototype.version = $util.Long ? $util.Long.fromBits(0, 0, false) : 0;

    /**
     * Creates a new OperatorSetIdProto instance using the specified properties.
     * @function create
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {onnx.IOperatorSetIdProto=} [properties] Properties to set
     * @returns {onnx.OperatorSetIdProto} OperatorSetIdProto instance
     */
    OperatorSetIdProto.create = function create(properties) {
      return new OperatorSetIdProto(properties);
    };

    /**
     * Encodes the specified OperatorSetIdProto message. Does not implicitly {@link onnx.OperatorSetIdProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {onnx.IOperatorSetIdProto} message OperatorSetIdProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    OperatorSetIdProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.domain != null && Object.hasOwnProperty.call(message, 'domain'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.domain);
      if (message.version != null && Object.hasOwnProperty.call(message, 'version'))
        writer.uint32(/* id 2, wireType 0 =*/ 16).int64(message.version);
      return writer;
    };

    /**
     * Encodes the specified OperatorSetIdProto message, length delimited. Does not implicitly {@link onnx.OperatorSetIdProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {onnx.IOperatorSetIdProto} message OperatorSetIdProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    OperatorSetIdProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an OperatorSetIdProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.OperatorSetIdProto} OperatorSetIdProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    OperatorSetIdProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.OperatorSetIdProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.domain = reader.string();
            break;
          }
          case 2: {
            message.version = reader.int64();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes an OperatorSetIdProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.OperatorSetIdProto} OperatorSetIdProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    OperatorSetIdProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an OperatorSetIdProto message.
     * @function verify
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    OperatorSetIdProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.domain != null && message.hasOwnProperty('domain'))
        if (!$util.isString(message.domain)) return 'domain: string expected';
      if (message.version != null && message.hasOwnProperty('version'))
        if (
          !$util.isInteger(message.version) &&
          !(message.version && $util.isInteger(message.version.low) && $util.isInteger(message.version.high))
        )
          return 'version: integer|Long expected';
      return null;
    };

    /**
     * Creates an OperatorSetIdProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.OperatorSetIdProto} OperatorSetIdProto
     */
    OperatorSetIdProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.OperatorSetIdProto) return object;
      var message = new $root.onnx.OperatorSetIdProto();
      if (object.domain != null) message.domain = String(object.domain);
      if (object.version != null)
        if ($util.Long) (message.version = $util.Long.fromValue(object.version)).unsigned = false;
        else if (typeof object.version === 'string') message.version = parseInt(object.version, 10);
        else if (typeof object.version === 'number') message.version = object.version;
        else if (typeof object.version === 'object')
          message.version = new $util.LongBits(object.version.low >>> 0, object.version.high >>> 0).toNumber();
      return message;
    };

    /**
     * Creates a plain object from an OperatorSetIdProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {onnx.OperatorSetIdProto} message OperatorSetIdProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    OperatorSetIdProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.defaults) {
        object.domain = '';
        if ($util.Long) {
          var long = new $util.Long(0, 0, false);
          object.version =
            options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
        } else object.version = options.longs === String ? '0' : 0;
      }
      if (message.domain != null && message.hasOwnProperty('domain')) object.domain = message.domain;
      if (message.version != null && message.hasOwnProperty('version'))
        if (typeof message.version === 'number')
          object.version = options.longs === String ? String(message.version) : message.version;
        else
          object.version =
            options.longs === String
              ? $util.Long.prototype.toString.call(message.version)
              : options.longs === Number
                ? new $util.LongBits(message.version.low >>> 0, message.version.high >>> 0).toNumber()
                : message.version;
      return object;
    };

    /**
     * Converts this OperatorSetIdProto to JSON.
     * @function toJSON
     * @memberof onnx.OperatorSetIdProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    OperatorSetIdProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for OperatorSetIdProto
     * @function getTypeUrl
     * @memberof onnx.OperatorSetIdProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    OperatorSetIdProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.OperatorSetIdProto';
    };

    return OperatorSetIdProto;
  })();

  /**
   * OperatorStatus enum.
   * @name onnx.OperatorStatus
   * @enum {number}
   * @property {number} EXPERIMENTAL=0 EXPERIMENTAL value
   * @property {number} STABLE=1 STABLE value
   */
  onnx.OperatorStatus = (function () {
    var valuesById = {},
      values = Object.create(valuesById);
    values[(valuesById[0] = 'EXPERIMENTAL')] = 0;
    values[(valuesById[1] = 'STABLE')] = 1;
    return values;
  })();

  onnx.FunctionProto = (function () {
    /**
     * Properties of a FunctionProto.
     * @memberof onnx
     * @interface IFunctionProto
     * @property {string|null} [name] FunctionProto name
     * @property {Array.<string>|null} [input] FunctionProto input
     * @property {Array.<string>|null} [output] FunctionProto output
     * @property {Array.<string>|null} [attribute] FunctionProto attribute
     * @property {Array.<onnx.IAttributeProto>|null} [attributeProto] FunctionProto attributeProto
     * @property {Array.<onnx.INodeProto>|null} [node] FunctionProto node
     * @property {string|null} [docString] FunctionProto docString
     * @property {Array.<onnx.IOperatorSetIdProto>|null} [opsetImport] FunctionProto opsetImport
     * @property {string|null} [domain] FunctionProto domain
     */

    /**
     * Constructs a new FunctionProto.
     * @memberof onnx
     * @classdesc Represents a FunctionProto.
     * @implements IFunctionProto
     * @constructor
     * @param {onnx.IFunctionProto=} [properties] Properties to set
     */
    function FunctionProto(properties) {
      this.input = [];
      this.output = [];
      this.attribute = [];
      this.attributeProto = [];
      this.node = [];
      this.opsetImport = [];
      if (properties)
        for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
          if (properties[keys[i]] != null) this[keys[i]] = properties[keys[i]];
    }

    /**
     * FunctionProto name.
     * @member {string} name
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.name = '';

    /**
     * FunctionProto input.
     * @member {Array.<string>} input
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.input = $util.emptyArray;

    /**
     * FunctionProto output.
     * @member {Array.<string>} output
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.output = $util.emptyArray;

    /**
     * FunctionProto attribute.
     * @member {Array.<string>} attribute
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.attribute = $util.emptyArray;

    /**
     * FunctionProto attributeProto.
     * @member {Array.<onnx.IAttributeProto>} attributeProto
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.attributeProto = $util.emptyArray;

    /**
     * FunctionProto node.
     * @member {Array.<onnx.INodeProto>} node
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.node = $util.emptyArray;

    /**
     * FunctionProto docString.
     * @member {string} docString
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.docString = '';

    /**
     * FunctionProto opsetImport.
     * @member {Array.<onnx.IOperatorSetIdProto>} opsetImport
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.opsetImport = $util.emptyArray;

    /**
     * FunctionProto domain.
     * @member {string} domain
     * @memberof onnx.FunctionProto
     * @instance
     */
    FunctionProto.prototype.domain = '';

    /**
     * Creates a new FunctionProto instance using the specified properties.
     * @function create
     * @memberof onnx.FunctionProto
     * @static
     * @param {onnx.IFunctionProto=} [properties] Properties to set
     * @returns {onnx.FunctionProto} FunctionProto instance
     */
    FunctionProto.create = function create(properties) {
      return new FunctionProto(properties);
    };

    /**
     * Encodes the specified FunctionProto message. Does not implicitly {@link onnx.FunctionProto.verify|verify} messages.
     * @function encode
     * @memberof onnx.FunctionProto
     * @static
     * @param {onnx.IFunctionProto} message FunctionProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    FunctionProto.encode = function encode(message, writer) {
      if (!writer) writer = $Writer.create();
      if (message.name != null && Object.hasOwnProperty.call(message, 'name'))
        writer.uint32(/* id 1, wireType 2 =*/ 10).string(message.name);
      if (message.input != null && message.input.length)
        for (var i = 0; i < message.input.length; ++i)
          writer.uint32(/* id 4, wireType 2 =*/ 34).string(message.input[i]);
      if (message.output != null && message.output.length)
        for (var i = 0; i < message.output.length; ++i)
          writer.uint32(/* id 5, wireType 2 =*/ 42).string(message.output[i]);
      if (message.attribute != null && message.attribute.length)
        for (var i = 0; i < message.attribute.length; ++i)
          writer.uint32(/* id 6, wireType 2 =*/ 50).string(message.attribute[i]);
      if (message.node != null && message.node.length)
        for (var i = 0; i < message.node.length; ++i)
          $root.onnx.NodeProto.encode(message.node[i], writer.uint32(/* id 7, wireType 2 =*/ 58).fork()).ldelim();
      if (message.docString != null && Object.hasOwnProperty.call(message, 'docString'))
        writer.uint32(/* id 8, wireType 2 =*/ 66).string(message.docString);
      if (message.opsetImport != null && message.opsetImport.length)
        for (var i = 0; i < message.opsetImport.length; ++i)
          $root.onnx.OperatorSetIdProto.encode(
            message.opsetImport[i],
            writer.uint32(/* id 9, wireType 2 =*/ 74).fork(),
          ).ldelim();
      if (message.domain != null && Object.hasOwnProperty.call(message, 'domain'))
        writer.uint32(/* id 10, wireType 2 =*/ 82).string(message.domain);
      if (message.attributeProto != null && message.attributeProto.length)
        for (var i = 0; i < message.attributeProto.length; ++i)
          $root.onnx.AttributeProto.encode(
            message.attributeProto[i],
            writer.uint32(/* id 11, wireType 2 =*/ 90).fork(),
          ).ldelim();
      return writer;
    };

    /**
     * Encodes the specified FunctionProto message, length delimited. Does not implicitly {@link onnx.FunctionProto.verify|verify} messages.
     * @function encodeDelimited
     * @memberof onnx.FunctionProto
     * @static
     * @param {onnx.IFunctionProto} message FunctionProto message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    FunctionProto.encodeDelimited = function encodeDelimited(message, writer) {
      return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a FunctionProto message from the specified reader or buffer.
     * @function decode
     * @memberof onnx.FunctionProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {onnx.FunctionProto} FunctionProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    FunctionProto.decode = function decode(reader, length) {
      if (!(reader instanceof $Reader)) reader = $Reader.create(reader);
      var end = length === undefined ? reader.len : reader.pos + length,
        message = new $root.onnx.FunctionProto();
      while (reader.pos < end) {
        var tag = reader.uint32();
        switch (tag >>> 3) {
          case 1: {
            message.name = reader.string();
            break;
          }
          case 4: {
            if (!(message.input && message.input.length)) message.input = [];
            message.input.push(reader.string());
            break;
          }
          case 5: {
            if (!(message.output && message.output.length)) message.output = [];
            message.output.push(reader.string());
            break;
          }
          case 6: {
            if (!(message.attribute && message.attribute.length)) message.attribute = [];
            message.attribute.push(reader.string());
            break;
          }
          case 11: {
            if (!(message.attributeProto && message.attributeProto.length)) message.attributeProto = [];
            message.attributeProto.push($root.onnx.AttributeProto.decode(reader, reader.uint32()));
            break;
          }
          case 7: {
            if (!(message.node && message.node.length)) message.node = [];
            message.node.push($root.onnx.NodeProto.decode(reader, reader.uint32()));
            break;
          }
          case 8: {
            message.docString = reader.string();
            break;
          }
          case 9: {
            if (!(message.opsetImport && message.opsetImport.length)) message.opsetImport = [];
            message.opsetImport.push($root.onnx.OperatorSetIdProto.decode(reader, reader.uint32()));
            break;
          }
          case 10: {
            message.domain = reader.string();
            break;
          }
          default:
            reader.skipType(tag & 7);
            break;
        }
      }
      return message;
    };

    /**
     * Decodes a FunctionProto message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof onnx.FunctionProto
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {onnx.FunctionProto} FunctionProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    FunctionProto.decodeDelimited = function decodeDelimited(reader) {
      if (!(reader instanceof $Reader)) reader = new $Reader(reader);
      return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a FunctionProto message.
     * @function verify
     * @memberof onnx.FunctionProto
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    FunctionProto.verify = function verify(message) {
      if (typeof message !== 'object' || message === null) return 'object expected';
      if (message.name != null && message.hasOwnProperty('name'))
        if (!$util.isString(message.name)) return 'name: string expected';
      if (message.input != null && message.hasOwnProperty('input')) {
        if (!Array.isArray(message.input)) return 'input: array expected';
        for (var i = 0; i < message.input.length; ++i)
          if (!$util.isString(message.input[i])) return 'input: string[] expected';
      }
      if (message.output != null && message.hasOwnProperty('output')) {
        if (!Array.isArray(message.output)) return 'output: array expected';
        for (var i = 0; i < message.output.length; ++i)
          if (!$util.isString(message.output[i])) return 'output: string[] expected';
      }
      if (message.attribute != null && message.hasOwnProperty('attribute')) {
        if (!Array.isArray(message.attribute)) return 'attribute: array expected';
        for (var i = 0; i < message.attribute.length; ++i)
          if (!$util.isString(message.attribute[i])) return 'attribute: string[] expected';
      }
      if (message.attributeProto != null && message.hasOwnProperty('attributeProto')) {
        if (!Array.isArray(message.attributeProto)) return 'attributeProto: array expected';
        for (var i = 0; i < message.attributeProto.length; ++i) {
          var error = $root.onnx.AttributeProto.verify(message.attributeProto[i]);
          if (error) return 'attributeProto.' + error;
        }
      }
      if (message.node != null && message.hasOwnProperty('node')) {
        if (!Array.isArray(message.node)) return 'node: array expected';
        for (var i = 0; i < message.node.length; ++i) {
          var error = $root.onnx.NodeProto.verify(message.node[i]);
          if (error) return 'node.' + error;
        }
      }
      if (message.docString != null && message.hasOwnProperty('docString'))
        if (!$util.isString(message.docString)) return 'docString: string expected';
      if (message.opsetImport != null && message.hasOwnProperty('opsetImport')) {
        if (!Array.isArray(message.opsetImport)) return 'opsetImport: array expected';
        for (var i = 0; i < message.opsetImport.length; ++i) {
          var error = $root.onnx.OperatorSetIdProto.verify(message.opsetImport[i]);
          if (error) return 'opsetImport.' + error;
        }
      }
      if (message.domain != null && message.hasOwnProperty('domain'))
        if (!$util.isString(message.domain)) return 'domain: string expected';
      return null;
    };

    /**
     * Creates a FunctionProto message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof onnx.FunctionProto
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {onnx.FunctionProto} FunctionProto
     */
    FunctionProto.fromObject = function fromObject(object) {
      if (object instanceof $root.onnx.FunctionProto) return object;
      var message = new $root.onnx.FunctionProto();
      if (object.name != null) message.name = String(object.name);
      if (object.input) {
        if (!Array.isArray(object.input)) throw TypeError('.onnx.FunctionProto.input: array expected');
        message.input = [];
        for (var i = 0; i < object.input.length; ++i) message.input[i] = String(object.input[i]);
      }
      if (object.output) {
        if (!Array.isArray(object.output)) throw TypeError('.onnx.FunctionProto.output: array expected');
        message.output = [];
        for (var i = 0; i < object.output.length; ++i) message.output[i] = String(object.output[i]);
      }
      if (object.attribute) {
        if (!Array.isArray(object.attribute)) throw TypeError('.onnx.FunctionProto.attribute: array expected');
        message.attribute = [];
        for (var i = 0; i < object.attribute.length; ++i) message.attribute[i] = String(object.attribute[i]);
      }
      if (object.attributeProto) {
        if (!Array.isArray(object.attributeProto))
          throw TypeError('.onnx.FunctionProto.attributeProto: array expected');
        message.attributeProto = [];
        for (var i = 0; i < object.attributeProto.length; ++i) {
          if (typeof object.attributeProto[i] !== 'object')
            throw TypeError('.onnx.FunctionProto.attributeProto: object expected');
          message.attributeProto[i] = $root.onnx.AttributeProto.fromObject(object.attributeProto[i]);
        }
      }
      if (object.node) {
        if (!Array.isArray(object.node)) throw TypeError('.onnx.FunctionProto.node: array expected');
        message.node = [];
        for (var i = 0; i < object.node.length; ++i) {
          if (typeof object.node[i] !== 'object') throw TypeError('.onnx.FunctionProto.node: object expected');
          message.node[i] = $root.onnx.NodeProto.fromObject(object.node[i]);
        }
      }
      if (object.docString != null) message.docString = String(object.docString);
      if (object.opsetImport) {
        if (!Array.isArray(object.opsetImport)) throw TypeError('.onnx.FunctionProto.opsetImport: array expected');
        message.opsetImport = [];
        for (var i = 0; i < object.opsetImport.length; ++i) {
          if (typeof object.opsetImport[i] !== 'object')
            throw TypeError('.onnx.FunctionProto.opsetImport: object expected');
          message.opsetImport[i] = $root.onnx.OperatorSetIdProto.fromObject(object.opsetImport[i]);
        }
      }
      if (object.domain != null) message.domain = String(object.domain);
      return message;
    };

    /**
     * Creates a plain object from a FunctionProto message. Also converts values to other types if specified.
     * @function toObject
     * @memberof onnx.FunctionProto
     * @static
     * @param {onnx.FunctionProto} message FunctionProto
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    FunctionProto.toObject = function toObject(message, options) {
      if (!options) options = {};
      var object = {};
      if (options.arrays || options.defaults) {
        object.input = [];
        object.output = [];
        object.attribute = [];
        object.node = [];
        object.opsetImport = [];
        object.attributeProto = [];
      }
      if (options.defaults) {
        object.name = '';
        object.docString = '';
        object.domain = '';
      }
      if (message.name != null && message.hasOwnProperty('name')) object.name = message.name;
      if (message.input && message.input.length) {
        object.input = [];
        for (var j = 0; j < message.input.length; ++j) object.input[j] = message.input[j];
      }
      if (message.output && message.output.length) {
        object.output = [];
        for (var j = 0; j < message.output.length; ++j) object.output[j] = message.output[j];
      }
      if (message.attribute && message.attribute.length) {
        object.attribute = [];
        for (var j = 0; j < message.attribute.length; ++j) object.attribute[j] = message.attribute[j];
      }
      if (message.node && message.node.length) {
        object.node = [];
        for (var j = 0; j < message.node.length; ++j)
          object.node[j] = $root.onnx.NodeProto.toObject(message.node[j], options);
      }
      if (message.docString != null && message.hasOwnProperty('docString')) object.docString = message.docString;
      if (message.opsetImport && message.opsetImport.length) {
        object.opsetImport = [];
        for (var j = 0; j < message.opsetImport.length; ++j)
          object.opsetImport[j] = $root.onnx.OperatorSetIdProto.toObject(message.opsetImport[j], options);
      }
      if (message.domain != null && message.hasOwnProperty('domain')) object.domain = message.domain;
      if (message.attributeProto && message.attributeProto.length) {
        object.attributeProto = [];
        for (var j = 0; j < message.attributeProto.length; ++j)
          object.attributeProto[j] = $root.onnx.AttributeProto.toObject(message.attributeProto[j], options);
      }
      return object;
    };

    /**
     * Converts this FunctionProto to JSON.
     * @function toJSON
     * @memberof onnx.FunctionProto
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    FunctionProto.prototype.toJSON = function toJSON() {
      return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    /**
     * Gets the default type url for FunctionProto
     * @function getTypeUrl
     * @memberof onnx.FunctionProto
     * @static
     * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns {string} The default type url
     */
    FunctionProto.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
      if (typeUrlPrefix === undefined) {
        typeUrlPrefix = 'type.googleapis.com';
      }
      return typeUrlPrefix + '/onnx.FunctionProto';
    };

    return FunctionProto;
  })();

  return onnx;
})();

module.exports = $root;

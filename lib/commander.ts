import { defaults } from "./utils/lodash";
import Command from "./command";
import Script from "./script";
import * as PromiseContainer from "./promiseContainer";
import asCallback from "standard-as-callback";

export interface ICommanderOptions {
  showFriendlyErrorStack?: boolean;
}

const DROP_BUFFER_SUPPORT_ERROR =
  "*Buffer methods are not available " +
  'because "dropBufferSupport" option is enabled.' +
  "Refer to https://github.com/luin/ioredis/wiki/Improve-Performance for more details.";

/**
 * Commander
 *
 * This is the base class of Redis, Redis.Cluster and Pipeline
 *
 * @param {boolean} [options.showFriendlyErrorStack=false] - Whether to show a friendly error stack.
 * Will decrease the performance significantly.
 * @constructor
 */
export default function Commander() {
  this.options = defaults({}, this.options || {}, {
    showFriendlyErrorStack: false,
  });
  this.scriptsSet = {};
}

const commands = require("redis-commands").list.filter(function (command) {
  return command !== "monitor";
});
commands.push("sentinel");

/**
 * Return supported builtin commands
 *
 * @return {string[]} command list
 * @public
 */
Commander.prototype.getBuiltinCommands = function () {
  return commands.slice(0);
};

/**
 * Create a builtin command
 *
 * @param {string} commandName - command name
 * @return {object} functions
 * @public
 */
Commander.prototype.createBuiltinCommand = function (commandName) {
  return {
    string: generateFunction(commandName, "utf8"),
    buffer: generateFunction(commandName, null),
  };
};

commands.forEach(function (commandName) {
  Commander.prototype[commandName] = generateFunction(commandName, "utf8");
  Commander.prototype[commandName + "Buffer"] = generateFunction(
    commandName,
    null
  );
});

Commander.prototype.call = generateFunction("utf8");
Commander.prototype.callBuffer = generateFunction(null);
// eslint-disable-next-line @typescript-eslint/naming-convention
Commander.prototype.send_command = Commander.prototype.call;

/**
 * Define a custom command using lua script
 *
 * @param {string} name - the command name
 * @param {object} definition
 * @param {string} definition.lua - the lua code
 * @param {number} [definition.numberOfKeys=null] - the number of keys.
 * @param {boolean} [definition.readOnly=false] - force this script to be readonly so it executes on slaves as well.
 * If omit, you have to pass the number of keys as the first argument every time you invoke the command
 */
Commander.prototype.defineCommand = function (name, definition) {
  const script = new Script(
    definition.lua,
    definition.numberOfKeys,
    this.options.keyPrefix,
    definition.readOnly
  );
  this.scriptsSet[name] = script;
  this[name] = generateScriptingFunction(script, "utf8");
  this[name + "Buffer"] = generateScriptingFunction(script, null);
};

/**
 * Send a command
 *
 * @abstract
 * @public
 */
Commander.prototype.sendCommand = function () {};

function generateFunction(_encoding: string);
function generateFunction(_commandName: string | void, _encoding: string);
function generateFunction(_commandName?: string, _encoding?: string) {
  if (typeof _encoding === "undefined") {
    _encoding = _commandName;
    _commandName = null;
  }
  return function () {
    let firstArgIndex = 0;
    let commandName = _commandName;
    if (commandName === null) {
      commandName = arguments[0];
      firstArgIndex = 1;
    }
    let length = arguments.length;
    const lastArgIndex = length - 1;
    let callback = arguments[lastArgIndex];
    if (typeof callback !== "function") {
      callback = undefined;
    } else {
      length = lastArgIndex;
    }
    const args = new Array(length - firstArgIndex);
    for (let i = firstArgIndex; i < length; ++i) {
      args[i - firstArgIndex] = arguments[i];
    }

    let options;
    if (this.options.dropBufferSupport) {
      if (!_encoding) {
        return asCallback(
          PromiseContainer.get().reject(new Error(DROP_BUFFER_SUPPORT_ERROR)),
          callback
        );
      }
      options = { replyEncoding: null };
    } else {
      options = { replyEncoding: _encoding };
    }

    if (this.options.showFriendlyErrorStack) {
      options.errorStack = new Error().stack;
    }
    if (this.options.keyPrefix) {
      options.keyPrefix = this.options.keyPrefix;
    }

    return this.sendCommand(new Command(commandName, args, options, callback));
  };
}

function generateScriptingFunction(_script, _encoding) {
  return function () {
    let length = arguments.length;
    const lastArgIndex = length - 1;
    let callback = arguments[lastArgIndex];
    if (typeof callback !== "function") {
      callback = undefined;
    } else {
      length = lastArgIndex;
    }
    const args = new Array(length);
    for (let i = 0; i < length; i++) {
      args[i] = arguments[i];
    }

    let options;
    if (this.options.dropBufferSupport) {
      if (!_encoding) {
        return asCallback(
          PromiseContainer.get().reject(new Error(DROP_BUFFER_SUPPORT_ERROR)),
          callback
        );
      }
      options = { replyEncoding: null };
    } else {
      options = { replyEncoding: _encoding };
    }

    if (this.options.showFriendlyErrorStack) {
      options.errorStack = new Error().stack;
    }

    return _script.execute(this, args, options, callback);
  };
}

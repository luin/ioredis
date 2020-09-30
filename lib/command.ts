import * as commands from "redis-commands";
import * as calculateSlot from "cluster-key-slot";
import asCallback from "standard-as-callback";
import {
  convertBufferToString,
  optimizeErrorStack,
  toArg,
  convertMapToArray,
  convertObjectToArray,
} from "./utils";
import { flatten } from "./utils/lodash";
import { get as getPromise } from "./promiseContainer";
import { CallbackFunction, ICommand, CommandParameter } from "./types";

interface ICommandOptions {
  /**
   * Set the encoding of the reply, by default buffer will be returned.
   *
   * @type {(string | null)}
   * @memberof ICommandOptions
   */
  replyEncoding?: string | null;
  errorStack?: string;
  keyPrefix?: string;
  /**
   * Force the command to be readOnly so it will also execute on slaves
   */
  readOnly?: boolean;
}

type ArgumentTransformer = (args: any[]) => any[];
type ReplyTransformer = (reply: any) => any;
interface IFlagMap {
  [flag: string]: { [command: string]: true };
}

export interface ICommandNameFlags {
  // Commands that can be processed when client is in the subscriber mode
  VALID_IN_SUBSCRIBER_MODE: [
    "subscribe",
    "psubscribe",
    "unsubscribe",
    "punsubscribe",
    "ping",
    "quit"
  ];
  // Commands that are valid in monitor mode
  VALID_IN_MONITOR_MODE: ["monitor", "auth"];
  // Commands that will turn current connection into subscriber mode
  ENTER_SUBSCRIBER_MODE: ["subscribe", "psubscribe"];
  // Commands that may make current connection quit subscriber mode
  EXIT_SUBSCRIBER_MODE: ["unsubscribe", "punsubscribe"];
  // Commands that will make client disconnect from server TODO shutdown?
  WILL_DISCONNECT: ["quit"];
}

/**
 * Command instance
 *
 * It's rare that you need to create a Command instance yourself.
 *
 * @export
 * @class Command
 *
 * @example
 * ```js
 * var infoCommand = new Command('info', null, function (err, result) {
 *   console.log('result', result);
 * });
 *
 * redis.sendCommand(infoCommand);
 *
 * // When no callback provided, Command instance will have a `promise` property,
 * // which will resolve/reject with the result of the command.
 * var getCommand = new Command('get', ['foo']);
 * getCommand.promise.then(function (result) {
 *   console.log('result', result);
 * });
 * ```
 * @see {@link Redis#sendCommand} which can send a Command instance to Redis
 */
export default class Command implements ICommand {
  public static FLAGS: {
    [key in keyof ICommandNameFlags]: ICommandNameFlags[key];
  } = {
    VALID_IN_SUBSCRIBER_MODE: [
      "subscribe",
      "psubscribe",
      "unsubscribe",
      "punsubscribe",
      "ping",
      "quit",
    ],
    VALID_IN_MONITOR_MODE: ["monitor", "auth"],
    ENTER_SUBSCRIBER_MODE: ["subscribe", "psubscribe"],
    EXIT_SUBSCRIBER_MODE: ["unsubscribe", "punsubscribe"],
    WILL_DISCONNECT: ["quit"],
  };

  private static flagMap?: IFlagMap;

  private static getFlagMap(): IFlagMap {
    if (!this.flagMap) {
      this.flagMap = Object.keys(Command.FLAGS).reduce((map, flagName) => {
        map[flagName] = {};
        Command.FLAGS[flagName].forEach((commandName) => {
          map[flagName][commandName] = true;
        });
        return map;
      }, {});
    }
    return this.flagMap;
  }

  /**
   * Check whether the command has the flag
   *
   * @param {string} flagName
   * @param {string} commandName
   * @return {boolean}
   */
  public static checkFlag<T extends keyof ICommandNameFlags>(
    flagName: T,
    commandName: string
  ): commandName is ICommandNameFlags[T][number] {
    return !!this.getFlagMap()[flagName][commandName];
  }

  private static _transformer: {
    argument: { [command: string]: ArgumentTransformer };
    reply: { [command: string]: ReplyTransformer };
  } = {
    argument: {},
    reply: {},
  };

  public static setArgumentTransformer(
    name: string,
    func: ArgumentTransformer
  ): void {
    this._transformer.argument[name] = func;
  }

  public static setReplyTransformer(
    name: string,
    func: ReplyTransformer
  ): void {
    this._transformer.reply[name] = func;
  }

  public ignore?: boolean;
  public isReadOnly?: boolean;

  private replyEncoding: string | null;
  private errorStack: string;
  public args: CommandParameter[];
  private callback: CallbackFunction;
  private transformed = false;
  public isCustomCommand = false;
  public inTransaction = false;
  public pipelineIndex?: number;

  private slot?: number | null;
  private keys?: Array<string | Buffer>;

  public reject: (err: Error) => void;
  public resolve: (result: any) => void;
  public promise: Promise<any>;

  /**
   * Creates an instance of Command.
   * @param {string} name Command name
   * @param {(Array<string | Buffer | number>)} [args=[]] An array of command arguments
   * @param {ICommandOptions} [options={}]
   * @param {CallbackFunction} [callback] The callback that handles the response.
   * If omit, the response will be handled via Promise
   * @memberof Command
   */
  constructor(
    public name: string,
    args: Array<
      string | Buffer | number | Array<string | Buffer | number | any[]>
    > = [],
    options: ICommandOptions = {},
    callback?: CallbackFunction
  ) {
    this.replyEncoding = options.replyEncoding;
    this.errorStack = options.errorStack;

    this.args = flatten(args);
    this.callback = callback;

    this.initPromise();

    if (options.keyPrefix) {
      this._iterateKeys((key) => options.keyPrefix + key);
    }

    if (options.readOnly) {
      this.isReadOnly = true;
    }
  }

  private initPromise() {
    const Promise = getPromise();
    const promise = new Promise((resolve, reject) => {
      if (!this.transformed) {
        this.transformed = true;
        const transformer = Command._transformer.argument[this.name];
        if (transformer) {
          this.args = transformer(this.args);
        }
        this.stringifyArguments();
      }

      this.resolve = this._convertValue(resolve);
      if (this.errorStack) {
        this.reject = (err) => {
          reject(optimizeErrorStack(err, this.errorStack, __dirname));
        };
      } else {
        this.reject = reject;
      }
    });

    this.promise = asCallback(promise, this.callback);
  }

  public getSlot() {
    if (typeof this.slot === "undefined") {
      const key = this.getKeys()[0];
      this.slot = key == null ? null : calculateSlot(key);
    }
    return this.slot;
  }

  public getKeys(): Array<string | Buffer> {
    return this._iterateKeys();
  }

  /**
   * Iterate through the command arguments that are considered keys.
   *
   * @param {Function} [transform=(key) => key] The transformation that should be applied to
   * each key. The transformations will persist.
   * @returns {string[]} The keys of the command.
   * @memberof Command
   */
  private _iterateKeys(
    transform: (any) => any = (key) => key
  ): Array<string | Buffer> {
    if (typeof this.keys === "undefined") {
      this.keys = [];
      if (commands.exists(this.name)) {
        const keyIndexes = commands.getKeyIndexes(this.name, this.args);
        for (const index of keyIndexes) {
          this.args[index] = transform(this.args[index]);
          this.keys.push(this.args[index] as string | Buffer);
        }
      }
    }
    return this.keys;
  }

  /**
   * Convert command to writable buffer or string
   *
   * @return {string|Buffer}
   * @see {@link Redis#sendCommand}
   * @public
   */
  public toWritable(): string | Buffer {
    let bufferMode = false;
    for (const arg of this.args) {
      if (arg instanceof Buffer) {
        bufferMode = true;
        break;
      }
    }

    let result;
    const commandStr =
      "*" +
      (this.args.length + 1) +
      "\r\n$" +
      Buffer.byteLength(this.name) +
      "\r\n" +
      this.name +
      "\r\n";
    if (bufferMode) {
      const buffers = new MixedBuffers();
      buffers.push(commandStr);
      for (const arg of this.args) {
        if (arg instanceof Buffer) {
          if (arg.length === 0) {
            buffers.push("$0\r\n\r\n");
          } else {
            buffers.push("$" + arg.length + "\r\n");
            buffers.push(arg);
            buffers.push("\r\n");
          }
        } else {
          buffers.push(
            "$" +
              Buffer.byteLength(arg as string | Buffer) +
              "\r\n" +
              arg +
              "\r\n"
          );
        }
      }
      result = buffers.toBuffer();
    } else {
      result = commandStr;
      for (const arg of this.args) {
        result +=
          "$" +
          Buffer.byteLength(arg as string | Buffer) +
          "\r\n" +
          arg +
          "\r\n";
      }
    }
    return result;
  }

  stringifyArguments(): void {
    for (let i = 0; i < this.args.length; ++i) {
      const arg = this.args[i];
      if (!(arg instanceof Buffer) && typeof arg !== "string") {
        this.args[i] = toArg(arg);
      }
    }
  }

  /**
   * Convert the value from buffer to the target encoding.
   *
   * @private
   * @param {Function} resolve The resolve function of the Promise
   * @returns {Function} A funtion to transform and resolve a value
   * @memberof Command
   */
  // eslint-disable-next-line @typescript-eslint/ban-types
  private _convertValue(resolve: Function): (result: any) => void {
    return (value) => {
      try {
        resolve(this.transformReply(value));
      } catch (err) {
        this.reject(err);
      }
      return this.promise;
    };
  }

  /**
   * Convert buffer/buffer[] to string/string[],
   * and apply reply transformer.
   *
   * @memberof Command
   */
  public transformReply(
    result: Buffer | Buffer[]
  ): string | string[] | Buffer | Buffer[] {
    if (this.replyEncoding) {
      result = convertBufferToString(result, this.replyEncoding);
    }
    const transformer = Command._transformer.reply[this.name];
    if (transformer) {
      result = transformer(result);
    }

    return result;
  }
}

const msetArgumentTransformer = function (args) {
  if (args.length === 1) {
    if (typeof Map !== "undefined" && args[0] instanceof Map) {
      return convertMapToArray(args[0]);
    }
    if (typeof args[0] === "object" && args[0] !== null) {
      return convertObjectToArray(args[0]);
    }
  }
  return args;
};

Command.setArgumentTransformer("mset", msetArgumentTransformer);
Command.setArgumentTransformer("msetnx", msetArgumentTransformer);

Command.setArgumentTransformer("hmset", function (args) {
  if (args.length === 2) {
    if (typeof Map !== "undefined" && args[1] instanceof Map) {
      return [args[0]].concat(convertMapToArray(args[1]));
    }
    if (typeof args[1] === "object" && args[1] !== null) {
      return [args[0]].concat(convertObjectToArray(args[1]));
    }
  }
  return args;
});

Command.setReplyTransformer("hgetall", function (result) {
  if (Array.isArray(result)) {
    const obj = {};
    for (let i = 0; i < result.length; i += 2) {
      obj[result[i]] = result[i + 1];
    }
    return obj;
  }
  return result;
});

Command.setArgumentTransformer("hset", function (args) {
  if (args.length === 2) {
    if (typeof Map !== "undefined" && args[1] instanceof Map) {
      return [args[0]].concat(convertMapToArray(args[1]));
    }
    if (typeof args[1] === "object" && args[1] !== null) {
      return [args[0]].concat(convertObjectToArray(args[1]));
    }
  }
  return args;
});

class MixedBuffers {
  length = 0;
  items = [];

  public push(x: string | Buffer) {
    this.length += Buffer.byteLength(x);
    this.items.push(x);
  }

  public toBuffer(): Buffer {
    const result = Buffer.allocUnsafe(this.length);
    let offset = 0;
    for (const item of this.items) {
      const length = Buffer.byteLength(item);
      Buffer.isBuffer(item)
        ? (item as Buffer).copy(result, offset)
        : result.write(item, offset, length);
      offset += length;
    }
    return result;
  }
}

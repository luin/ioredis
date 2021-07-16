exports = module.exports = require("./redis").default;

export { default } from "./redis";
export { default as Cluster } from "./cluster";
export { default as Command } from "./command";
export { default as ScanStream } from "./ScanStream";
export { default as RedisReadable } from "./streams/RedisReadable";
export { default as RedisWritable } from "./streams/RedisWritable";
export { default as Pipeline } from "./pipeline";
export { default as AbstractConnector } from "./connectors/AbstractConnector";
export {
  default as SentinelConnector,
  SentinelIterator,
} from "./connectors/SentinelConnector";

// Type Exports
export { ISentinelAddress } from "./connectors/SentinelConnector";
export { IRedisOptions } from "./redis/RedisOptions";

// No TS typings
export const ReplyError = require("redis-errors").ReplyError;

const PromiseContainer = require("./promiseContainer");
Object.defineProperty(exports, "Promise", {
  get() {
    return PromiseContainer.get();
  },
  set(lib) {
    PromiseContainer.set(lib);
  },
});

export function print(err: Error | null, reply?: any) {
  if (err) {
    console.log("Error: " + err);
  } else {
    console.log("Reply: " + reply);
  }
}

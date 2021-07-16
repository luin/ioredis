import { Writable, WritableOptions } from "stream";

/**
 * Options for RedisWritable
 *
 * @export
 * @interface IRedisWritableOptions
 * @extends {WritableOptions}
 */
interface IRedisWritableOptions extends WritableOptions {
  redis: any;

  key: string;

  /**
   * The ttl of the entry in Redis in seconds.
   * @default 900
   */
  ttl?: number;

  /**
   * The ttl of the temp Key in seconds.
   * @default 900
   */
  ttlTempKey?: number;
}

/**
 * Convenient class to use Redis Streams introduced by Redis 5.0.0
 *
 * @see https://redis.io/topics/streams-intro
 *
 * @export
 * @class RedisWritable
 * @extends {Writable}
 */
export default class RedisWritable extends Writable {
  private hasData: boolean;

  constructor(private opts = ({} as unknown) as IRedisWritableOptions) {
    super(opts);

    if (!this.opts.redis) {
      throw new Error(
        "Failed to instantiate RedisWritable. The field 'redis' was not provided as option."
      );
    }

    if (!this.opts.key) {
      throw new Error(
        "Failed to instantiate RedisWritable. The field 'key' was not provided as option."
      );
    }

    if (this.opts.objectMode) {
      throw new Error("RedisWritable does not support objectMode.");
    }

    this.opts.ttl = this.opts.ttl ?? 15 * 60; // default is 15 minutes
    this.opts.ttlTempKey = this.opts.ttlTempKey ?? 15 * 60; // default is 15 minutes
  }

  public async ensureWriteLock(): Promise<void> {
    if (!this.hasData) {
      const tempKeyExists = await this.opts.redis.exists(
        `${this.opts.key}_temp`,
        this.opts.key
      );

      if (tempKeyExists !== 0) {
        throw new Error(
          `Error while writing to Redis Stream. Key: '${this.opts.key}' is already in use.`
        );
      }
    }
  }

  _write(
    chunk: any,
    encoding: string,
    callback: (error?: Error | null) => void
  ): void {
    this.ensureWriteLock()
      .then(() => {
        const tmpKey = `${this.opts.key}_temp`;

        const xaddBufferArguments =
          typeof this.opts.highWaterMark === "number"
            ? [tmpKey, "MAXLEN", this.opts.highWaterMark, "*", null, chunk]
            : [tmpKey, "*", null, chunk];

        this.opts.redis.xaddBuffer
          .apply(this.opts.redis, xaddBufferArguments)
          .catch((err) => callback(err))
          .then(() => {
            if (!this.hasData) {
              this.opts.redis
                .expire(tmpKey, this.opts.ttlTempKey)
                .then(() => {
                  this.hasData = true;
                  callback(null);
                })
                .catch((err) => callback(err));
            } else {
              callback(null);
            }
          });
      })
      .catch((err) => callback(err));
  }

  _final(callback: (error?: Error | null) => void): void {
    if (!this.hasData) {
      return callback();
    }

    this.opts.redis
      .rename(`${this.opts.key}_temp`, this.opts.key)
      .then(() => {
        if (this.opts.ttl) {
          this.opts.redis
            .expire(this.opts.key, this.opts.ttl)
            .then(() => callback(null))
            .catch((err) => callback(err));
        } else {
          this.opts.redis
            .persist(this.opts.key)
            .then(() => callback(null))
            .catch((err) => callback(err));
        }
      })
      .catch((err) => callback(err));
  }

  _destroy(err, callback: (error?: Error | null) => void): void {
    this.opts.redis.unlink(`${this.opts.key}_temp`).catch((err) => {});
    callback(err);
  }
}

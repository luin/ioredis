import { Readable, ReadableOptions } from "stream";

/**
 * Options for RedisReadable
 *
 * @export
 * @interface RedisReadable
 * @extends {ReadableOptions}
 */
interface IRedisReadableOptions extends ReadableOptions {
  redis: any;

  key: string;

  /**
   * The minimum ttl of the key.
   *
   * RedisReadable will increase the ttl if the current ttl of the key is
   * lower than the given value.
   *
   * @default 900
   */
  minimumReadTTL?: number;
}

/**
 * Convenient class to use Redis Streams introduced by Redis 5.0.0
 *
 * @see https://redis.io/topics/streams-intro
 *
 * @export
 * @class RedisReadable
 * @extends {Readable}
 */
export default class RedisReadable extends Readable {
  private chunkSize: number;

  private next: string;

  constructor(private opts = ({} as unknown) as IRedisReadableOptions) {
    super(opts);

    if (!this.opts.redis) {
      throw new Error(
        "Failed to instantiate RedisReadable. The field 'redis' was not provided as option."
      );
    }

    if (!this.opts.key) {
      throw new Error(
        "Failed to instantiate RedisReadable. The field 'key' was not provided as option."
      );
    }

    if (this.opts.objectMode) {
      throw new Error("RedisReadable does not support objectMode.");
    }

    this.next = "-";

    this.ensureTTL(this.opts.minimumReadTTL).catch((reason) =>
      this.emit("error", reason)
    );
  }

  _read(size: number): void {
    const xrangeCount = Math.max(1, Math.ceil(size / this.chunkSize)) || 1;

    const xrangeBufferArguments = this.chunkSize
      ? [this.opts.key, this.next, "+", "COUNT", xrangeCount]
      : [this.opts.key, "-", "+", "COUNT", 1];

    this.opts.redis.xrangeBuffer
      .apply(this.opts.redis, xrangeBufferArguments)
      .then((chunks: [Buffer, [Buffer, Buffer]][]) => {
        if (chunks.length === 0) {
          this.push(null);
          return;
        }

        if (!this.chunkSize) {
          this.chunkSize = chunks[0][1][1].length;
        }

        this.next = this.getNext(chunks);

        this.push(Buffer.concat(chunks.map((res) => res[1][1])));
      });
  }

  _destroy(err, callback: (error?: Error | null) => void): void {
    this.opts.autoDestroy
      ? this.opts.redis
          .unlink(this.opts.key)
          .catch((err) => callback(err))
          .then(() => callback(err))
      : callback(err);
  }

  async ensureTTL(minimumTTL = 15 * 60): Promise<void> {
    const ttl = await this.opts.redis.ttl(this.opts.key);

    if (ttl !== -1 && ttl < minimumTTL) {
      await this.opts.redis.expire(this.opts.key, minimumTTL);
    }
  }

  getNext(chunks: [Buffer, [Buffer, Buffer]][]): string {
    const [id, iterator] = chunks[chunks.length - 1][0]
      .toString("binary")
      .split("-") as [string, string];
    return `${id}-${Number(iterator) + 1}`;
  }
}

import { Debug } from "../utils";
import Deque = require("denque");

const debug = Debug("delayqueue");

/**
 * Options for DelayQueue
 *
 * @export
 * @interface IDelayQueueOptions
 */
export interface IDelayQueueOptions {
  // eslint-disable-next-line @typescript-eslint/ban-types
  callback?: Function;
  timeout: number;
}

/**
 * Queue that runs items after specified duration
 *
 * @export
 * @class DelayQueue
 */
export default class DelayQueue {
  private queues: { [key: string]: any | null } = {};
  private timeouts: { [key: string]: NodeJS.Timer } = {};

  /**
   * Add a new item to the queue
   *
   * @param {string} bucket bucket name
   * @param {Function} item function that will run later
   * @param {IDelayQueueOptions} options
   * @memberof DelayQueue
   */
  public push(
    bucket: string,
    // eslint-disable-next-line @typescript-eslint/ban-types
    item: Function,
    options: IDelayQueueOptions
  ): void {
    const callback = options.callback || process.nextTick;
    if (!this.queues[bucket]) {
      this.queues[bucket] = new Deque();
    }

    const queue = this.queues[bucket];
    queue.push(item);

    if (!this.timeouts[bucket]) {
      this.timeouts[bucket] = setTimeout(() => {
        callback(() => {
          this.timeouts[bucket] = null;
          this.execute(bucket);
        });
      }, options.timeout);
    }
  }

  private execute(bucket: string): void {
    const queue = this.queues[bucket];
    if (!queue) {
      return;
    }
    const { length } = queue;
    if (!length) {
      return;
    }
    debug("send %d commands in %s queue", length, bucket);

    this.queues[bucket] = null;
    while (queue.length > 0) {
      queue.shift()();
    }
  }
}

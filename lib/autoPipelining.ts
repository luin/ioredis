import * as PromiseContainer from "./promiseContainer";
import * as calculateSlot from "cluster-key-slot";
import asCallback from "standard-as-callback";

export const kExec = Symbol("exec");
export const kCallbacks = Symbol("callbacks");
export const notAllowedAutoPipelineCommands = [
  "auth",
  "info",
  "script",
  "quit",
  "cluster",
  "pipeline",
  "multi",
  "subscribe",
  "psubscribe",
  "unsubscribe",
  "unpsubscribe",
];

function findAutoPipeline(
  client,
  _commandName,
  ...args: Array<string>
): string {
  if (!client.isCluster) {
    return "main";
  }

  // We have slot information, we can improve routing by grouping slots served by the same subset of nodes
  const prefixedKey = client.options.keyPrefix
    ? `${client.options.keyPrefix}${args[0]}`
    : args[0];
  return client.slots[calculateSlot(prefixedKey)].join(",");
}

function executeAutoPipeline(client, slotKey: string) {
  /*
    If a pipeline is already executing, keep queueing up commands
    since ioredis won't serve two pipelines at the same time
  */
  if (client._runningAutoPipelines.has(slotKey)) {
    return;
  }

  client._runningAutoPipelines.add(slotKey);

  // Get the pipeline and immediately delete it so that new commands are queued on a new pipeline
  const pipeline = client._autoPipelines.get(slotKey);
  client._autoPipelines.delete(slotKey);

  const callbacks = pipeline[kCallbacks];

  // Perform the call
  pipeline.exec(function (err, results) {
    client._runningAutoPipelines.delete(slotKey);

    /*
      Invoke all callback in nextTick so the stack is cleared 
      and callbacks can throw errors without affecting other callbacks.
    */
    if (err) {
      for (let i = 0; i < callbacks.length; i++) {
        process.nextTick(callbacks[i], err);
      }
    } else {
      for (let i = 0; i < callbacks.length; i++) {
        process.nextTick(callbacks[i], ...results[i]);
      }
    }

    // If there is another pipeline on the same node, immediately execute it without waiting for nextTick
    if (client._autoPipelines.has(slotKey)) {
      executeAutoPipeline(client, slotKey);
    }
  });
}

export function shouldUseAutoPipelining(
  client,
  functionName: string,
  commandName: string
): boolean {
  return (
    functionName &&
    client.options.enableAutoPipelining &&
    !client.isPipeline &&
    !notAllowedAutoPipelineCommands.includes(commandName) &&
    !client.options.autoPipeliningIgnoredCommands.includes(commandName)
  );
}

export function executeWithAutoPipelining(
  client,
  functionName: string,
  commandName: string,
  args: string[],
  callback
) {
  const CustomPromise = PromiseContainer.get();

  // On cluster mode let's wait for slots to be available
  if (client.isCluster && !client.slots.length) {
    return new CustomPromise(function (resolve, reject) {
      client.delayUntilReady((err) => {
        if (err) {
          reject(err);
          return;
        }

        executeWithAutoPipelining(
          client,
          functionName,
          commandName,
          args,
          callback
        ).then(resolve, reject);
      });
    });
  }

  const slotKey = findAutoPipeline(client, commandName, ...args);

  if (!client._autoPipelines.has(slotKey)) {
    const pipeline = client.pipeline();
    pipeline[kExec] = false;
    pipeline[kCallbacks] = [];
    client._autoPipelines.set(slotKey, pipeline);
  }

  const pipeline = client._autoPipelines.get(slotKey);

  /* 
    Mark the pipeline as scheduled.
    The symbol will make sure that the pipeline is only scheduled once per tick.
    New commands are appended to an already scheduled pipeline.
  */
  if (!pipeline[kExec]) {
    pipeline[kExec] = true;
    /*
      Deferring with setImmediate so we have a chance to capture multiple
      commands that can be scheduled by I/O events already in the event loop queue.
    */
    setImmediate(executeAutoPipeline, client, slotKey);
  }

  // Create the promise which will execute the
  const autoPipelinePromise = new CustomPromise(function (resolve, reject) {
    pipeline[kCallbacks].push(function (err, value) {
      if (err) {
        reject(err);
        return;
      }

      resolve(value);
    });

    pipeline[functionName](...args);
  });

  return asCallback(autoPipelinePromise, callback);
}

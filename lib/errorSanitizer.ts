type RedisError = Error & {
  command?: Record<string, any>;
};

function clone(instance) {
  return Object.assign(
    Object.create(Object.getPrototypeOf(instance)),
    instance
  );
}

export function sanitizeError(originalError: RedisError): RedisError {
  if (!originalError?.command?.args) {
    return originalError;
  }
  const error = clone(originalError);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { args, ...commandWithoutArgs } = error.command;
  error.command = commandWithoutArgs;

  return error;
}

let warned = false;
export function warnOnceAboutSanitizeErrors() {
  if (warned) return;
  warned = true;
  console.warn(
    "[WARN] In the future, command args will automatically be sanitized away from error objects. To keep the current behavior, set sanitizeErrors: false when configuring ioredis"
  );
}

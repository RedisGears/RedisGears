/**
 * Client object that is used to perform operation on Redis.
 */
export interface NativeClient {
    /**
     * Run a command on Redis. The command is executed on the current Redis instance.
     * @param args - The command to execute. 
     */
    call<T = unknown>(...args: Array<string | ArrayBuffer>): T;

    /**
     * Return true if it is allow to return promise from the function callback.
     * In case it is allowed and a promise is return, Redis will wait for the promise
     * to be fulfilled and only then will return the function result.
     */
    isBlockAllowed(): boolean;

    /**
     * Execute the given function asynchroniusly. Return a promise that will be fulfilled
     * when the promise will be resolved/rejected.
     * @param fn 
     */
    executeAsync(fn: (asyncClient: NativeAsyncClient) => any): Promise<any>;
}

/**
 * Background client object that is used to perform background operation on Redis.
 * This client is given to any background task that runs as a JS coroutine.
 */
export interface NativeAsyncClient {
    /**
     * Blocks Redis for command invocation.
     * All the command that are executed inside the given
     * function is considered atomic and will be wrapped with `Multi/Exec`
     * and send to the replica/AOF.
     * @param fn - the function to execute atomically.
     */
    block<T>(fn: (client: NativeClient) => T): T;

    /**
     * Runs a remote function on a given key. If the key located on the current
     * shard on which we currently runs on, the remote function will run write away.
     * Otherwise the remote function will run on the remote shard. Returns a promise
     * which will be fulfilled when the invocation finishes. Notice that the remote
     * function must return a json serializable result so we can serialize the result back
     * to the original shard.
     * 
     * Notice that remote function can only perform read operations, not writes are allowed.
     * 
     * @param key - The key on which to run the remote function on.
     * @param remoteFunction - The remote function name to run
     * @param args - Extra arguments to give to the remote function (must be json serializabale).
     */
    runOnKey(key: string, remoteFunction: string, ...args: Array<string | object>): Promise<any>

    /**
     * Runs a remote function on all the shards. Returns a promise
     * which will be fulfilled when the invocation finishes on all the shards.
     * 
     * The result is array of 2 elements, the first is another array of all the results.
     * The second is an array of all the errors happened durring the invocation. Notice that
     * the remote function must return a json serializable result so we can serialize the result back
     * to the original shard.
     * 
     * Notice that remote function can only perform read operations, not writes are allowed.
     * 
     * @param remoteFunction - The remote function name to run
     * @param args - Extra arguments to give to the remote function (must be json serializabale).
     */
    runOnShards(remoteFunction: string, ...args: Array<string | object>): Promise<any>
}

/**
 * Options object for functions. Must be of the following format:
 * 
 * ```js
 * {
 *      flags: [...],
 *      description: "short description"
 * }
 * ```
 * 
 * `flags`: array of flags of the following values:
 * * `redis.functionFlags.NO_WRITES` - indicate that the function performs not write operations.
 * * `redis.functionFlags.ALLOW_OOM` - allow bypass the OOM limitation.
 * * `redis.functionFlags.RAW_ARGUMENTS` - do not decode function arguments.
 * 
 * `description`: short description of what the function is doing.
 */
export interface FunctionOptions {
    flags: Array<[string, string]>;
    description: string;
}

/**
 * Options object for stream trigger. Must be of the following format:
 * 
 * ```js
 * {
 *      window: 1,
 *      description: "short description",
 *      isStreamTrimmed: true
 * }
 * ```
 * 
 * `window`: How many elements to process in parallel.
 * 
 * `description`: short description of what the function is doing.
 * 
 * `isStreamTrimmed`: whether or not to trim the stream.
 */
export interface StreamTriggerOptions {
    description: string;
    window: number;
    isStreamTrimmed: boolean;
}

/**
 * Options object for key space trigger. Must be of the following format:
 * 
 * ```js
 * {
 *      description: "short description",
 *      onTriggerFired: ()=>{}
 * }
 * ```
 * 
 * `description`: short description of what the function is doing.
 * 
 * `onTriggerFired`: a callback that will be called directly when the key space
 * notication happened and allow to read the data as it was at the time of the
 * notification. The `data` object can be extended with additional data that will
 * be later provided to the actual key space notification callback.
 */
export interface KeySpaceTriggerOptions {
    description: string;
    onTriggerFired: (client: NativeClient, data: NotificationsConsumerData) => void;
}

/**
 * Object that is given to a stream trigger callback contains information about the stream record.
 * 
 * `id`: Record ID.
 * 
 * `stream_name`: The name of the stream (if the name is not a valid utf8, this value will be `null`).
 * 
 * `stream_name_raw`: The name of the stream as ArrayBuffer.
 * 
 * `record`: Array of tuples, each tuple is a key-value entery of the record data decoded to utf8 or null if failed to decode.
 * 
 * `record_raw`: Array of tuples, each tuple is a key-value entery of the record data as ArrayBuffer
 */
export interface StreamConsumerData {
    id: [ms: `${number}`, seq: `${number}`];
    stream_name: string;
    stream_name_raw: ArrayBuffer;
    record: Array<[string, string]>;
    record_raw: Array<[ArrayBuffer, ArrayBuffer]>;
}

/**
 * Object that is given to a key space trigger callback contains information about the fired key space notification.
 * 
 * `event`: Name of the event that caused the notification (For more information see: https://redis.io/docs/manual/keyspace-notifications/).
 * 
 * `key`: The key on which the notification was fired on, decoded as UTF8 or null if the decoding failed.
 * 
 * `key_raw`: The key on which the notification was fired on as ArrayBuffer.
 */
export interface NotificationsConsumerData {
    event: string;
    key: string;
    key_raw: ArrayBuffer;
}

/**
 * Redis globals object
 */
export const redis: {
    /**
     * Can only be called on library load time.
     * Register a new function that can later be invoke using `TFCALL` command (https://github.com/RedisGears/RedisGears/blob/master/docs/commands.md#tfcall).
     * 
     * @param name - the name of the function, can later be used to invoke the function with `TFCALL`
     * @param fn - the function callback.
     * @param options - extra options to control the function invocation.
     */
    registerFunction(name: string, fn: (client: NativeClient, ...args: Array<string> | Array<ArrayBuffer>) => any, options: FunctionOptions): void;

    /**
     * Can only be called on library load time.
     * Register a new async function that can later be invoke using `TFCALLASYNC` command (https://github.com/RedisGears/RedisGears/blob/master/docs/commands.md#tfcallasync)
     * For more information about sync and async function refer to: https://github.com/RedisGears/RedisGears/blob/master/docs/sync_and_async_run.md
     * 
     * @param name - the name of the function, can later be used to invoke the function with `TFCALLASYNC`
     * @param fn - the async function callback
     * @param options - extra options to control the function invocation.
     */
    registerAsyncFunction(name: string, fn: (asyncClient: NativeAsyncClient) => any, options: FunctionOptions): void;

    /**
     * Can only be called on library load time.
     * Register a stream trigger that will be invoke whenever a data is added to a stream.
     * For more information refer to: https://github.com/RedisGears/RedisGears/blob/master/docs/stream_processing.md
     * 
     * @param name - the name of the trigger.
     * @param prefix - prefix of streams names to register on.
     * @param fn - the stream trigger callback.
     * @param options - extra options to control stream processing.
     */
    registerStreamTrigger(name: string, prefix: string, fn: (client: NativeClient, data: StreamConsumerData) => any, options: StreamTriggerOptions): void;

    /**
     * Can only be called on library load time.
     * Register a key space notification trigger that will run whenever a key space notification fired.
     * For more information refer to: https://github.com/RedisGears/RedisGears/blob/master/docs/databse_triggers.md
     * 
     * @param name - the name of the trigger.
     * @param prefix - prefix of keys to fire on.
     * @param fn - the key space trigger callback.
     * @param options - extra options to control trigger processing.
     */
    registerKeySpaceTrigger(name: string, prefix: string, fn: (client: NativeClient, data: NotificationsConsumerData) => any, options: KeySpaceTriggerOptions): any;

    /**
     * Can only be called on library load time.
     * Register a cluster function that can later be called using `NativeAsyncClient::runOnKey` or `NativeAsyncClient::runOnShards`.
     * For more information refer to: https://github.com/RedisGears/RedisGears/blob/master/docs/cluster_support.md
     * 
     * @param name - the name of the cluster function.
     * @param fn - the cluster function callback.
     */
    registerClusterFunction(name: string, fn: (client: NativeClient, data: NotificationsConsumerData) => any): any;

    /**
     * The V8 version.
     */
    v8Version: String;

    /**
     * Write a message to the Redis log file.
     * 
     * @param msg - the message to write to the Redis log file.
     */
    log(msg: String);
};

export { };

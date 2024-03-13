/**
 * The object is going to be defined by the RedisGears module
 * implementation.
 */
var redis = redis;

/**
 * Notification events are filtered by their type (string events, set
 * events, etc), and the subscriber callback receives only events that
 * match a specific mask of event types.
 *
 * When subscribing to notifications with `registerKeySpaceTrigger`, the
 * module must provide an event type-mask, denoting the events the
 * subscriber is interested in. This can be an ORed mask of any of the
 * following flags.
 */
const EventNotificationFlags = {
    /**
     * Generic commands like DEL, EXPIRE, RENAME
     */
    GENERIC: 'GENERIC',
    /**
     * String events.
     */
    STRING: 'STRING',
    /**
     * List events.
     */
    LIST: 'LIST',
    /**
     * Set events.
     */
    SET: 'SET',
    /**
     * Hash events.
     */
    HASH: 'HASH',
    /**
     * Sorted Set events.
     */
    ZSET: 'ZSET',
    /**
     * Expiration events.
     */
    EXPIRED: 'EXPIRED',
    /**
     * Eviction events.
     */
    EVICTED: 'EVICTED',
    /**
     * Stream events.
     */
    STREAM: 'STREAM',
    /**
     * Module types events.
     */
    MODULE: 'MODULE',
    /**
     * Key-miss events Notice, key-miss event is the only type of event
     * that is fired from within a read command.
     */
    KEYMISS: 'KEYMISS',
    /**
     * All events (Excluding `KEYMISS`).
     */
    ALL: 'ALL',
    /**
     * A special notification available only for modules, indicates that
     * the key was loaded from persistence.
     */
    LOADED: 'LOADED',
    /**
     * Only new events (created).
     */
    NEW: 'NEW'
};

export { redis, EventNotificationFlags }

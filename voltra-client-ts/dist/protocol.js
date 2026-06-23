// ============================================================================
// Voltra TypeScript Client SDK — MessagePack Wire Protocol
//
// Voltra uses rmp_serde (Rust MessagePack) with the following conventions:
//
//   Structs     → MessagePack ARRAY (positional, no field names)
//   Enums       → MessagePack MAP with one entry: { "VariantName": [fields…] }
//   Option<T>   → nil (null) or T
//   Vec<u8>     → MessagePack BIN
//
// Outgoing (client → server):
//   ClientMessage::ReducerCall(call)  → { "ReducerCall":  [call_id, name, args_bin] }
//   ClientMessage::Subscribe(…)       → { "Subscribe":    [sub_id, query] }
//   ClientMessage::Unsubscribe(…)     → { "Unsubscribe":  [sub_id] }
//
// Incoming (server → client) — classic protocol:
//   ReducerResponse (bare struct)     → [call_id, success, result_bin|nil, error|nil]
//   ServerMessage::SubscriptionAck    → { "SubscriptionAck":    [sub_id, ok, msg|nil] }
//   ServerMessage::SubscriptionDiff   → { "SubscriptionDiff":   [sub_id, table, key, op, data|nil] }
//   ServerMessage::Error              → { "Error": [message] }
//
// Incoming (server → client) — two-frame protocol (TODO-013):
//   ServerMessage::SubscriptionRoute  → { "SubscriptionRoute": [[sub_id, ...]] }
//   ServerMessage::SubscriptionBody   → { "SubscriptionBody":  [table, key, op, data|nil] }
// ============================================================================
import { encode, decode } from "@msgpack/msgpack";
import { decompress as zstdDecompress } from "fzstd";
export function encodeReducerCall(callId, reducerName, args) {
    return encode({ ReducerCall: [callId, reducerName, args] });
}
export function encodeSubscribe(subscriptionId, query) {
    return encode({ Subscribe: [subscriptionId, query] });
}
export function encodeUnsubscribe(subscriptionId) {
    return encode({ Unsubscribe: [subscriptionId] });
}
export function encodeArgs(args) {
    return encode(args);
}
export function decodeServerMessage(bytes) {
    const buf = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    let value;
    try {
        value = decode(buf);
    }
    catch {
        return { type: "Unknown" };
    }
    // Bare ReducerResponse: array [call_id, success, result|nil, error|nil]
    if (Array.isArray(value) && value.length >= 2) {
        const [rawCallId, success] = value;
        if ((typeof rawCallId === "number" || typeof rawCallId === "bigint") &&
            typeof success === "boolean") {
            const callId = typeof rawCallId === "bigint" ? Number(rawCallId) : rawCallId;
            const resultRaw = value[2];
            const errorRaw = value[3];
            return {
                type: "ReducerResponse",
                data: {
                    callId,
                    success,
                    resultBytes: resultRaw instanceof Uint8Array ? resultRaw : null,
                    error: typeof errorRaw === "string" ? errorRaw : null,
                },
            };
        }
    }
    // ServerMessage variant: { "VariantName": [fields…] }
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        const entries = Object.entries(value);
        if (entries.length === 1) {
            const [variant, content] = entries[0];
            const fields = Array.isArray(content) ? content : [content];
            switch (variant) {
                case "SubscriptionAck":
                    return {
                        type: "SubscriptionAck",
                        data: {
                            subscriptionId: String(fields[0] ?? ""),
                            success: Boolean(fields[1]),
                            message: fields[2] != null ? String(fields[2]) : null,
                        },
                    };
                case "SubscriptionDiff": {
                    const rawData = fields[4];
                    const rowData = rawData != null &&
                        typeof rawData === "object" &&
                        !Array.isArray(rawData)
                        ? rawData
                        : null;
                    return {
                        type: "SubscriptionDiff",
                        data: {
                            subscriptionId: String(fields[0] ?? ""),
                            tableName: String(fields[1] ?? ""),
                            rowKey: String(fields[2] ?? ""),
                            operation: String(fields[3] ?? ""),
                            rowData,
                        },
                    };
                }
                case "SubscriptionRoute": {
                    // fields[0] is an array of subscription id strings
                    const idsRaw = fields[0];
                    const subscriptionIds = Array.isArray(idsRaw)
                        ? idsRaw.map((v) => String(v))
                        : [];
                    return { type: "SubscriptionRoute", data: { subscriptionIds } };
                }
                case "SubscriptionBody": {
                    // [table_name, row_key, operation, row_data|nil]
                    const rawData = fields[3];
                    const rowData = rawData != null &&
                        typeof rawData === "object" &&
                        !Array.isArray(rawData)
                        ? rawData
                        : null;
                    return {
                        type: "SubscriptionBody",
                        data: {
                            tableName: String(fields[0] ?? ""),
                            rowKey: String(fields[1] ?? ""),
                            operation: String(fields[2] ?? ""),
                            rowData,
                        },
                    };
                }
                case "ReducerResponse": {
                    const inner = Array.isArray(content) ? content : [];
                    const rawCallId = inner[0];
                    const callId = typeof rawCallId === "bigint"
                        ? Number(rawCallId)
                        : Number(rawCallId ?? 0);
                    return {
                        type: "ReducerResponse",
                        data: {
                            callId,
                            success: Boolean(inner[1]),
                            resultBytes: inner[2] instanceof Uint8Array ? inner[2] : null,
                            error: inner[3] != null ? String(inner[3]) : null,
                        },
                    };
                }
                case "BatchUpdate": {
                    const isCompressed = Boolean(fields[0]);
                    const payloadRaw = fields[1];
                    if (!(payloadRaw instanceof Uint8Array)) {
                        return { type: "Unknown" };
                    }
                    let raw;
                    if (isCompressed) {
                        try {
                            raw = zstdDecompress(payloadRaw);
                        }
                        catch {
                            console.warn("[Voltra] BatchUpdate zstd decompress failed");
                            return { type: "Unknown" };
                        }
                    }
                    else {
                        raw = payloadRaw;
                    }
                    let diffArrays;
                    try {
                        diffArrays = decode(raw);
                    }
                    catch {
                        console.warn("[Voltra] BatchUpdate inner decode failed");
                        return { type: "Unknown" };
                    }
                    if (!Array.isArray(diffArrays)) {
                        return { type: "Unknown" };
                    }
                    const diffs = diffArrays.map((d) => {
                        const arr = Array.isArray(d) ? d : [];
                        const rawData = arr[4];
                        const rowData = rawData != null &&
                            typeof rawData === "object" &&
                            !Array.isArray(rawData)
                            ? rawData
                            : null;
                        return {
                            subscriptionId: String(arr[0] ?? ""),
                            tableName: String(arr[1] ?? ""),
                            rowKey: String(arr[2] ?? ""),
                            operation: String(arr[3] ?? ""),
                            rowData,
                        };
                    });
                    return { type: "BatchUpdate", diffs };
                }
                case "Error":
                    return {
                        type: "Error",
                        message: String(fields[0] ?? "Unknown error"),
                    };
                default:
                    return { type: "Unknown" };
            }
        }
    }
    return { type: "Unknown" };
}
export function decodeResult(bytes) {
    return decode(bytes);
}
//# sourceMappingURL=protocol.js.map
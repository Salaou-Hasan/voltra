import type { ReducerResult, SubscriptionAck, RowDiff, SubscriptionRouteData, SubscriptionBodyData } from "./types.js";
export type DecodedMessage = {
    type: "ReducerResponse";
    data: ReducerResult;
} | {
    type: "SubscriptionAck";
    data: SubscriptionAck;
} | {
    type: "SubscriptionDiff";
    data: RowDiff;
} | {
    type: "BatchUpdate";
    diffs: RowDiff[];
} | {
    type: "SubscriptionRoute";
    data: SubscriptionRouteData;
} | {
    type: "SubscriptionBody";
    data: SubscriptionBodyData;
} | {
    type: "Error";
    message: string;
} | {
    type: "Unknown";
};
export declare function encodeReducerCall(callId: number, reducerName: string, args: Uint8Array): Uint8Array;
export declare function encodeSubscribe(subscriptionId: string, query: string): Uint8Array;
export declare function encodeUnsubscribe(subscriptionId: string): Uint8Array;
export declare function encodeArgs(args: unknown): Uint8Array;
export declare function decodeServerMessage(bytes: ArrayBuffer | Uint8Array): DecodedMessage;
export declare function decodeResult<T = unknown>(bytes: Uint8Array): T;
//# sourceMappingURL=protocol.d.ts.map
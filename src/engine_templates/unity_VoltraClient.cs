// ============================================================================
// Voltra Unity Client
//
// Drop this file into Assets/Scripts/, add the VoltraBehaviour component to a
// GameObject (or call VoltraClient directly), and you have:
//
//   var db = new VoltraClient("ws://127.0.0.1:3000");
//   await db.Connect();
//   db.Subscribe("players WHERE zone = 'z_0_0'", diff => {
//       Debug.Log($"{diff.Op} {diff.RowKey}: {diff.Data}");
//   });
//   var result = await db.Call("spawn", new object[] { "player1", 0, 0, "warrior" });
//
// Wire protocol: MessagePack (rmp_serde conventions) over WebSocket.
//   ReducerCall  → { "ReducerCall": [call_id, name, args_bin] }
//   Subscribe    → { "Subscribe":   [sub_id, query] }
//   Response     → [call_id, success, result_bin|nil, error|nil]  (bare array)
//                  or { "ReducerResponse": [...] }
//   Diff         → { "SubscriptionDiff": [sub_id, table, key, op, data|nil] }
//   Batch        → { "BatchUpdate": [compressed_bool, payload_bin] }  (zstd)
//                  payload is MsgPack Vec<[sub_id, table, key, op, data|nil]>
//                  zstd-compressed when compressed_bool = true
//
// Dependency: ZstdSharp.Port (NuGet) — pure managed C# zstd decompressor.
//   Install via: NuGet for Unity, or download from nuget.org.
//
// Works in the Editor and standalone players (uses System.Net.WebSockets).
// For WebGL builds you need a JS WebSocket bridge — see README.
// ============================================================================

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using ZstdSharp;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Voltra
{
    // ── Public types ─────────────────────────────────────────────────────────

    public sealed class ReducerResult
    {
        public bool Success;
        public object Result;   // decoded MessagePack value (Dictionary / List / primitives)
        public string Error;
    }

    public sealed class RowDiff
    {
        public string SubscriptionId;
        public string Table;
        public string RowKey;
        // Op values: "insert" | "update" | "delete" | "initial_snapshot" | "patch"
        // When Op == "patch", Data contains only the changed fields — merge into your
        // existing row dict instead of replacing it.
        public string Op;
        public object Data;     // Dictionary<string, object> or null
    }

    // ── Client ───────────────────────────────────────────────────────────────

    public sealed class VoltraClient : IDisposable
    {
        private readonly string _url;
        private readonly string _apiKey;
        private ClientWebSocket _ws;
        private CancellationTokenSource _cts;
        private long _nextCallId = 1;
        private long _nextSubId = 1;

        private readonly ConcurrentDictionary<long, TaskCompletionSource<ReducerResult>> _pending
            = new ConcurrentDictionary<long, TaskCompletionSource<ReducerResult>>();
        private readonly ConcurrentDictionary<string, Action<RowDiff>> _subs
            = new ConcurrentDictionary<string, Action<RowDiff>>();

        /// Queue drained by VoltraBehaviour.Update() so callbacks run on the
        /// Unity main thread (required for touching GameObjects).
        public readonly ConcurrentQueue<Action> MainThreadQueue = new ConcurrentQueue<Action>();

        public event Action OnDisconnected;

        public VoltraClient(string url, string apiKey = null)
        {
            _url = url;
            _apiKey = apiKey;
        }

        public async Task Connect()
        {
            _ws = new ClientWebSocket();
            if (!string.IsNullOrEmpty(_apiKey))
                _ws.Options.SetRequestHeader("Authorization", "Bearer " + _apiKey);
            _cts = new CancellationTokenSource();
            await _ws.ConnectAsync(new Uri(_url), _cts.Token);
            _ = Task.Run(ReadLoop);
        }

        /// Call a reducer with positional args. Args may be numbers, strings,
        /// bools, nulls, object[] / List, and Dictionary<string, object>.
        public Task<ReducerResult> Call(string reducer, object[] args, int timeoutMs = 5000)
        {
            long id = Interlocked.Increment(ref _nextCallId);
            var tcs = new TaskCompletionSource<ReducerResult>();
            _pending[id] = tcs;

            byte[] argsBin = MsgPack.Encode(args ?? new object[0]);
            var w = new MsgPackWriter();
            w.WriteMapHeader(1);
            w.WriteString("ReducerCall");
            w.WriteArrayHeader(3);
            w.WriteInt(id);
            w.WriteString(reducer);
            w.WriteBin(argsBin);
            _ = SendRaw(w.ToArray());

            var timeout = Task.Delay(timeoutMs).ContinueWith(_ =>
            {
                if (_pending.TryRemove(id, out var t))
                    t.TrySetResult(new ReducerResult { Success = false, Error = "timeout" });
            });
            return tcs.Task;
        }

        /// Live query subscription. The callback fires for the initial
        /// snapshot and every later change, on the Unity main thread when
        /// used through VoltraBehaviour.
        public string Subscribe(string query, Action<RowDiff> onDiff)
        {
            string subId = "u" + Interlocked.Increment(ref _nextSubId);
            _subs[subId] = onDiff;
            var w = new MsgPackWriter();
            w.WriteMapHeader(1);
            w.WriteString("Subscribe");
            w.WriteArrayHeader(2);
            w.WriteString(subId);
            w.WriteString(query);
            _ = SendRaw(w.ToArray());
            return subId;
        }

        public void Unsubscribe(string subId)
        {
            _subs.TryRemove(subId, out _);
            var w = new MsgPackWriter();
            w.WriteMapHeader(1);
            w.WriteString("Unsubscribe");
            w.WriteArrayHeader(1);
            w.WriteString(subId);
            _ = SendRaw(w.ToArray());
        }

        private async Task SendRaw(byte[] frame)
        {
            try
            {
                await _ws.SendAsync(new ArraySegment<byte>(frame),
                    WebSocketMessageType.Binary, true, _cts.Token);
            }
            catch { /* connection dead — ReadLoop will surface it */ }
        }

        private async Task ReadLoop()
        {
            var buffer = new byte[64 * 1024];
            var frame = new MemoryStream();
            try
            {
                while (_ws.State == WebSocketState.Open)
                {
                    frame.SetLength(0);
                    WebSocketReceiveResult r;
                    do
                    {
                        r = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
                        if (r.MessageType == WebSocketMessageType.Close) return;
                        frame.Write(buffer, 0, r.Count);
                    } while (!r.EndOfMessage);
                    HandleFrame(frame.ToArray());
                }
            }
            catch { }
            finally
            {
                MainThreadQueue.Enqueue(() => OnDisconnected?.Invoke());
                foreach (var kv in _pending)
                    kv.Value.TrySetResult(new ReducerResult { Success = false, Error = "disconnected" });
                _pending.Clear();
            }
        }

        private void HandleFrame(byte[] bytes)
        {
            object v;
            try { v = MsgPack.Decode(bytes); } catch { return; }

            // Bare ReducerResponse: [call_id, success, result|nil, error|nil]
            if (v is List<object> arr && arr.Count >= 2 && arr[1] is bool ok)
            {
                long callId = Convert.ToInt64(arr[0]);
                if (_pending.TryRemove(callId, out var tcs))
                {
                    object result = null;
                    if (arr.Count > 2 && arr[2] is byte[] rb)
                        try { result = MsgPack.Decode(rb); } catch { }
                    string err = arr.Count > 3 ? arr[3] as string : null;
                    tcs.TrySetResult(new ReducerResult { Success = ok, Result = result, Error = err });
                }
                return;
            }

            // ServerMessage variant: { "Name": [...] }
            if (v is Dictionary<string, object> map && map.Count == 1)
            {
                foreach (var kv in map)
                {
                    var fields = kv.Value as List<object> ?? new List<object> { kv.Value };
                    switch (kv.Key)
                    {
                        case "ReducerResponse":
                            HandleFrame(MsgPack.Encode(fields)); // re-route as bare array
                            break;
                        case "SubscriptionDiff":
                            if (fields.Count >= 4)
                            {
                                var diff = new RowDiff
                                {
                                    SubscriptionId = fields[0] as string,
                                    Table = fields[1] as string,
                                    RowKey = fields[2] as string,
                                    Op = fields[3] as string,
                                    Data = fields.Count > 4 ? fields[4] : null,
                                };
                                if (diff.SubscriptionId != null &&
                                    _subs.TryGetValue(diff.SubscriptionId, out var cb))
                                    MainThreadQueue.Enqueue(() => cb(diff));
                            }
                            break;

                        case "BatchUpdate":
                            // fields = [compressed_bool, payload_bin]
                            if (fields.Count >= 2 && fields[1] is byte[] batchPayload)
                            {
                                bool isCompressed = fields[0] is bool bc && bc;
                                byte[] batchRaw;
                                if (isCompressed)
                                {
                                    using var dec = new Decompressor();
                                    batchRaw = dec.Unwrap(batchPayload).ToArray();
                                }
                                else
                                {
                                    batchRaw = batchPayload;
                                }
                                // payload is MsgPack array of diffs: [[sub_id, table, key, op, data?], ...]
                                if (MsgPack.Decode(batchRaw) is List<object> diffList)
                                {
                                    foreach (var item in diffList)
                                    {
                                        if (item is List<object> df && df.Count >= 4)
                                        {
                                            var diff = new RowDiff
                                            {
                                                SubscriptionId = df[0] as string,
                                                Table          = df[1] as string,
                                                RowKey         = df[2] as string,
                                                Op             = df[3] as string,
                                                Data           = df.Count > 4 ? df[4] : null,
                                            };
                                            if (diff.SubscriptionId != null &&
                                                _subs.TryGetValue(diff.SubscriptionId, out var cb2))
                                            {
                                                var captured = diff;
                                                MainThreadQueue.Enqueue(() => cb2(captured));
                                            }
                                        }
                                    }
                                }
                            }
                            break;
                    }
                }
            }
        }

        public void Dispose()
        {
            try { _cts?.Cancel(); } catch { }
            try { _ws?.Dispose(); } catch { }
        }
    }

    // ── Minimal MessagePack (the subset Voltra speaks) ───────────────────────

    public sealed class MsgPackWriter
    {
        private readonly MemoryStream _s = new MemoryStream();

        public byte[] ToArray() => _s.ToArray();
        private void B(byte b) => _s.WriteByte(b);
        private void Raw(byte[] b) => _s.Write(b, 0, b.Length);
        private void BE(byte[] b) { Array.Reverse(b); Raw(b); }

        public void WriteNil() => B(0xc0);
        public void WriteBool(bool v) => B(v ? (byte)0xc3 : (byte)0xc2);

        public void WriteInt(long v)
        {
            if (v >= 0 && v < 128) { B((byte)v); }
            else if (v < 0 && v >= -32) { B((byte)(0xe0 | (v + 32))); }
            else if (v >= sbyte.MinValue && v <= sbyte.MaxValue) { B(0xd0); B((byte)(sbyte)v); }
            else if (v >= short.MinValue && v <= short.MaxValue) { B(0xd1); BE(BitConverter.GetBytes((short)v)); }
            else if (v >= int.MinValue && v <= int.MaxValue) { B(0xd2); BE(BitConverter.GetBytes((int)v)); }
            else { B(0xd3); BE(BitConverter.GetBytes(v)); }
        }

        public void WriteDouble(double v) { B(0xcb); BE(BitConverter.GetBytes(v)); }

        public void WriteString(string s)
        {
            var b = Encoding.UTF8.GetBytes(s);
            if (b.Length < 32) B((byte)(0xa0 | b.Length));
            else if (b.Length < 256) { B(0xd9); B((byte)b.Length); }
            else { B(0xda); BE(BitConverter.GetBytes((ushort)b.Length)); }
            Raw(b);
        }

        public void WriteBin(byte[] b)
        {
            if (b.Length < 256) { B(0xc4); B((byte)b.Length); }
            else { B(0xc5); BE(BitConverter.GetBytes((ushort)b.Length)); }
            Raw(b);
        }

        public void WriteArrayHeader(int n)
        {
            if (n < 16) B((byte)(0x90 | n));
            else { B(0xdc); BE(BitConverter.GetBytes((ushort)n)); }
        }

        public void WriteMapHeader(int n)
        {
            if (n < 16) B((byte)(0x80 | n));
            else { B(0xde); BE(BitConverter.GetBytes((ushort)n)); }
        }

        public void WriteValue(object v)
        {
            switch (v)
            {
                case null: WriteNil(); break;
                case bool b: WriteBool(b); break;
                case sbyte i: WriteInt(i); break;
                case byte i: WriteInt(i); break;
                case short i: WriteInt(i); break;
                case ushort i: WriteInt(i); break;
                case int i: WriteInt(i); break;
                case uint i: WriteInt(i); break;
                case long i: WriteInt(i); break;
                case float f: WriteDouble(f); break;
                case double d: WriteDouble(d); break;
                case string s: WriteString(s); break;
                case byte[] bin: WriteBin(bin); break;
                case System.Collections.IDictionary dict:
                    WriteMapHeader(dict.Count);
                    foreach (System.Collections.DictionaryEntry e in dict)
                    {
                        WriteString(e.Key.ToString());
                        WriteValue(e.Value);
                    }
                    break;
                case System.Collections.IEnumerable seq:
                    var items = new List<object>();
                    foreach (var item in seq) items.Add(item);
                    WriteArrayHeader(items.Count);
                    foreach (var item in items) WriteValue(item);
                    break;
                default:
                    WriteString(v.ToString());
                    break;
            }
        }
    }

    public static class MsgPack
    {
        public static byte[] Encode(object v)
        {
            var w = new MsgPackWriter();
            w.WriteValue(v);
            return w.ToArray();
        }

        public static object Decode(byte[] data)
        {
            int pos = 0;
            return Read(data, ref pos);
        }

        private static object Read(byte[] d, ref int p)
        {
            byte t = d[p++];
            if (t < 0x80) return (long)t;                       // positive fixint
            if (t >= 0xe0) return (long)(sbyte)t;               // negative fixint
            if (t >= 0xa0 && t <= 0xbf) return Str(d, ref p, t & 0x1f);
            if (t >= 0x90 && t <= 0x9f) return Arr(d, ref p, t & 0x0f);
            if (t >= 0x80 && t <= 0x8f) return Map(d, ref p, t & 0x0f);
            switch (t)
            {
                case 0xc0: return null;
                case 0xc2: return false;
                case 0xc3: return true;
                case 0xc4: return Bin(d, ref p, d[p++]);
                case 0xc5: return Bin(d, ref p, U16(d, ref p));
                case 0xc6: return Bin(d, ref p, (int)U32(d, ref p));
                case 0xca: { var b = Be(d, ref p, 4); return (double)BitConverter.ToSingle(b, 0); }
                case 0xcb: { var b = Be(d, ref p, 8); return BitConverter.ToDouble(b, 0); }
                case 0xcc: return (long)d[p++];
                case 0xcd: return (long)U16(d, ref p);
                case 0xce: return (long)U32(d, ref p);
                case 0xcf: { var b = Be(d, ref p, 8); return (long)BitConverter.ToUInt64(b, 0); }
                case 0xd0: return (long)(sbyte)d[p++];
                case 0xd1: { var b = Be(d, ref p, 2); return (long)BitConverter.ToInt16(b, 0); }
                case 0xd2: { var b = Be(d, ref p, 4); return (long)BitConverter.ToInt32(b, 0); }
                case 0xd3: { var b = Be(d, ref p, 8); return BitConverter.ToInt64(b, 0); }
                case 0xd9: return Str(d, ref p, d[p++]);
                case 0xda: return Str(d, ref p, U16(d, ref p));
                case 0xdb: return Str(d, ref p, (int)U32(d, ref p));
                case 0xdc: return Arr(d, ref p, U16(d, ref p));
                case 0xdd: return Arr(d, ref p, (int)U32(d, ref p));
                case 0xde: return Map(d, ref p, U16(d, ref p));
                case 0xdf: return Map(d, ref p, (int)U32(d, ref p));
                default: throw new Exception($"msgpack: unsupported type 0x{t:x2}");
            }
        }

        private static byte[] Be(byte[] d, ref int p, int n)
        {
            var b = new byte[n];
            Array.Copy(d, p, b, 0, n);
            p += n;
            Array.Reverse(b);
            return b;
        }
        private static int U16(byte[] d, ref int p) { var b = Be(d, ref p, 2); return BitConverter.ToUInt16(b, 0); }
        private static uint U32(byte[] d, ref int p) { var b = Be(d, ref p, 4); return BitConverter.ToUInt32(b, 0); }
        private static string Str(byte[] d, ref int p, int len)
        {
            var s = Encoding.UTF8.GetString(d, p, len);
            p += len;
            return s;
        }
        private static byte[] Bin(byte[] d, ref int p, int len)
        {
            var b = new byte[len];
            Array.Copy(d, p, b, 0, len);
            p += len;
            return b;
        }
        private static List<object> Arr(byte[] d, ref int p, int n)
        {
            var l = new List<object>(n);
            for (int i = 0; i < n; i++) l.Add(Read(d, ref p));
            return l;
        }
        private static Dictionary<string, object> Map(byte[] d, ref int p, int n)
        {
            var m = new Dictionary<string, object>(n);
            for (int i = 0; i < n; i++)
            {
                var k = Read(d, ref p)?.ToString() ?? "";
                m[k] = Read(d, ref p);
            }
            return m;
        }
    }
}

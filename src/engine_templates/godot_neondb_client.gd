# ==============================================================================
# NeonDB Godot 4 Client — single-file, zero dependencies.
#
# Add as an autoload (Project Settings → Autoload → this script as "NeonDB"),
# or attach to a Node. Then:
#
#   NeonDB.connect_to("ws://127.0.0.1:3000")
#   await NeonDB.connected
#
#   var result = await NeonDB.call_reducer("spawn", ["player1", 0, 0, "warrior"])
#   print(result.success, " ", result.data)
#
#   NeonDB.subscribe("players WHERE zone = 'z_0_0'")
#   # then connect the row_update signal:
#   NeonDB.row_update.connect(func(table, key, op, data):
#       print(op, " ", key, ": ", data))
#
# Wire protocol: MessagePack (rmp_serde conventions) over WebSocket.
#   ReducerCall  → { "ReducerCall": [call_id, name, args_bin] }
#   Subscribe    → { "Subscribe":   [sub_id, query] }
#   Response     → [call_id, success, result_bin|nil, error|nil]
#   Diff         → { "SubscriptionDiff": [sub_id, table, key, op, data|nil] }
#   Batch        → { "BatchUpdate": [compressed_bool, payload_bin] }  (zstd)
#                  payload is MsgPack [[sub_id, table, key, op, data|nil], ...]
#                  zstd-compressed when compressed_bool = true
# ==============================================================================
extends Node

signal connected
signal disconnected
## op can be "insert" | "update" | "delete" | "initial_snapshot" | "patch"
## When op == "patch", data contains only changed fields — merge into your row cache.
signal row_update(table: String, row_key: String, op: String, data)

var _ws := WebSocketPeer.new()
var _state := WebSocketPeer.STATE_CLOSED
var _next_call_id := 1
var _next_sub_id := 1
var _pending := {}   # call_id -> {done: bool, result: Dictionary}
var _api_key := ""

func connect_to(url: String, api_key: String = "") -> void:
	_api_key = api_key
	if api_key != "":
		_ws.handshake_headers = PackedStringArray(["Authorization: Bearer " + api_key])
	var err := _ws.connect_to_url(url)
	if err != OK:
		push_error("[NeonDB] connect failed: %s" % err)
	set_process(true)

func _process(_delta: float) -> void:
	_ws.poll()
	var s := _ws.get_ready_state()
	if s != _state:
		if s == WebSocketPeer.STATE_OPEN:
			connected.emit()
		elif s == WebSocketPeer.STATE_CLOSED and _state == WebSocketPeer.STATE_OPEN:
			disconnected.emit()
			for id in _pending:
				_pending[id] = {"done": true, "result": {"success": false, "error": "disconnected", "data": null}}
		_state = s
	while s == WebSocketPeer.STATE_OPEN and _ws.get_available_packet_count() > 0:
		_handle_frame(_ws.get_packet())

# ── Public API ────────────────────────────────────────────────────────────────

## Call a reducer with positional args. Awaitable:
##   var r = await NeonDB.call_reducer("spawn", ["p1", 0, 0, "warrior"])
##   r = {"success": bool, "data": Variant, "error": String|null}
func call_reducer(reducer: String, args: Array = [], timeout_secs := 5.0) -> Dictionary:
	var call_id := _next_call_id
	_next_call_id += 1
	_pending[call_id] = {"done": false, "result": {}}

	var args_bin := MsgPack.pack(args)
	var frame := MsgPack.pack_map_header(1)
	frame.append_array(MsgPack.pack("ReducerCall"))
	frame.append_array(MsgPack.pack_array_header(3))
	frame.append_array(MsgPack.pack(call_id))
	frame.append_array(MsgPack.pack(reducer))
	frame.append_array(MsgPack.pack_bin(args_bin))
	_ws.send(frame)

	var waited := 0.0
	while not _pending[call_id]["done"] and waited < timeout_secs:
		await get_tree().process_frame
		waited += get_process_delta_time()
	var entry: Dictionary = _pending[call_id]
	_pending.erase(call_id)
	if not entry["done"]:
		return {"success": false, "error": "timeout", "data": null}
	return entry["result"]

## Subscribe to a live query. Updates arrive on the row_update signal.
func subscribe(query: String) -> String:
	var sub_id := "g%d" % _next_sub_id
	_next_sub_id += 1
	var frame := MsgPack.pack_map_header(1)
	frame.append_array(MsgPack.pack("Subscribe"))
	frame.append_array(MsgPack.pack_array_header(2))
	frame.append_array(MsgPack.pack(sub_id))
	frame.append_array(MsgPack.pack(query))
	_ws.send(frame)
	return sub_id

func unsubscribe(sub_id: String) -> void:
	var frame := MsgPack.pack_map_header(1)
	frame.append_array(MsgPack.pack("Unsubscribe"))
	frame.append_array(MsgPack.pack_array_header(1))
	frame.append_array(MsgPack.pack(sub_id))
	_ws.send(frame)

# ── Frame handling ────────────────────────────────────────────────────────────

func _handle_frame(bytes: PackedByteArray) -> void:
	var parsed = MsgPack.unpack(bytes)
	if parsed == null:
		return

	# Bare ReducerResponse: [call_id, success, result_bin|nil, error|nil]
	if parsed is Array and parsed.size() >= 2 and parsed[1] is bool:
		var call_id = int(parsed[0])
		if _pending.has(call_id):
			var data = null
			if parsed.size() > 2 and parsed[2] is PackedByteArray:
				data = MsgPack.unpack(parsed[2])
			var err = parsed[3] if parsed.size() > 3 else null
			_pending[call_id] = {"done": true, "result": {
				"success": parsed[1], "data": data, "error": err}}
		return

	# ServerMessage variant: { "Name": [...] }
	if parsed is Dictionary and parsed.size() == 1:
		for variant in parsed:
			var fields = parsed[variant]
			if not (fields is Array):
				fields = [fields]
			match variant:
				"SubscriptionDiff":
					if fields.size() >= 4:
						var data = fields[4] if fields.size() > 4 else null
						row_update.emit(str(fields[1]), str(fields[2]), str(fields[3]), data)
				"ReducerResponse":
					_handle_frame(MsgPack.pack(fields))
				"BatchUpdate":
					# fields = [compressed_bool, payload_bytes]
					if fields.size() >= 2 and fields[1] is PackedByteArray:
						var batch_raw: PackedByteArray = fields[1]
						if fields[0] == true:
							# zstd decompress (FileAccess.COMPRESSION_ZSTD = 2)
							batch_raw = batch_raw.decompress_dynamic(-1, 2)
						if batch_raw == null or batch_raw.size() == 0:
							return
						var diffs = MsgPack.unpack(batch_raw)
						if diffs is Array:
							for df in diffs:
								if df is Array and df.size() >= 4:
									var d = df[4] if df.size() > 4 else null
									row_update.emit(str(df[1]), str(df[2]), str(df[3]), d)

# ==============================================================================
# Minimal MessagePack (the subset NeonDB speaks)
# ==============================================================================
class MsgPack:
	static func pack(v) -> PackedByteArray:
		var out := PackedByteArray()
		_pack_value(out, v)
		return out

	static func pack_bin(b: PackedByteArray) -> PackedByteArray:
		var out := PackedByteArray()
		if b.size() < 256:
			out.append(0xc4); out.append(b.size())
		else:
			out.append(0xc5); out.append((b.size() >> 8) & 0xff); out.append(b.size() & 0xff)
		out.append_array(b)
		return out

	static func pack_array_header(n: int) -> PackedByteArray:
		var out := PackedByteArray()
		if n < 16:
			out.append(0x90 | n)
		else:
			out.append(0xdc); out.append((n >> 8) & 0xff); out.append(n & 0xff)
		return out

	static func pack_map_header(n: int) -> PackedByteArray:
		var out := PackedByteArray()
		if n < 16:
			out.append(0x80 | n)
		else:
			out.append(0xde); out.append((n >> 8) & 0xff); out.append(n & 0xff)
		return out

	static func _pack_value(out: PackedByteArray, v) -> void:
		if v == null:
			out.append(0xc0)
		elif v is bool:
			out.append(0xc3 if v else 0xc2)
		elif v is int:
			if v >= 0 and v < 128:
				out.append(v)
			elif v < 0 and v >= -32:
				out.append(0xe0 | (v + 32))
			else:
				out.append(0xd3)
				for i in range(7, -1, -1):
					out.append((v >> (i * 8)) & 0xff)
		elif v is float:
			out.append(0xcb)
			var b := PackedByteArray()
			b.resize(8)
			b.encode_double(0, v)
			b.reverse()
			out.append_array(b)
		elif v is String:
			# Explicit type: Godot 4.6's stricter inference rejects `:=` here
			# because `v` is untyped (the method's return type can't be inferred).
			var utf: PackedByteArray = v.to_utf8_buffer()
			if utf.size() < 32:
				out.append(0xa0 | utf.size())
			elif utf.size() < 256:
				out.append(0xd9); out.append(utf.size())
			else:
				out.append(0xda); out.append((utf.size() >> 8) & 0xff); out.append(utf.size() & 0xff)
			out.append_array(utf)
		elif v is PackedByteArray:
			out.append_array(pack_bin(v))
		elif v is Array:
			out.append_array(pack_array_header(v.size()))
			for item in v:
				_pack_value(out, item)
		elif v is Dictionary:
			out.append_array(pack_map_header(v.size()))
			for k in v:
				_pack_value(out, str(k))
				_pack_value(out, v[k])
		else:
			_pack_value(out, str(v))

	static func unpack(bytes: PackedByteArray):
		var p := [0]
		return _read(bytes, p)

	static func _read(d: PackedByteArray, p: Array):
		if p[0] >= d.size():
			return null
		var t := d[p[0]]
		p[0] += 1
		if t < 0x80:
			return t
		if t >= 0xe0:
			return t - 256
		if t >= 0xa0 and t <= 0xbf:
			return _str(d, p, t & 0x1f)
		if t >= 0x90 and t <= 0x9f:
			return _arr(d, p, t & 0x0f)
		if t >= 0x80 and t <= 0x8f:
			return _map(d, p, t & 0x0f)
		match t:
			0xc0: return null
			0xc2: return false
			0xc3: return true
			0xc4: return _bin(d, p, _u8(d, p))
			0xc5: return _bin(d, p, _u16(d, p))
			0xc6: return _bin(d, p, _u32(d, p))
			0xca:
				var b := d.slice(p[0], p[0] + 4); b.reverse(); p[0] += 4
				return b.decode_float(0)
			0xcb:
				var b := d.slice(p[0], p[0] + 8); b.reverse(); p[0] += 8
				return b.decode_double(0)
			0xcc: return _u8(d, p)
			0xcd: return _u16(d, p)
			0xce: return _u32(d, p)
			0xcf: return _i64(d, p)
			0xd0:
				var v := _u8(d, p)
				return v - 256 if v > 127 else v
			0xd1:
				var v := _u16(d, p)
				return v - 65536 if v > 32767 else v
			0xd2:
				var v := _u32(d, p)
				return v - 4294967296 if v > 2147483647 else v
			0xd3: return _i64(d, p)
			0xd9: return _str(d, p, _u8(d, p))
			0xda: return _str(d, p, _u16(d, p))
			0xdb: return _str(d, p, _u32(d, p))
			0xdc: return _arr(d, p, _u16(d, p))
			0xdd: return _arr(d, p, _u32(d, p))
			0xde: return _map(d, p, _u16(d, p))
			0xdf: return _map(d, p, _u32(d, p))
		return null

	static func _u8(d: PackedByteArray, p: Array) -> int:
		var v := d[p[0]]; p[0] += 1; return v
	static func _u16(d: PackedByteArray, p: Array) -> int:
		var v := (d[p[0]] << 8) | d[p[0] + 1]; p[0] += 2; return v
	static func _u32(d: PackedByteArray, p: Array) -> int:
		var v := (d[p[0]] << 24) | (d[p[0] + 1] << 16) | (d[p[0] + 2] << 8) | d[p[0] + 3]
		p[0] += 4; return v
	static func _i64(d: PackedByteArray, p: Array) -> int:
		var v := 0
		for i in range(8):
			v = (v << 8) | d[p[0] + i]
		p[0] += 8
		return v
	static func _str(d: PackedByteArray, p: Array, n: int) -> String:
		var s := d.slice(p[0], p[0] + n).get_string_from_utf8()
		p[0] += n; return s
	static func _bin(d: PackedByteArray, p: Array, n: int) -> PackedByteArray:
		var b := d.slice(p[0], p[0] + n)
		p[0] += n; return b
	static func _arr(d: PackedByteArray, p: Array, n: int) -> Array:
		var a := []
		for i in range(n):
			a.append(_read(d, p))
		return a
	static func _map(d: PackedByteArray, p: Array, n: int) -> Dictionary:
		var m := {}
		for i in range(n):
			var k = _read(d, p)
			m[str(k)] = _read(d, p)
		return m

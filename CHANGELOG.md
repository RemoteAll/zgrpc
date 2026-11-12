# 鍙樻洿璁板綍

鏈簱閬靛惊 [璇箟鍖栫増鏈琞(https://semver.org/lang/zh-CN/)銆傜増鏈彿鍦?`build.zig.zon`锛?
杩愯鏃跺彲閫氳繃 `zgrpc.version` 鍙栧埌锛堜篃浼氫綔涓洪粯璁?`user-agent` 鐨勪竴閮ㄥ垎鍙戝嚭锛夈€?

## 0.2.0 鈥?2026-11-12

棣栦釜銆屽姛鑳藉畬鏁淬€佸彲涓婄嚎銆嶇殑鐗堟湰銆傜浉瀵?0.1.0锛堝彧鏈?hpack + h2 + grpc 鏈嶅姟绔鏋讹級锛?
琛ラ綈浜嗕粠銆屽崗璁纭€с€嶅埌銆岀敓浜у彲鐢ㄦ€с€嶇殑鏁存潯閾捐矾銆?

### 鏂板

**瀹㈡埛绔?*
- `client.Client`锛氭彙鎵嬨€乣unary`銆乣callMany`锛堝悓杩炴帴骞跺彂澶氭祦锛夈€乣clientStream`锛堝鎴风娴?/ bidi锛?
- `Options.content_type` 涓夋。锛氬師鐢?gRPC / `grpc-web` / `grpc-web-text`
- 鑷姩 SETTINGS ACK銆丳ING ACK銆佺獥鍙ｅ洖琛ワ紱鍝嶅簲鎸?stream_id 褰掍綅
- 鍙戦€?`grpc-timeout`銆乣user-agent`銆乣grpc-accept-encoding`銆佽嚜瀹氫箟 metadata
- 鎸夎鑼?*鍚堟垚** Status/Status-Message锛堝绔潪 200 鎴栫己 `grpc-status` 鏃讹級
- gRPC-Web 灏惧抚鐘舵€佽В鏋愶紱`DATA` 甯?`END_STREAM` 鏃舵纭叧娴?

**gRPC 璇箟灞?*
- 鏈嶅姟绔祦寮?/ 瀹㈡埛绔祦 / bidi锛坄Server.stream_routes` + `streamHooks`锛?
- gRPC-Web锛坱railers 璧?body 灏惧抚锛変笌 **鏂囨湰妯″紡**锛坄application/grpc-web-text`锛宐ase64锛?
- gzip 娑堟伅鍘嬬缉锛坄grpc-encoding: gzip`锛岄槇鍊?256 瀛楄妭锛屼笌 zzig / DH.RustBase 鍚屽彛寰勶級
- `grpc-timeout`锛氳В鏋愪负鍗曡皟鏃堕挓鎴鏃跺埢锛岃秴鏃跺洖 `DEADLINE_EXCEEDED(4)`锛?
  `Call.expired()` / `checkDeadline()` 渚涗笟鍔″煁妫€鏌ョ偣
- **Custom-Metadata**锛歚-bin` base64锛堢紪鐮佹棤 padding銆佽В鐮佸弻瀹瑰繊銆侀€楀彿鍚堝苟閫愭瑙ｇ爜锛夛紝
  `setHeader` / `setBinaryHeader` / `setTrailer` / `setBinaryTrailer`
- `grpc-message` percent-encoding锛堝彂閫佺紪鐮?/ 瀹㈡埛绔В鐮?/ 闈炴硶 `%xx` 鍘熸牱淇濈暀锛?
- 闈?POST 鈫?**HTTP 405**锛沜ontent-type 涓嶇 鈫?**HTTP 415**
- 鍝嶅簲澶存寜 ABNF 椤哄簭锛歚[grpc-encoding] [grpc-accept-encoding] content-type` + 鑷畾涔?metadata

**鏍囧噯鏈嶅姟**
- `health`锛歚grpc.health.v1.Health` 鐨?`Check` / `Watch`锛堥浂渚濊禆鎵嬪啓 protobuf锛?

**protobuf**
- `pb`锛歷arint / zigzag / tag / length-delimited / fixed / packed 璇诲啓鍘熻锛?
  鏈煡瀛楁鑷姩璺宠繃銆佹埅鏂暟鎹繑鍥?`null` 涓?panic
- `tools/proto_gen.zig`锛歱roto3 **瀛愰泦**浠ｇ爜鐢熸垚锛坄.proto` 鈫?Zig struct + `encode`/`decode` + 璺敱甯搁噺锛夛紱
  瓒呰寖鍥磋娉曪紙`import` / `map` / `oneof` / `optional` / 宓屽瀹氫箟绛夛級**鎶ラ敊甯﹁鍙?*

**鍩虹璁炬柦**
- `clock`锛氬崟璋冩椂閽熶笌鐫＄湢锛圸ig 0.17 宸茬Щ闄?`std.time.nanoTimestamp` / `std.Thread.sleep`锛?
- `gzip`锛氭秷鎭帇缂?

### 鍋ュ．鎬?/ 瀹夊叏

- h2 灞備笂闄愶細`max_concurrent_streams`锛堥粯璁?100锛夈€乣max_header_block`锛堥粯璁?64 KiB锛夈€?
  `max_body`锛堥粯璁?4 MiB锛夛紝瓒呴檺鏈夋槑纭殑 `RST_STREAM` 鎷掔粷鐮?
- **娴佺骇閿欒涓嶅啀鎷嗘暣鏉¤繛鎺?*锛歅ADDED 甯?pad 瓒婄晫銆佸绔?CONTINUATION 鍧囨寜娴佸鐞?
  锛圧FC 9113 瑕佹眰锛夛紝骞舵湁澶氱粍鍥炲綊娴嬭瘯鍊煎畧
- 瀹㈡埛绔祦鍙峰繀椤讳负姝ｅ鏁帮紱`RST_STREAM` 杞借嵎蹇呴』 4 瀛楄妭
- 9 涓?h2 鍋ュ．鎬ф祴璇曪紙鍧忓墠瑷€ / 骞跺彂瓒呴檺 / header 瓒呴檺 / body 瓒呴檺 / 鎴柇甯?/ PADDED 瓒婄晫 /
  瀛ょ珛 CONTINUATION / 鍋舵暟娴佸彿 / RST 闀垮害锛?

### 娴嬭瘯涓庨獙璇?

- 鍗曞厓 / 绔埌绔祴璇?**50** 涓紙`zig test src/zgrpc.zig`锛? 鐢熸垚浠ｇ爜娴嬭瘯 3 涓紙`zig build test`锛?
- 涓€閿洖褰?`tools/regression.ps1`锛氭瀯寤?鈫?鍗曟祴 鈫?浠ｇ爜鐢熸垚 鈫?鐪熷疄 socket 鍐掔儫 鈫?**鍥涘悜 Rust 瀵规媿**
  锛坺grpc client 鈫?Rust server锛汻ust client 鍘熺敓 / gRPC-Web / gRPC-Web-text 鈫?zgrpc server锛?
- 鍐掔儫瑕嗙洊锛氬苟鍙戝弻娴併€乥idi 3/3銆佸仴搴锋鏌?SERVING銆乣grpc-timeout 鈫?4`銆乣-bin` metadata 寰€杩斻€?
  grpc-web-text 寰€杩斻€乸b 缁撴瀯鍖栧線杩?

### 鏄庣‘涓嶅仛锛堥檮鍘熷洜锛?

| 椤?| 鍘熷洜 |
|---|---|
| 鍐呯疆 TLS | Zig 0.17 鏍囧噯搴撳彧鏈?tls **client** 涓旀棤 ALPN锛涙湇鍔＄ TLS 搴斾氦缁欏弽浠ｏ紙瑙?`docs/閮ㄧ讲.md`锛?|
| `grpc-timeout` 寮哄埗涓柇 | Zig 鏃犳姠鍗犲紡鍙栨秷锛屽悓姝?handler 鏃犳硶琚閮ㄦ墦鏂紱鏀逛负涓氬姟鍩?`checkDeadline()` |
| well-known types / 鏈嶅姟鍙嶅皠 | 闇€瑕佸畬鏁?protobuf descriptor 浣撶郴锛岃秴鍑轰紶杈撳眰瀹氫綅 |
| 瀹屾暣鐨?`.proto` 鏀寔 | 鍚屼笂锛涘綋鍓嶈鐩栨湇鍔￠棿鑷帶涓ょ鐨勫父瑙佸啓娉?|

## 0.1.0 鈥?鍒濆鐗堟湰

- `hpack`锛圧PACK 7541锛岃В鐮佸惈 Huffman锛夈€乣h2`锛堟槑鏂?h2c锛夈€乣grpc` 璇箟灞傞鏋?

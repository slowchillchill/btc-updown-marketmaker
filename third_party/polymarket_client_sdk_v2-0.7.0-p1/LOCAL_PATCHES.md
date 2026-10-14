# 本地补丁来源

本目录继续采用锁定 `polymarket_client_sdk_v2` 0.7.0，不升级为 V3。原发布源码身份由 `.cargo_vcs_info.json` 保存；已有本地修复保持。新增 #301 / D-0350 补丁由本仓库对应提交维护准确源码身份，不把本地扩展写成上游已经提供的接口。

`Client::sign_and_hash` 在原 V1/V2 域选择和签名路径内返回同一 `SignedOrder` 与基础 EIP-712 Order hash。原 `sign` 与提前身份入口共用原签名核心，但不生成其不需要的额外基础摘要。普通V1/V2直接返回原签名摘要。Poly1271仍签原包装摘要，复用其原`contents_hash`和`app_domain_separator`；依据[D-0351](https://github.com/slowchillchill/btc-updown-marketmaker/issues/301#issuecomment-6161579473)，仅在提前身份入口增加一次`Keccak256(0x1901 || domain32 || contents32)`，没有重复计算结构或域。对外身份保持基础Order hash；不逐帧、不进行运行时哈希校验，也不声称具体耗时。平台POST JSON没有新增字段。

协议依据：[POST orderID](https://docs.polymarket.com/api-reference/trade/post-a-new-order)、[查询订单](https://docs.polymarket.com/api-reference/trade/get-single-order-by-id)和[私有订单事件](https://docs.polymarket.com/api-reference/wss/user)。独立期望来自 [固定官方 TS 源码](https://github.com/Polymarket/clob-client-v2/tree/8046a89ee9ba75676e31a11830785c046ccee167)，用原 `ExchangeOrderBuilderV1/V2::buildOrder`、`buildOrderTypedData`、`buildOrderHash` 及固定 viem 2.46.3 生成。测试夹具 `tests/order_hash_vectors.json` 保存输入、域条件和独立期望；包含原官方 V1 公布向量。生成程序及固定来源材料随 #301 验证证据保存。

此补丁只增加发送前可见的身份能力。它不证明平台已经受理，不增加查询、重试或资金政策。

# Changelog

## [0.3.0](https://github.com/aofei/wirehop/compare/v0.2.1...v0.3.0) (2026-10-11)


### ⚠ BREAKING CHANGES

* V1 frame and handshake encodings have changed. Deploy matching clients and servers together. The protocol version remains V1.
* Reserve zero-ID Data with a zero deadline for exactly 4096 padding bytes. Deploy matching client and server builds together. Protocol version remains 1.
* Replace the v1 admission and relay wire layouts, remove Probe frames, and use numeric lane and path group selectors. Deploy matching client and server builds together. Protocol version remains 1.
* Redefine the v1 frame format with canonical unsigned LEB128 fields and remove Probe IDs and diagnostic length fields. Deploy matching client and server builds together. Protocol version remains 1.

### Bug Fixes

* improve recovery on slow and changing networks ([1833888](https://github.com/aofei/wirehop/commit/18338884db11b44e25a45295a25650213a5bea44))
* preserve delivery sampling provenance and compact relay state ([4e2f59f](https://github.com/aofei/wirehop/commit/4e2f59fb0e24acd2cc5287f94091c7853005cafb))
* preserve relay feedback and batch receive accounting ([b8d619c](https://github.com/aofei/wirehop/commit/b8d619c65e0c8bdc52bf48e622c71bedac3a1d34))
* recover stalled relay paths and reduce receive overhead ([8115564](https://github.com/aofei/wirehop/commit/8115564297bdf9693a7136c15d5a8b2669191fce))
* release expired relay payloads and avoid event allocations ([141d2b2](https://github.com/aofei/wirehop/commit/141d2b2f5bd631a6a4f5518db08ec1c088287924))
* stabilize multipath throughput with bounded capacity discovery ([9764416](https://github.com/aofei/wirehop/commit/97644163fed83afa3c9061f3e11e56520fddebca))


### Code Refactoring

* compact v1 framing and coalesce queued controls ([80e9d23](https://github.com/aofei/wirehop/commit/80e9d2325d02649ffb6fc97c3ed8f2012bbb0928))
* compact v1 framing and handshake encoding ([4bf361d](https://github.com/aofei/wirehop/commit/4bf361db9706c6a11789e097df9061edfabe231f))
* reduce relay feedback and deduplication allocations ([c62c761](https://github.com/aofei/wirehop/commit/c62c76126d147a73c3524de5dcf424bafbf21c65))
* reduce relay queue scans and maintenance work ([49b25a9](https://github.com/aofei/wirehop/commit/49b25a94a12a7495ffcea6bb7bc1e7a351d888dd))
* simplify v1 admission and delivery feedback ([d2e334d](https://github.com/aofei/wirehop/commit/d2e334dc901d9c64d3f364a07441ba1e8bc60476))

## [0.2.1](https://github.com/aofei/wirehop/compare/v0.2.0...v0.2.1) (2026-09-10)


### Bug Fixes

* correct multipath scheduling estimates and Linux clock reads ([7ab2779](https://github.com/aofei/wirehop/commit/7ab2779e5cf3eb74cf2b516f3998070b007e1ffa))
* keep packet retention on the protocol clock ([d7af038](https://github.com/aofei/wirehop/commit/d7af038e9812aebd76eaf786b276d6a6167f16d7))
* preserve forwarding stability through delays and rekeys ([2872013](https://github.com/aofei/wirehop/commit/28720137561842b25dd4bb1bb11c1ceccee9a2d7))
* preserve proxy CONNECT rejection classification ([9f9cbac](https://github.com/aofei/wirehop/commit/9f9cbacfdcbc7c5a9b1176562084b3f00f502112))
* preserve sessions through transient network failures ([3635021](https://github.com/aofei/wirehop/commit/36350214f8792ce898540a935a661637f47c7de6))
* reject empty lane URL fragments and clarify relay behavior ([f55d1c1](https://github.com/aofei/wirehop/commit/f55d1c10858c1b89dea62d702b9197bcb1e5fd90))

## [0.2.0](https://github.com/aofei/wirehop/compare/v0.1.0...v0.2.0) (2026-09-09)


### Features

* add batched forwarding and resilient network recovery ([e1de96f](https://github.com/aofei/wirehop/commit/e1de96f3b308ae1a703688ff43882cc0b9e9d17d))
* add direct WireGuard UDP forwarding ([7ad5ad4](https://github.com/aofei/wirehop/commit/7ad5ad42c7f07d81b6531b5adc1ba96cde42eb14))


### Bug Fixes

* correct UDP recovery and streamline acknowledgment cleanup ([ca4dae2](https://github.com/aofei/wirehop/commit/ca4dae20b8d71988bc3c41d8c8b0d31fef79f134))
* limit the UDP GSO workaround to affected Linux releases ([c165c2c](https://github.com/aofei/wirehop/commit/c165c2c146bd21fafd5eec72e2c57a8711378e15))


### Tests

* wait for the confirmed session before reconnecting a lane ([bb56f02](https://github.com/aofei/wirehop/commit/bb56f02dd2abd4ca76e59e5d5400c23ee7e6ca88))


### Miscellaneous Chores

* bump Go to 1.27 ([fd0e6d8](https://github.com/aofei/wirehop/commit/fd0e6d88e2a90df7b42cccc10ec6ac4d59f2ac6a))
* release 0.2.0 ([c259fbe](https://github.com/aofei/wirehop/commit/c259fbe5dae63dffcc3014e64c6f66acb9540acd))

## 0.1.0 (2026-08-16)


### Features

* implement multipath WireGuard relay ([38424e1](https://github.com/aofei/wirehop/commit/38424e17f05870d210cb9caf7032dd10165930e0))


### Bug Fixes

* **laneurl:** stabilize unbracketed IPv6 diagnostics ([6ff6c41](https://github.com/aofei/wirehop/commit/6ff6c4188cb72c9867ccc4ad74dab47952c8f9dc))


### Tests

* **server:** stabilize raw rejection timing ([893a9b5](https://github.com/aofei/wirehop/commit/893a9b542a76ce662207bb4277227c9099e74cb8))


### Miscellaneous Chores

* add LICENSE ([efe43f7](https://github.com/aofei/wirehop/commit/efe43f77a4f9fb413a3a09bbd2d86fa52fa0ddeb))
* release 0.1.0 ([9e6e505](https://github.com/aofei/wirehop/commit/9e6e50503a5cb19d0536f575e4562ed3723ede9c))
* **release:** add automated release packaging ([cce04d3](https://github.com/aofei/wirehop/commit/cce04d3b8e5aaf0c95d5338e606d67469767c5ad))

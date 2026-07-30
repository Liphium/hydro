# Changelog

## 1.0.0 (2026-07-30)


### ⚠ BREAKING CHANGES

* Add outbox support for the listener dictionary to properly

### Features

* Add listener deletions ([a16ed12](https://github.com/Liphium/hydro/commit/a16ed12cb0c963ff9938c529002c69d2082b1df1))
* Add outbox support for the listener dictionary to properly ([2a0f366](https://github.com/Liphium/hydro/commit/2a0f366b02aaf1679aeaa80564da0ee0f136415c))
* **backend:** Initial architecture for mutex backends ([5359102](https://github.com/Liphium/hydro/commit/535910285ba9ea8b552e41e516b404ece9da8c83))
* **bridge:** Bridge manager and tcp bridge ([bca8a7b](https://github.com/Liphium/hydro/commit/bca8a7bfadf18cdc4bdbdcb4da6b0334e8c0ec66))
* **bridge:** UDP Bridge ([7fda84d](https://github.com/Liphium/hydro/commit/7fda84d2baedb4dc470ad714e02265e0fb305416))
* Current hydro code ([b90ffd5](https://github.com/Liphium/hydro/commit/b90ffd5ebc18cd95dd7852a6b25e783e22c87453))
* **hydro:** General structure ([6d3bf77](https://github.com/Liphium/hydro/commit/6d3bf77fb5ecd30f6f76244de02ce16fa7ecb57a))
* Initial commit ([d49557f](https://github.com/Liphium/hydro/commit/d49557fb240d5bb0aa629f1b266104759dbc9fbe))
* Listener outbox + Lots of fixes ([6791c1d](https://github.com/Liphium/hydro/commit/6791c1d38cd2bf4f43e0b7c16487a16f2f70e82e))
* **listeners:** Implement basic pub/sub for listeners using a pub/sub ([ec77194](https://github.com/Liphium/hydro/commit/ec77194bee06132b4c7baea0b20cd0bf0d5e24ba))
* PostgreSQL driver example ([50b89b6](https://github.com/Liphium/hydro/commit/50b89b68fd0a512b269fbcd9292863dd0a9109bc))
* PostgreSQL driver package ([50b89b6](https://github.com/Liphium/hydro/commit/50b89b68fd0a512b269fbcd9292863dd0a9109bc))
* **pubsub:** Add local pub/sub implementation + Worker closures in the ([2654c18](https://github.com/Liphium/hydro/commit/2654c187816b280866b93e607c9f2241e1de438b))
* **pubsub:** Outbox for transactional pub/sub ([db08e37](https://github.com/Liphium/hydro/commit/db08e376f8b323a6fe5d3bf9f3ece44e229b6636))
* **pubsub:** Pub/sub pool + Interface changes ([819b2aa](https://github.com/Liphium/hydro/commit/819b2aaf4e75631894c7a3710886069cd716c9d5))
* **pubsub:** Start on the interface ([9408947](https://github.com/Liphium/hydro/commit/9408947b037990cf149eb7031f9b8c1c97aabe1e))
* Rewrite to more generic interface ([17db882](https://github.com/Liphium/hydro/commit/17db882e46ca2ee20b3b3623c917a0a1e79cd42d))


### Bug Fixes

* Make outbox messages use bytes ([d238fd5](https://github.com/Liphium/hydro/commit/d238fd50ceda1a548a064b107a12e5cf7ccedabc))
* Make sure outboxes can only be created with the instance pubsub ([f5bb04c](https://github.com/Liphium/hydro/commit/f5bb04c84076738b26d834bc146eef3c7593dbcf))
* Make sure the outbox uses per entry events ([779193d](https://github.com/Liphium/hydro/commit/779193d0e0a164ddeffa293a7222dd7b3ccd6ef7))
* Outbox for listener dictionary ([6b965b8](https://github.com/Liphium/hydro/commit/6b965b8d27dfc29503b03ffaf640806a4e9db615))
* Remove unneeded argument ([a9994b4](https://github.com/Liphium/hydro/commit/a9994b4055a4155c400c5d8c58120fa8d86fefc5))
* Subscriptions didn't work ([b27e354](https://github.com/Liphium/hydro/commit/b27e35443738760e78dfdd6c3c39b7e3592ed506))

# Zrdb

POC data store using RESP api and [sled] as storage backend, modelled to be
a drop in replacement of [0-db].

## Testing

### Setup

These tests come from running `rzdb` with a unix socket. Data is written
by using `dd` in a [modified qemu VM], using a `blocksize` of `4K`.

### Observations

- A single threaded runtime, which handles every request in sequence (decode request,
	do DB operation, encode and send response), achieves about `70 to 80 MB/s` of write
	performance. This is similar to the original [0-db] implementation.
- After refactoring the application to run a separate DB task for every client,
	which batches the write calls, we notice 2 things:
	- Using a batch size of 1, performance stays similar to earlier recorded performance.
	- Using a larger batch size, where the batch size is **smaller** then the amount
		of chunks written, gives considerably worse performance. A batch size of 10
		results in a write throughput of roughly `15 to 20 MB/s`.
	- Using a batch size **larger** than the amount of chunks written results in a marginal
		performance increase, to about `90 MB/s`.

Note that the DB only performs the writes here when the write queue is completely full.
This means it sits idle while the client is exchanging (write) messages, which explains
why a batch size larger than 1 tanks performance (as there comes a point where a client
write will be waiting while the write queue is being flushed).

Another important observation is the limited throughput when the batch size is larger
than the amount of chunks written. Since no actual disk operations are done here,
this suggests that the remote is not able to send the chunks any faster than this.

[0-db]: https://github.com/threefoldtech/0-db
[sled]: https://github.com/spacejam/sled
[modified qemu VM]: https://github.com/threefoldtech/qemu

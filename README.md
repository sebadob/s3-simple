# s3-simple

simple, fast and efficient s3 client for bucket operations

## Why?

Why another s3 client crate? Well, there are a lot of them out there, a lot of them are unmaintained, a lot of them
have flaws, a lot of them come with lots of dependencies.

Most often, you need your bucket CRUD operations, that's it.  
This crate has been created out of the need for an efficient solution, that does not eat up all your memory for large
files while being as fast as possible.  
Quite a bit of code from the [rust-s3](https://crates.io/crates/rust-s3) crate has been reused, especially for the
headers signature. There was no need reinvent the wheel. What it does differently, it only works with async, it has
a fixed, built-in request backend (reqwest) with connection pooling and it does not provide (and never will)
all possible S3 API actions.

I tried quite a few different s3 client crates and was not fully happy with any of them so far. There were pretty good
ones, like [rusty-s3](https://crates.io/crates/rusty-s3), but I don't like using pre-signed URLs, when I don't need to,
for security reasons. Yes, you cannot guess a URL with random parts, but they get logged in lots of places where you
can simply read them.  
Other crates had the problem, that they re-created a new client for each single request, which means new TLS handshakes
for each object, even if its only 200 bytes big, which was a huge overhead. And then others again try to buffer files
of any size fully in memory before writing a single byte to disk, which OOM killed my applications a few times, since
they are often running on not that powerful big servers.

## What it offers

- fast, efficient, minimal client
- internal connection pooling with reqwest
- streaming without eating up your memory
- incomplete S3 API on purpose to reduce complexity
- only accepts connections via access key and secret
- the following currently implemented operations:
    - HEAD object for metadata
    - GET object - `S3Response` is a wrapper around `reqwest::Response`, so you can decide yourself if you
      want it in-memory or convert it to a stream
    - GET object range for partial downloads
    - DELETE an object
    - PUT an object (direct upload)
    - PUT streaming from any source that implements `AsyncRead`
    - list bucket contents
    - S3 internal copy of objects
- all operations are tested against [Minio](https://github.com/minio/minio)
  and [Garage](https://git.deuxfleurs.fr/Deuxfleurs/garage)

## How to use it

Take a look at the [examples](https://github.com/sebadob/s3-simple/tree/main/examples), but basically:

```rust
let bucket = Bucket::try_from_env() ?;

// upload
bucket.put("test.txt", b"Hello S3").await?;

// get it back
let res = bucket.get("test.txt").await?;
// no manual status code checking necessary,
// any non-success will return an S3Error
let body = res.bytes().await?;
assert_eq!(body.as_ref(), b"Hello S3");
```

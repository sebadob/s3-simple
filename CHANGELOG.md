# Changelog

## v0.6.2

Makes it possible to use the crate without any TLS at all by disabling default features.

## v0.6.1

- Removes an `.expect()` after a bad response from the S3 server during Multipart uploads, returns an error instead.
- Lowered log level during failed StreamWriter tasks to avoid double `error` logs.

## v0.6.0

You can now change the TLS provider via features. The default is still `rustls` with embedded root certs to not create a
breaking change, but you can disable default features and change it to `native-tls`, if you like.

In addition, some client functions have a `*_with` function, which lets you add additional custom headers to requests.

## v0.5.0

This version only bumps some dependency versions and applies `clippy` lints from Rust 1.89. With the dependency bumps,
the MSRV was bumped to `1.82.0` as well.

## v0.4.1

There is the new `copy_internal_from()` for a `Bucket`, which allows you to do an internal copy on the S3 storage from
a different bucket (your key has access to) into the configured one. The already existing `copy_internal()` only allowed
copies inside the same bucket.

## v0.4.0

- make internal values for `AccessKeyId`, `AccessKeySecret` and `Region` `pub` for a better DX
  when used without env vars
- added a `new()` fn for `Credentials`
- clippy lints from latest Rust version have been applied
- external deps have been updated

## v0.3.0

Any request status returned from any action will be checked early from this version on.  
Beforehand, you had to check the status yourself, because you got the full, unmodified `S3Response` to have the most
amount of freedom. This is still the case, but if the status is any non-success status, an `S3Error` will be returned
instead of the response itself. This makes it possible to get rid of any dedicated status checking on the client side,
as long as you only care about success or not.  
You will still get access to the unmodified `S3Response` and you could check the status code if you are looking for
something special, but success only is being done internally for even more ease of use.

This makes it possible to go from:

### old

```rust
let bucket = Bucket::try_from_env()?;

// upload
let res = bucket.put("test.txt", b"Hello S3").await?;
assert!(res.status().is_success());

// get it back
let res = bucket.get("test.txt").await?;
assert!(res.status().is_success());
let body = res.bytes().await?;
assert_eq!(body.as_ref(), b"Hello S3");
```

### new

```rust
let bucket = Bucket::try_from_env()?;

// upload
bucket.put("test.txt", b"Hello S3").await?;

// get it back
let res = bucket.get("test.txt").await?;
let body = res.bytes().await?;
assert_eq!(body.as_ref(), b"Hello S3");
```

## v0.2.0

- removes the `prelude` module and exposes everything at the crate root for ease of use
- internal code cleanup and reduced debug logging
- optimized ci / test recipes to not fail with multiple shared test-runtimes in parallel
- improved docs and readme
- removed accidentally added `examples/target` folder from git

## v0.1.0

This just fixes the description in the `Cargo.toml` for crates.io

## v0.1.0

Initial release with all the basic operations included and working.

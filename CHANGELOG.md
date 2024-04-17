# Changelog

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

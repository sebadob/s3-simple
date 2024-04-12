use s3_simple::*;

#[tokio::main]
async fn main() -> Result<(), S3Error> {
    dotenvy::dotenv().ok().unwrap();

    // You can create your bucket from ENV, or manually.
    // `try_from_env` expects:
    // ```
    // # optional
    // S3_DANGER_ALLOW_INSECURE=false
    // S3_REGION=home
    // S3_PATH_STYLE=true
    // S3_BUCKET=
    // S3_URL=
    // S3_ACCESS_KEY_ID=
    // S3_ACCESS_KEY_SECRET=
    // ```
    let bucket = Bucket::try_from_env().expect("env vars to be set in .env");

    let file_name = "test.txt";
    let content = b"Hello to S3";

    // upload
    let res = bucket.put(file_name, content).await?;
    assert!(res.status().is_success());

    // GET it back
    // The `S3Response` return is a wrapper around `reqwest::Response` so you can
    // do whatever you like with it. Either read it fully in memory or stream it.
    let res = bucket.get(file_name).await?;
    assert!(res.status().is_success());
    let body = res.bytes().await?;
    assert_eq!(body.as_ref(), content);

    // you can easily list the bucket content and make sure it shows up
    let list = bucket.list("/", None).await?;
    for entry in list.iter() {
        // find our bucket
        if entry.name == bucket.name {
            // search through the objects in our bucket and look for your file name
            for object in entry.contents.iter() {
                if object.key == file_name {
                    // we found our dummy object, check the size
                    assert_eq!(object.size, content.len() as u64);
                    break;
                }
            }
        }
    }

    // HEAD request for an object to get back metadata like file size
    let res = bucket.head(&file_name).await?;
    assert_eq!(res.content_length, Some(content.len() as u64));

    // S3 internal copy -> no need to download and upload again
    let copy_file_name = "test_copy.txt";
    let res = bucket.copy_internal(file_name, copy_file_name).await?;
    assert!(res.is_success());

    // GET the new copied object
    let res = bucket.get(copy_file_name).await?;
    assert!(res.status().is_success());
    let body = res.bytes().await?;
    // the copied file should have the exact same content as our original
    assert_eq!(body.as_ref(), content);

    // clean up and delete the test file
    let res = bucket.delete(file_name).await?;
    assert!(res.status().is_success());
    let res = bucket.delete(copy_file_name).await?;
    assert!(res.status().is_success());

    Ok(())
}

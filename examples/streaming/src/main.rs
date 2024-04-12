use futures_util::stream::StreamExt;
use s3_simple::*;
use std::os::unix::fs::MetadataExt;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

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

    // create and write some test data
    // You would want to stream large files most probably.
    // It does not really make sense for data < 8MiB.
    let _ = fs::create_dir_all("test_files").await;
    let file_name = "test.data";
    let path = format!("test_files/{}", file_name);

    // fill up a dummy file with bytes and write it to disk
    let file_size = 10 * 1024 * 1024; // 10 MiB
    let bytes = (0..file_size).map(|_| 0u8).collect::<Vec<u8>>();
    fs::write(&path, &bytes).await?;

    // streaming upload
    // You can provide any reader that implements `AsyncRead`.
    // `.put_stream()` will do an efficient and fast upload.
    // It has an internal buffer with the size to fit 2 chunks at once.
    // The default chunk size is 8MiB.
    // The writing side will be spawned on a dedicated tokio::task to
    // actually make use of multiple cores.
    // This means you should get maximum throughput for your network / disks
    // while usually not exceeding 2 * 8MiB memory usage, no matter how
    // large the file is.
    //
    // This works of course as well when you receive a Multipart upload
    // on your API. You can convert an incoming Multipart field to
    // a stream and directly stream the response to S3.
    let mut reader_file = File::open(&path).await?;
    let res = bucket
        .put_stream(&mut reader_file, file_name.to_string())
        .await?;
    assert!(res.status_code < 300);
    assert_eq!(res.uploaded_bytes, file_size);

    // streaming download
    let path_output = "test_files/out.data";
    let mut file = fs::File::create(path_output).await?;
    // The `S3Response` is simply a wrapper around `reqwest::Response`.
    // You can decide, if you want to buffer the body in memory or
    // convert it into a stream.
    let res = bucket.get(&file_name).await?;
    assert!(res.status().is_success());
    let stream = res.bytes_stream();
    tokio::pin!(stream);
    while let Some(Ok(item)) = stream.next().await {
        let _ = file.write(item.as_ref()).await?;
    }
    // flush / sync all possibly left over data
    file.sync_all().await?;

    // make sure the files match
    let meta_original = fs::File::open(&path).await?.metadata().await.unwrap();
    let meta_downloaded = fs::File::open(&path_output)
        .await?
        .metadata()
        .await
        .unwrap();
    assert_eq!(meta_original.size(), meta_downloaded.size());

    Ok(())
}

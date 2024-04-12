use crate::constants::LONG_DATE_TIME;
use crate::credentials::{AccessKeyId, AccessKeySecret};
use crate::error::S3Error;
use crate::Region;
use bytes::BytesMut;
use hmac::Hmac;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::header::HeaderMap;
use reqwest::Url;
use sha2::digest::Mac;
use sha2::{Digest, Sha256};
use time::macros::format_description;
use time::OffsetDateTime;

const SHORT_DATE: &[time::format_description::BorrowedFormatItem<'static>] =
    format_description!("[year][month][day]");

const FRAGMENT: &AsciiSet = &CONTROLS
    // URL_RESERVED
    .add(b':')
    .add(b'?')
    .add(b'#')
    .add(b'[')
    .add(b']')
    .add(b'@')
    .add(b'!')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b';')
    .add(b'=')
    // URL_UNSAFE
    .add(b'"')
    .add(b' ')
    .add(b'<')
    .add(b'>')
    .add(b'%')
    .add(b'{')
    .add(b'}')
    .add(b'|')
    .add(b'\\')
    .add(b'^')
    .add(b'`');

pub const FRAGMENT_SLASH: &AsciiSet = &FRAGMENT.add(b'/');

pub fn uri_encode(string: &str, encode_slash: bool) -> String {
    if encode_slash {
        utf8_percent_encode(string, FRAGMENT_SLASH).to_string()
    } else {
        utf8_percent_encode(string, FRAGMENT).to_string()
    }
}

fn canonical_uri_string(uri: &Url) -> String {
    // decode `Url`'s percent-encoding and then reencode it
    // according to AWS's rules
    let decoded = percent_encoding::percent_decode_str(uri.path()).decode_utf8_lossy();
    uri_encode(&decoded, false)
}

fn canonical_header_string(headers: &HeaderMap) -> Result<String, S3Error> {
    let mut keyvalues = Vec::with_capacity(12);
    for (key, value) in headers.iter() {
        keyvalues.push(format!(
            "{}:{}",
            key.as_str().to_lowercase(),
            value.to_str()?.trim()
        ))
    }
    keyvalues.sort();
    Ok(keyvalues.join("\n"))
}

fn canonical_query_string(uri: &Url) -> String {
    let mut keyvalues: Vec<(String, String)> = uri
        .query_pairs()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    keyvalues.sort();
    let keyvalues: Vec<String> = keyvalues
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                utf8_percent_encode(k, FRAGMENT_SLASH),
                utf8_percent_encode(v, FRAGMENT_SLASH)
            )
        })
        .collect();
    keyvalues.join("&")
}

pub fn signed_header_string(headers: &HeaderMap) -> String {
    let mut keys = headers
        .keys()
        .map(|key| key.as_str().to_lowercase())
        .collect::<Vec<String>>();
    keys.sort();
    keys.join(";")
}

pub fn canonical_request(
    method: &http::Method,
    host: &Url,
    headers: &HeaderMap,
    sha256: &str,
) -> Result<String, S3Error> {
    Ok(format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        method.as_str(),
        canonical_uri_string(host),
        canonical_query_string(host),
        canonical_header_string(headers)?,
        signed_header_string(headers),
        sha256
    ))
}

fn scope_string(datetime: &OffsetDateTime, region: &Region) -> Result<String, S3Error> {
    Ok(format!(
        "{}/{}/s3/aws4_request",
        datetime.format(SHORT_DATE)?,
        region.as_str(),
    ))
}

pub fn string_to_sign(
    datetime: &OffsetDateTime,
    region: &Region,
    canonical_req: &[u8],
) -> Result<String, S3Error> {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req);
    let string_to = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        datetime.format(LONG_DATE_TIME)?,
        scope_string(datetime, region)?,
        hex::encode(hasher.finalize().as_slice())
    );
    Ok(string_to)
}

pub fn signing_key(
    datetime: &OffsetDateTime,
    secret_key: &AccessKeySecret,
    region: &Region,
) -> Result<Vec<u8>, S3Error> {
    let mut secret = BytesMut::with_capacity(72);
    secret.extend(b"AWS4");
    secret.extend(secret_key.as_ref().as_bytes());

    let mut date_hmac = Hmac::<Sha256>::new_from_slice(secret.as_ref())?;
    date_hmac.update(datetime.format(SHORT_DATE)?.as_bytes());

    let mut region_hmac = Hmac::<Sha256>::new_from_slice(&date_hmac.finalize().into_bytes())?;
    region_hmac.update(region.as_str().as_bytes());

    let mut service_hmac = Hmac::<Sha256>::new_from_slice(&region_hmac.finalize().into_bytes())?;
    service_hmac.update(b"s3");

    let mut signing_hmac = Hmac::<Sha256>::new_from_slice(&service_hmac.finalize().into_bytes())?;
    signing_hmac.update(b"aws4_request");

    Ok(signing_hmac.finalize().into_bytes().to_vec())
}

pub fn authorization_header(
    access_key: &AccessKeyId,
    datetime: &OffsetDateTime,
    region: &Region,
    signed_headers: &str,
    signature: &str,
) -> Result<String, S3Error> {
    Ok(format!(
        "AWS4-HMAC-SHA256 Credential={}/{},\
            SignedHeaders={},Signature={}",
        access_key.as_ref(),
        scope_string(datetime, region)?,
        signed_headers,
        signature,
    ))
}

// fn authorization_query_params_no_sig(
//     access_key: &AccessKeyId,
//     datetime: &OffsetDateTime,
//     region: &Region,
//     expires: u32,
//     custom_headers: Option<&HeaderMap>,
//     token: Option<&str>,
// ) -> Result<String, S3Error> {
//     let credentials = format!(
//         "{}/{}",
//         access_key.as_ref(),
//         scope_string(datetime, region)?
//     );
//     let credentials = utf8_percent_encode(&credentials, FRAGMENT_SLASH);
//
//     let mut signed_headers = vec!["host".to_string()];
//
//     if let Some(custom_headers) = &custom_headers {
//         for k in custom_headers.keys() {
//             signed_headers.push(k.to_string())
//         }
//     }
//
//     signed_headers.sort();
//     let signed_headers = signed_headers.join(";");
//     let signed_headers = utf8_percent_encode(&signed_headers, FRAGMENT_SLASH);
//
//     let mut query_params = format!(
//         "?X-Amz-Algorithm=AWS4-HMAC-SHA256\
//             &X-Amz-Credential={}\
//             &X-Amz-Date={}\
//             &X-Amz-Expires={}\
//             &X-Amz-SignedHeaders={}",
//         credentials,
//         datetime.format(LONG_DATE_TIME)?,
//         expires,
//         signed_headers,
//     );
//
//     if let Some(token) = token {
//         write!(
//             query_params,
//             "&X-Amz-Security-Token={}",
//             utf8_percent_encode(token, FRAGMENT_SLASH)
//         )
//         .expect("Could not write token");
//     }
//
//     Ok(query_params)
// }

// fn flatten_queries(queries: Option<&HashMap<String, String>>) -> Result<String, S3Error> {
//     match queries {
//         None => Ok(String::new()),
//         Some(queries) => {
//             let mut query_str = String::new();
//             for (k, v) in queries {
//                 write!(
//                     query_str,
//                     "&{}={}",
//                     utf8_percent_encode(k, FRAGMENT_SLASH),
//                     utf8_percent_encode(v, FRAGMENT_SLASH),
//                 )?;
//             }
//             Ok(query_str)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::str;

    use http::header::{HeaderName, HOST, RANGE};
    use http::HeaderMap;
    use time::Date;
    use url::Url;

    use super::*;

    #[test]
    fn test_base_url_encode() {
        // Make sure parsing doesn't remove extra slashes, as normalization
        // will mess up the path lookup.
        let url = Url::parse("http://s3.amazonaws.com/examplebucket///foo//bar//baz").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!("/examplebucket///foo//bar//baz", canonical);
    }

    #[test]
    fn test_path_encode() {
        let url = Url::parse("http://s3.amazonaws.com/bucket/Filename (xx)%=").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!("/bucket/Filename%20%28xx%29%25%3D", canonical);
    }

    #[test]
    fn test_path_slash_encode() {
        let url =
            Url::parse("http://s3.amazonaws.com/bucket/Folder (xx)%=/Filename (xx)%=").unwrap();
        let canonical = canonical_uri_string(&url);
        assert_eq!(
            "/bucket/Folder%20%28xx%29%25%3D/Filename%20%28xx%29%25%3D",
            canonical
        );
    }

    #[test]
    fn test_query_string_encode() {
        let url = Url::parse(
            "http://s3.amazonaws.com/examplebucket?prefix=somePrefix&marker=someMarker&max-keys=20",
        )
        .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("marker=someMarker&max-keys=20&prefix=somePrefix", canonical);

        let url = Url::parse("http://s3.amazonaws.com/examplebucket?acl").unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("acl=", canonical);

        let url = Url::parse(
            "http://s3.amazonaws.com/examplebucket?key=with%20space&also+space=with+plus",
        )
        .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("also%20space=with%20plus&key=with%20space", canonical);

        let url =
            Url::parse("http://s3.amazonaws.com/examplebucket?key-with-postfix=something&key=")
                .unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("key=&key-with-postfix=something", canonical);

        let url = Url::parse("http://s3.amazonaws.com/examplebucket?key=c&key=a&key=b").unwrap();
        let canonical = canonical_query_string(&url);
        assert_eq!("key=a&key=b&key=c", canonical);
    }

    #[test]
    fn test_headers_encode() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            "20130708T220855Z".parse().unwrap(),
        );
        headers.insert(HeaderName::from_static("foo"), "bAr".parse().unwrap());
        headers.insert(HOST, "s3.amazonaws.com".parse().unwrap());
        let canonical = canonical_header_string(&headers).unwrap();
        let expected = "foo:bAr\nhost:s3.amazonaws.com\nx-amz-date:20130708T220855Z";
        assert_eq!(expected, canonical);

        let signed = signed_header_string(&headers);
        assert_eq!("foo;host;x-amz-date", signed);
    }

    #[test]
    fn test_signing_key() {
        let key = AccessKeySecret::new("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string());
        let expected = "32f78051dcde24c552811d654f4a769112bb834b03975cdd6b1fd7d16248c269";
        let datetime = Date::from_calendar_date(2015, 8.try_into().unwrap(), 30)
            .unwrap()
            .with_hms(0, 0, 0)
            .unwrap()
            .assume_utc();
        let signature = signing_key(&datetime, &key, &Region("us-east-1".to_string())).unwrap();
        assert_eq!(expected, hex::encode(signature));
    }

    const EXPECTED_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[rustfmt::skip]
    const EXPECTED_CANONICAL_REQUEST: &str =
        "GET\n\
         /test.txt\n\
         \n\
         host:examplebucket.s3.amazonaws.com\n\
         range:bytes=0-9\n\
         x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
         x-amz-date:20130524T000000Z\n\
         \n\
         host;range;x-amz-content-sha256;x-amz-date\n\
         e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[rustfmt::skip]
    const EXPECTED_STRING_TO_SIGN: &str =
        "AWS4-HMAC-SHA256\n\
         20130524T000000Z\n\
         20130524/us-east-1/s3/aws4_request\n\
         7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";

    #[test]
    fn test_signing() {
        let url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            "20130524T000000Z".parse().unwrap(),
        );
        headers.insert(RANGE, "bytes=0-9".parse().unwrap());
        headers.insert(HOST, "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            EXPECTED_SHA.parse().unwrap(),
        );
        let canonical =
            canonical_request(&http::Method::GET, &url, &headers, EXPECTED_SHA).unwrap();
        assert_eq!(EXPECTED_CANONICAL_REQUEST, canonical);

        let datetime = Date::from_calendar_date(2013, 5.try_into().unwrap(), 24)
            .unwrap()
            .with_hms(0, 0, 0)
            .unwrap()
            .assume_utc();
        let string_to_sign = string_to_sign(
            &datetime,
            &Region("us-east-1".to_string()),
            canonical.as_bytes(),
        )
        .unwrap();
        assert_eq!(EXPECTED_STRING_TO_SIGN, string_to_sign);

        let expected = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
        let secret = AccessKeySecret::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let signing_key = signing_key(&datetime, &secret, &Region("us-east-1".to_string()));
        let mut hmac = Hmac::<Sha256>::new_from_slice(&signing_key.unwrap()).unwrap();
        hmac.update(string_to_sign.as_bytes());
        assert_eq!(expected, hex::encode(hmac.finalize().into_bytes()));
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode(r#"~!@#$%^&*()-_=+[]\{}|;:'",.<>? привет 你好"#, true), "~%21%40%23%24%25%5E%26%2A%28%29-_%3D%2B%5B%5D%5C%7B%7D%7C%3B%3A%27%22%2C.%3C%3E%3F%20%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82%20%E4%BD%A0%E5%A5%BD");
    }
}

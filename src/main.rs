use lazy_static::lazy_static;
use regex::Regex;
use reqwest::Client;

#[tokio::main]
async fn main() {
    let client = Client::builder().build().expect("failed to create client");
    let resp = client
        .head("https://alist.gloryouth.com/d/EDU/%E5%AF%B9%E5%A4%96/%E6%88%90%E7%89%87/%E8%8A%82%E7%9B%AE1_%E6%94%B9_ai.mp4")
        .send()
        .await.expect("error during request");
    let v = resp
        .headers()
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok())
        .expect("Content-Length is invalid");
    println!("{:?}", v);
    return;
    match resp.headers().get("alt-svc") {
        None => {
            let v = resp
                .headers()
                .get("Content-Length")
                .and_then(|v| v.to_str().ok());
            println!("{:?}", v);
        }
        Some(str) => {
            lazy_static! {
                static ref HASHTAG_REGEX: Regex =
                    Regex::new(r#"h3="([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
            }
            println!(
                "{}",
                HASHTAG_REGEX.find(&str.to_str().unwrap()).unwrap().as_str()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use regex::Regex;

    #[test]
    fn test_parse() {
        lazy_static! {
            static ref HASHTAG_REGEX: Regex =
                Regex::new(r#"h3="([1-9][0-9]{1,3}|[1-9]|[1-5][0-9]{4}|6553[0-5])""#).unwrap();
        }

        for i in -1000000_i64..=1000000_i64 {
            let str = format!(r#"h3="{}""#, i);
            if let Some(v) = HASHTAG_REGEX
                .captures(str.as_str())
                .and_then(|m| m.get(1))
                .and_then(|m| {
                    if i > 0 && i <= u16::MAX as i64 {
                        m.as_str().parse::<u16>().ok().map(|u| u as i64)
                    } else {
                        (!m.as_str().parse::<u16>().is_ok()).then_some(i)
                    }
                })
            {
                assert_eq!(v, i);
            }
        }
    }
}

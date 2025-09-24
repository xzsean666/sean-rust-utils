use sean_rust_utils::web2_utils::curl_helper::curl_to_reqwest;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let curl_command = r#"curl https://httpbin.org/get"#;

    println!("Executing curl command: {}", curl_command);
    let result = curl_to_reqwest(curl_command).await?;

    println!("Response: {:#?}", result);

    Ok(())
}

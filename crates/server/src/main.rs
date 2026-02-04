use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    eagle_server::run().await
}

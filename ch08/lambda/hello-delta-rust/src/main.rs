use lambda_http::{run, service_fn, Body, Error, Request, Response};
use serde_json::json;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(_event: Request) -> Result<Response<Body>, Error> {
    let table_url = std::env::var("TABLE_URL").expect("must set TABLE_URL in env");

    let response = match deltalake_core::open_table(&table_url).await {
        Ok(table) => {
            let files: Vec<String> = table
                .get_files_iter()?
                .map(|p| p.as_ref().to_string())
                .collect();

            let message = json!({
                    "table" : table_url,
                    "version" : table.version(),
                    "metadata" : table.metadata()?,
                    "files" : files,
            });
            Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(message.to_string().into())
                .map_err(Box::new)?
        }
        Err(e) => Response::builder()
            .status(500)
            .header("Content-Type", "text/plain")
            .body(format!("error: {e:?}").into())
            .map_err(Box::new)?,
    };

    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    deltalake_aws::register_handlers(None);
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}

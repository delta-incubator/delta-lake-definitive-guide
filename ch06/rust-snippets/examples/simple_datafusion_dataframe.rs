use deltalake::arrow::util::pretty::print_batches;
use deltalake::datafusion::execution::context::SessionContext;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("../data/deltatbl-partitioned").await?;

    let df = ctx.read_table(Arc::new(table))?;

    let _ = print_batches(
        &df.limit(0, Some(5))?
            .collect()
            .await
            .expect("Failed to limit"),
    );
    Ok(())
}

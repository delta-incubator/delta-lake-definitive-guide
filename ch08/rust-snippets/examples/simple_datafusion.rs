use deltalake::arrow::util::pretty::print_batches;
use deltalake::datafusion::execution::context::SessionContext;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();
    let table = deltalake::open_table("../data/deltatbl-partitioned")
        .await
        .unwrap();
    ctx.register_table("demo", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT * FROM demo LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    print_batches(&batches).expect("Failed to print batches");
}

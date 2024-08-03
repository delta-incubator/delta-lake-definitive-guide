///
/// This example performs a simple read of a delta table
///

#[tokio::main]
async fn main() {
    println!(">> Loading `deltatbl-partitioned`");
    let table = deltalake::open_table("../data/deltatbl-partitioned")
        .await
        .expect("Failed to open table");
    println!("..loaded version {}", table.version());
    for file in table.get_files_iter() {
        println!(" - {}", file.as_ref());
    }
}

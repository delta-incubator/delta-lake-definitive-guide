use tracing::info;
use deltalake::Path;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::*;
use std::env::{set_var, var};
use deltalake::errors::DeltaTableError;

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    info!("Logger initialized");
    set_var("USE_SSL", "0");
    set_var("AWS_ENDPOINT_URL", "http://0.0.0.0:4566");
    set_var("AWS_ACCESS_KEY_ID", "test");
    set_var("AWS_SECRET_ACCESS_KEY", "test");
    set_var("AWS_DEFAULT_REGION", "us-east-1");
    set_var("AWS_S3_BUCKET", "dldg");
    /* note: you can point this to anywhere you want your fresh delta table to be created
      // set_var("TABLE_URI", "data/ecomm-ingest");
    */
    let table_uri = std::env::var("TABLE_URI").map_err(|e| DeltaTableError::GenericError {
        source: Box::new(e),
    })?;
    info!("Using the location of: {:?}", table_uri);
    let table_path: Path = Path::parse(&table_uri)?;
    
    let maybe_table = open_table(&table_path).await;
    let _table = match maybe_table {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            info!("It doesn't look like our delta table has been created {:?}", table_path);
            create_empty_table(&table_path).await
        }
        Err(err) => {
            panic!("There is a problem with our table {:?} {:?}", table_path, err)
        }
    };

    return Ok(());
}

/* 
{
    "event_time": "2024-04-25T00:00:00Z",
    "event_type": "view",
    "product_id": 4782,
    "category_id": 2053013552326770905,
    "category_code": "appliances.environment.water_heater",
    "brand": "heater3",
    "price": 2789.0,
    "user_id": 195,
    "user_session": "19ae88e1-4a02-4b57-94a8-a46f6c6c60c4"
}
 */
struct EcommRecord {
    event_time: String,
    event_type: String,
    product_id: i32,
    category_id: i64,
    category_code: String,
    brand: String,
    price: f32,
    user_id: i32,
    user_session: String,
}

impl EcommRecord {
    fn columns() -> Vec<StructField> {
        vec![
            StructField::new(
                "event_time".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "event_type".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "product_id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "category_id".to_string(),
                DataType::Primitive(PrimitiveType::Long),
                true,
            ),
            StructField::new(
                "category_code".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "brand".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                "price".to_string(),
                DataType::Primitive(PrimitiveType::Float),
                true,
            ),
            StructField::new(
                "user_id".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "user_session".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            )
        ]
    }
}

async fn create_empty_table(table_path: &Path) -> DeltaTable {
    DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .create()
        .with_columns(EcommRecord::columns())
        .await
        .unwrap()
}

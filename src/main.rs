use std::path::PathBuf;

use clap::Parser;
use rand::Rng;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::{Surreal, engine::remote::ws::Client};

#[derive(Parser)]
#[command(name = "Parallel Surrealdb Import")]
#[command(bin_name = "parallel-surreal")]
#[command(color = clap::ColorChoice::Always)]
#[command(about = "Import data into SurrealDB in parallel")]
#[command(author = "Salvador Guzman")]
#[command(version = "1.0")]
#[command(long_about = None)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long)]
    input: PathBuf,

    // Name of surreal user
    #[arg(short, long, default_value = "root")]
    user: String,

    // Password for surreal user
    #[arg(short, long, default_value = "root")]
    password: String,

    // Name of surreal host
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    // Port of surreal host
    #[arg(short, long, default_value = "8000")]
    port: String,

    // Name of surreal namespace
    #[arg(short, long, default_value = "test")]
    ns: String,

    // Name of surreal database
    #[arg(short, long, default_value = "test")]
    db: String,

    // Name of surreal table
    #[arg(short, long)]
    table: Option<String>,

    // Number of threads
    #[arg(short, long, default_value_t = 8)]
    threads: usize,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArxivEntry {
    // Since abstract is a reserved word in Rust, we use `abstract_text` instead
    #[serde(rename = "abstract")]
    pub abstract_text: Option<String>,
    pub authors: Option<String>,
    pub authors_parsed: Vec<Vec<String>>,
    pub categories: Option<String>,
    pub comments: Option<String>,
    pub doi: Option<String>,
    pub journal_ref: Option<String>,
    pub license: Option<String>,
    pub report_no: Option<String>,
    pub submitter: Option<String>,
    pub title: Option<String>,
    pub update_date: Option<String>,
    pub versions: Vec<Version>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version {
    pub created: String,
    pub version: String,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    let cli = Cli::parse();

    // Random table name
    let table = generate_random_string();

    // Print the input data
    println!("Arguments: {:#?}", cli);

    let json_data = load_data(&cli.input);

    // Create a vector to hold all the task handles
    let mut tasks = Vec::new();

    for i in 1..=cli.threads {
        let input_clone = cli.clone();

        let table = table.clone();
        let slice = get_slice(json_data.clone(), i, cli.threads);
        let task = tokio::spawn(async move {
            match insert_items(i, table, &input_clone, &index_clone, &slice).await {
                Ok(_) => println!(
                    "Thread {}: Inserted {} items for index: {}",
                    i,
                    slice.len(),
                    index_clone
                ),
                Err(e) => eprintln!(
                    "Thread {}: Failed to insert items for index {}: {}",
                    i, index_clone, e
                ),
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        if let Err(e) = task.await {
            eprintln!("Task failed: {}", e);
        }
    }

    println!("Goodbye, world!");
    Ok(())
}

// Task of insertion into SurrealDB a single item
// Should initialize a new client
// and insert the item into the index
async fn insert_items(
    thread_id: usize,
    table: String,
    cli: &Cli,
    name: &str,
    item: &Vec<ArxivEntry>,
) -> surrealdb::Result<()> {
    println!(
        "Thread {}: Inserting {} items into index {}",
        thread_id,
        item.len(),
        name
    );
    let db = build_connection(cli).await;

    // Pass the struct directly instead of serializing it
    db.resource(table).content(&item).await?;
    Ok(())
}

// Generate a random 5 letter string
fn generate_random_string() -> String {
    let chars = "abcdefghijklmnopqrstuvwxyz";
    let random_string: String = (0..5)
        .map(|_| {
            let idx = rand::rng().random_range(0..chars.len());
            chars.chars().nth(idx).unwrap()
        })
        .collect();
    random_string
}

// Load JSON data from a file
fn load_data(path: &PathBuf) -> Vec<ArxivEntry> {
    let data = std::fs::read_to_string(path).expect("Failed to read file");
    let data: Vec<ArxivEntry> = serde_json::from_str(&data).expect("Failed to parse JSON");
    data
}

// Given a thread, get a slice of the data starting from the thread's index
fn get_slice(data: Vec<ArxivEntry>, thread: usize, num_threads: usize) -> Vec<ArxivEntry> {
    let start = (thread - 1) * data.len() / num_threads;
    let end = thread * data.len() / num_threads;
    data[start..end].to_vec()
}

async fn build_connection(cli: &Cli) -> Surreal<Client> {
    let address = format!("ws://{}:{}", cli.host, cli.port);
    let db = Surreal::new::<Ws>(address).await.unwrap();

    // Signin as a namespace, database, or root user
    db.signin(Root {
        username: &cli.user,
        password: &cli.password,
    })
    .await
    .unwrap();

    // Select a specific namespace / database
    db.use_ns(&cli.ns).use_db(&cli.db).await.unwrap();

    db
}

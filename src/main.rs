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
    #[arg(long)]
    input: PathBuf,

    // Name of surreal user
    #[arg(long, default_value = "root")]
    user: String,

    // Password for surreal user
    #[arg(long, default_value = "root")]
    password: String,

    // Name of surreal host
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    // Port of surreal host
    #[arg(long, default_value = "8000")]
    port: String,

    // Name of surreal namespace
    #[arg(long, default_value = "test")]
    ns: String,

    // Name of surreal database
    #[arg(long, default_value = "test")]
    db: String,

    // Name of surreal table
    #[arg(long)]
    table: Option<String>,

    // Number of threads
    #[arg(long, default_value_t = 8)]
    threads: usize,

    // Size of the chunk
    #[arg(long, default_value_t = 1000)]
    chunk_size: usize,
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
    pub id: u32,
    pub journal_ref: Option<String>,
    pub license: Option<String>,
    pub report_no: Option<String>,
    pub submitter: Option<String>,
    pub title: Option<String>,
    pub update_date: Option<String>,
    pub versions: Vec<Version>,
}

// ArxivEntry struct without the id field
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArxivEntry2 {
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

// Map the ArxivEntry struct to the ArxivEntry2 struct
impl From<ArxivEntry> for ArxivEntry2 {
    fn from(entry: ArxivEntry) -> Self {
        ArxivEntry2 {
            abstract_text: entry.abstract_text,
            authors: entry.authors,
            authors_parsed: entry.authors_parsed,
            categories: entry.categories,
            comments: entry.comments,
            doi: entry.doi,
            journal_ref: entry.journal_ref,
            license: entry.license,
            report_no: entry.report_no,
            submitter: entry.submitter,
            title: entry.title,
            update_date: entry.update_date,
            versions: entry.versions,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Version {
    pub created: String,
    pub version: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> surrealdb::Result<()> {
    let cli = Cli::parse();

    // Random table name
    let table = generate_random_string();

    // Print table name
    println!("Table name: {}", table);

    // Print the input data
    println!("Arguments: {:#?}", cli);

    let json_data = load_data(&cli.input);

    // Print the number of items to be inserted
    println!("Number of items to be inserted: {}", json_data.len());

    // Create a vector to hold all the task handles
    let mut tasks = Vec::new();
    let num_threads = cli.threads.clone();
    let table = if cli.table.is_some() {
        cli.table.clone().unwrap()
    } else {
        table
    };

    for i in 1..=cli.threads {
        let table = table.clone();
        let cli = cli.clone();

        let slice = get_slice(json_data.clone(), i, num_threads);
        let task = tokio::spawn(async move {
            match insert_items(i, table.clone(), &cli, &slice).await {
                Ok(_) => println!("Thread {}: Done", i),
                Err(e) => eprintln!("Thread {}: Failed to insert items: {}", i, e),
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
    items: &Vec<ArxivEntry>, // Renamed for clarity
) -> Result<(), surrealdb::Error> {
    if items.is_empty() {
        println!("Thread {}: No items to insert", thread_id);
        return Ok(());
    }

    // Convert the items to the new struct
    let items: Vec<ArxivEntry2> = items.iter().map(|item| item.clone().into()).collect();

    // --- Start Modification ---
    let db = build_connection(cli).await;
    let chunk_size = cli.chunk_size; // Adjust this value based on testing (start smaller, e.g., 100, 500, 1000)

    for (i, chunk) in items.chunks(chunk_size).enumerate() {
        println!(
            "Thread {}: Inserting chunk {}/{} ({} items)",
            thread_id,
            i + 1,
            (items.len() + chunk_size - 1) / chunk_size, // Calculate total chunks
            chunk.len()
        );

        match db
            .insert::<Vec<ArxivEntry2>>(&table) // Pass table name as reference
            .content(chunk.to_vec()) // Send only the current chunk
            .await
        {
            Ok(_) => {
                // Optional: Success log per chunk if needed
            }
            Err(e) => {
                // Log the specific chunk that failed
                eprintln!(
                    "Thread {}: Failed insertion on chunk {}: {}",
                    thread_id,
                    i + 1,
                    e
                );
                // Decide if you want to stop the thread on first error or continue
                return Err(e); // Stop and propagate error
            }
        }
        // Optional: Add a small delay between chunks if needed, e.g., for rate limiting
        // tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    // --- End Modification ---

    println!("Thread {}: Finished inserting all chunks", thread_id);
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
    let address = format!("{}:{}", cli.host, cli.port);
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

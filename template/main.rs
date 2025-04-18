use serde_json::{Value, json};
use std::fs;
use std::process;

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "JSON ID Adder")]
#[command(bin_name = "import-meil")]
#[command(color = clap::ColorChoice::Always)]
#[command(about = "Adds a unique ID to each object in a JSON array")]
#[command(author = "Salvador Guzman")]
#[command(version = "1.0")]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(long)]
    input: PathBuf,

    /// Optional key name
    #[arg(long)]
    #[arg(default_value = "id")]
    name: String,
}

fn main() {
    let matches = Cli::parse();

    // Get the input JSON file
    let input = matches.input;

    // Get set name of key if provided - default to "id"
    let key_name = matches.name;

    // Read the JSON input (either from a file or directly as a string)
    let input_json = match fs::read_to_string(&input) {
        Ok(content) => content,
        Err(_) => {
            eprintln!("Failed to read input JSON file.");
            process::exit(1);
        }
    };

    // Parse the JSON array
    let mut json_array: Vec<Value> = match serde_json::from_str(&input_json) {
        Ok(array) => array,
        Err(_) => {
            eprintln!("Invalid JSON array provided.");
            process::exit(1);
        }
    };

    // Add a unique `id` field to each object in the array
    for (index, object) in json_array.iter_mut().enumerate() {
        let id = index + 1;
        object[key_name.as_str()] = json!(id);
    }

    // Print the modified JSON array
    println!("{}", serde_json::to_string_pretty(&json_array).unwrap());
}

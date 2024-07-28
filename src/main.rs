use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use csv::Writer;
use futures::stream::{self, StreamExt};
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use walkdir::WalkDir;

#[tokio::main]
async fn main() -> Result<()> {
    let images_folder = Path::new("IMAGES");
    let csv_path = Path::new("api_responses.csv");
    let csv_writer = Arc::new(Mutex::new(create_csv_writer(csv_path)?));

    let image_paths: Vec<PathBuf> = WalkDir::new(images_folder)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file() && is_image(e.path()))
        .map(|e| e.path().to_owned())
        .collect();

    println!("Found {} images", image_paths.len());

    let tasks = stream::iter(image_paths)
        .map(|path| {
            let csv_writer = Arc::clone(&csv_writer);
            let path_clone = path.clone();
            tokio::spawn(async move {
                match process_image(path_clone, csv_writer).await {
                    Ok(_) => println!("Successfully processed {:?}", path),
                    Err(e) => eprintln!("Error processing {:?}: {}", path, e),
                }
            })
        })
        .buffer_unordered(10); // Process up to 10 images concurrently

    tasks.for_each(|_| async {}).await;

    Ok(())
}

async fn process_image(path: PathBuf, csv_writer: Arc<Mutex<Writer<File>>>) -> Result<()> {
    let path_str = path.to_string_lossy().into_owned();
    let image_base64 = tokio::task::spawn_blocking(move || encode_image(&path)).await??;
    let response = send_to_api(&image_base64).await?;

    let mut writer = csv_writer.lock().await;
    writer.write_record(&[path_str, response.to_string()])?;
    writer.flush()?;

    Ok(())
}

fn create_csv_writer(path: &Path) -> Result<Writer<File>> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)
        .context("Failed to open or create CSV file")?;

    let writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    Ok(writer)
}

fn is_image(path: &Path) -> bool {
    let extensions = ["jpg", "jpeg", "png", "gif", "bmp"];
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| extensions.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

fn encode_image(path: &Path) -> Result<String> {
    let mut file = File::open(path).context("Failed to open image file")?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)
        .context("Failed to read image file")?;
    Ok(general_purpose::STANDARD.encode(buffer))
}

async fn send_to_api(image_base64: &str) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let payload = json!({
        "image_base64": image_base64
    });

    let response = client
        .post("http://194..163.14:8995/ping")
        .json(&payload)
        .send()
        .await
        .context("Failed to send request to API")?;

    let json_response = response
        .json()
        .await
        .context("Failed to parse API response")?;
    Ok(json_response)
}

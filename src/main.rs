use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::types::CreateBucketConfiguration;
use futures::stream::{self, StreamExt};
use std::path::{Path, PathBuf};
use tokio::fs;

async fn create_bucket(s3_client: &aws_sdk_s3::Client, bucket_name: &str) -> Result<()> {
    match s3_client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            println!("Bucket {} already exists", bucket_name);
            Ok(())
        }
        Err(e) => {
            // If error is NoSuchBucket, create the bucket
            // Otherwise, propagate the error
            if e.to_string().contains("404") {
                s3_client
                    .create_bucket()
                    .bucket(bucket_name)
                    .create_bucket_configuration(
                        CreateBucketConfiguration::builder()
                            .location_constraint(
                                aws_sdk_s3::types::BucketLocationConstraint::UsWest2,
                            )
                            .build(),
                    )
                    .send()
                    .await
                    .context("Failed to create bucket")?;
                println!("Created bucket: {}", bucket_name);
                Ok(())
            } else {
                Err(anyhow::anyhow!("Error checking bucket: {}", e))
            }
        }
    }
}

async fn upload_file(
    s3_client: &aws_sdk_s3::Client,
    bucket_name: &str,
    file_path: &Path,
) -> Result<()> {
    let file_name = file_path
        .file_name()
        .context("Invalid file name")?
        .to_str()
        .context("Invalid file name encoding")?;
    let content = fs::read(file_path).await.context("Failed to read file")?;

    s3_client
        .put_object()
        .bucket(bucket_name)
        .key(file_name)
        .body(content.into())
        .send()
        .await
        .context(format!("Failed to upload file: {}", file_name))?;

    println!("Uploaded file: {}", file_name);
    Ok(())
}

async fn collect_files(dir_path: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut entries = fs::read_dir(dir_path)
        .await
        .context("Failed to read directory")?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .context("Failed to read directory entry")?
    {
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.ends_with(".parquet")
                    && file_name.starts_with("erc20_transfers_arbitrum_")
                {
                    files.push(path);
                }
            }
        }
    }
    Ok(files)
}

async fn process_directory(
    s3_client: &aws_sdk_s3::Client,
    bucket_name: &str,
    dir_path: &str,
    concurrent_uploads: usize,
) -> Result<()> {
    // First collect all eligible files
    let files = collect_files(dir_path).await?;
    let total_files = files.len();
    println!("Found {} files to upload", total_files);

    // Process files concurrently with bounded parallelism
    let mut successes = 0;
    let mut failures = 0;

    let results = stream::iter(files)
        .map(|file| {
            let client = s3_client.clone();
            let bucket = bucket_name.to_string();
            async move {
                match upload_file(&client, &bucket, &file).await {
                    Ok(()) => (file, true),
                    Err(e) => {
                        eprintln!("Error uploading {:?}: {}", file, e);
                        (file, false)
                    }
                }
            }
        })
        .buffer_unordered(concurrent_uploads)
        .collect::<Vec<_>>()
        .await;

    // Count results
    for (_file, success) in results {
        if success {
            successes += 1;
        } else {
            failures += 1;
        }
    }

    println!(
        "Upload complete: {} successful, {} failed",
        successes, failures
    );

    if failures > 0 {
        Err(anyhow::anyhow!("{} files failed to upload", failures))
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    println!("AWS Region: {:?}", config.region().unwrap());
    println!(
        "AWS credentials loaded: {}",
        config.credentials_provider().is_some()
    );

    let bucket_name = "arbitrum-erc20-transfer-events";

    // Create the bucket
    // create_bucket(&s3_client, bucket_name).await?;

    // Get the directory path from command line arguments
    let dir_path = std::env::args()
        .nth(1)
        .context("Please provide a directory path as an argument")?;

    // Process the directory and upload matching files
    // Using 10 concurrent uploads, adjust as needed
    process_directory(&s3_client, bucket_name, &dir_path, 10).await?;

    println!("All operations completed successfully");
    Ok(())
}

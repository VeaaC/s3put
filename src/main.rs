use aws_sdk_s3 as s3;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use clap::Parser;
use crossbeam::channel;
use http::StatusCode;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

fn parse_size(x: &str) -> anyhow::Result<usize> {
    let x = x.to_ascii_lowercase();
    if let Some(value) = x.strip_suffix("gb") {
        return Ok(usize::from_str(value)? * 1024 * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("mb") {
        return Ok(usize::from_str(value)? * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("kb") {
        return Ok(usize::from_str(value)? * 1024);
    }
    anyhow::bail!("Cannot parse size: '{}'", x)
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// S3 path to upload to
    s3_path: String,

    /// Input file name
    #[arg(long, short)]
    input: Option<PathBuf>,

    /// Block size used for data uploads
    #[arg(long, default_value = "32MB", value_parser = parse_size)]
    block_size: usize,

    /// Number of threads to use, defaults to number of logical cores
    #[arg(long, short, default_value = "6")]
    threads: usize,

    /// Print verbose information, statistics, etc
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Determines how often each chunk should be retried before giving up
    #[arg(long, default_value = "4")]
    max_retries: u32,
}

async fn start_upload(
    bucket: &str,
    key: &str,
    verbose: u8,
) -> anyhow::Result<(aws_config::SdkConfig, String)> {
    let config = aws_config::load_from_env().await;
    let region = config.region().cloned();
    let mut config = config
        .into_builder()
        .region(region.or_else(|| Some(s3::config::Region::new("us-east-2"))))
        .build();

    for _ in 0..3 {
        let client = s3::Client::new(&config);
        let response = match client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(x) => x,
            Err(e) => {
                if verbose > 1 {
                    eprintln!("{:?}", e);
                }
                if let s3::error::SdkError::ServiceError(response) = &e {
                    if response.raw().status().as_u16() == StatusCode::MOVED_PERMANENTLY {
                        if let Some(x) = response.raw().headers().get("x-amz-bucket-region") {
                            config = config
                                .into_builder()
                                .region(Some(s3::config::Region::new(x.to_string())))
                                .build();
                            if verbose > 0 {
                                eprintln!("Redirected to {}", x);
                            }
                            continue;
                        }
                    }
                }
                return Err(e.into());
            }
        };
        let upload_id = match response.upload_id {
            None => anyhow::bail!("Could not get upload_id"),
            Some(x) => x,
        };
        if verbose > 1 {
            eprintln!("Starting upload, upload_id = {upload_id}")
        }
        return Ok((config, upload_id));
    }
    anyhow::bail!("Stopped following redirects after 3 hops")
}

async fn upload(
    args: &Args,
    bucket: String,
    key: String,
    mut input: Box<dyn std::io::Read + Send + Sync>,
    num_tokens: usize,
    config: &aws_config::SdkConfig,
    upload_id: String,
) -> anyhow::Result<()> {
    // add initial tokens
    let (token_sender, token_receiver) = channel::bounded(num_tokens);
    for _ in 0..num_tokens {
        if token_sender.send(Ok(None)).is_err() {
            anyhow::bail!("Failed to initialize threads");
        }
    }

    let mut part_results = Vec::new();
    let mut wait_for_part = || {
        match token_receiver.recv() {
            Err(e) => anyhow::bail!("Failed communicate with threads: {e}"),
            Ok(Err(e)) => anyhow::bail!("Failed to upload part: {e}"),
            Ok(Ok(Some(x))) => part_results.push(x),
            Ok(Ok(None)) => (),
        }
        Ok(())
    };
    for part_number in 1.. {
        let mut buffer = vec![0_u8; args.block_size];
        let mut pos = 0;
        let mut end_of_file = false;
        while pos < buffer.len() && !end_of_file {
            let num_read = input.read(&mut buffer[pos..])?;
            end_of_file = num_read == 0;
            pos += num_read;
        }
        buffer.resize(pos, 0_u8);

        if buffer.is_empty() {
            break;
        }

        wait_for_part()?;

        let config = config.clone();
        let max_retries = args.max_retries;
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let token_sender = token_sender.clone();
        tokio::spawn(async move {
            let client = s3::Client::new(&config);
            let mut retry_count = 0;
            let result = loop {
                match client
                    .upload_part()
                    .body(buffer.clone().into())
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .send()
                    .await
                {
                    Ok(response) => {
                        break Ok(Some(
                            CompletedPart::builder()
                                .e_tag(response.e_tag.unwrap_or("".to_string()))
                                .part_number(part_number)
                                .build(),
                        ))
                    }
                    Err(e) => {
                        retry_count += 1;
                        if retry_count > max_retries {
                            break Err(e);
                        }
                        eprintln!("Failed to upload chunk: {}, retrying", e);
                        tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count))).await;
                    }
                }
            };
            let _ = token_sender.send(result);
        });

        if end_of_file {
            break;
        }
    }

    // drain remaining results
    for _ in 0..num_tokens {
        wait_for_part()?;
    }

    // finalize upload

    part_results.sort_by_key(|x| x.part_number);
    let client = s3::Client::new(config);
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(part_results))
                .build(),
        )
        .send()
        .await?;

    Ok(())
}

async fn run(args: &Args) -> anyhow::Result<()> {
    if args.block_size < 3 * 1024 * 1024 {
        anyhow::bail!("Part size too small, 5MB is the minimum");
    }

    let (bucket, key) = match args.s3_path.strip_prefix("s3://") {
        None => anyhow::bail!("S3 path has to start with 's3://'"),
        Some(x) => match x.split_once('/') {
            None => anyhow::bail!("S3 path should be 's3://bucket/key'"),
            Some((bucket, key)) => (bucket.to_string(), key.to_string()),
        },
    };

    let input: Box<dyn std::io::Read + Send + Sync> = if let Some(file) = &args.input {
        Box::new(match std::fs::File::open(file) {
            Err(e) => {
                eprintln!("Failed to open input file: {}", e);
                std::process::exit(1);
            }
            Ok(x) => x,
        })
    } else {
        Box::new(std::io::stdin())
    };

    let num_tokens = 2 * args.threads;

    // start multi-part upload
    let (config, upload_id) = start_upload(&bucket, &key, args.verbose).await?;

    if let Err(e) = upload(
        args,
        bucket.clone(),
        key.clone(),
        input,
        num_tokens,
        &config,
        upload_id.clone(),
    )
    .await
    {
        eprintln!("Aborting upload: {e}");
        let client = s3::Client::new(&config);
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
        anyhow::bail!("Failed upload");
    }

    Ok(())
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads + 2) // we need 2 extra threads for blocking I/O
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    if let Err(e) = rt.block_on(async move { run(&args).await }) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

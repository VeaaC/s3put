use crossbeam::channel;
use http::StatusCode;
use rusoto_core::RusotoError;
use rusoto_s3::AbortMultipartUploadRequest;
use rusoto_s3::CompleteMultipartUploadRequest;
use rusoto_s3::CompletedMultipartUpload;
use rusoto_s3::CompletedPart;
use rusoto_s3::CreateMultipartUploadRequest;
use rusoto_s3::UploadPartRequest;
use rusoto_s3::{S3Client, S3};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;

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

#[derive(StructOpt, Debug)]
#[structopt(name = "s3put")]
struct Args {
    /// S3 path to upload to
    s3_path: String,

    /// input file name
    #[structopt(long, short = "i")]
    input: Option<PathBuf>,

    /// block size used for data uploads
    #[structopt(long, default_value = "32MB", parse(try_from_str=parse_size))]
    block_size: usize,

    /// number of threads to use, defaults to number of logical cores
    #[structopt(long, short = "t", default_value = "6")]
    threads: usize,

    /// Print verbose information, statistics, etc
    #[structopt(long, short = "v")]
    verbose: bool,

    /// Determines how often each chunk should be retried before giving up
    #[structopt(long, default_value = "4")]
    max_retries: u32,
}

async fn start_upload(
    bucket: &str,
    key: &str,
    verbose: bool,
) -> anyhow::Result<(rusoto_core::Region, String)> {
    let mut region: rusoto_core::Region = Default::default();
    for _ in 0..3 {
        let client = S3Client::new(region.clone());
        let response = match client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(x) => x,
            Err(e) => {
                if let RusotoError::Unknown(response) = &e {
                    if response.status == StatusCode::MOVED_PERMANENTLY {
                        if let Some(x) = response.headers.get("x-amz-bucket-region") {
                            region = x.parse()?;
                            if verbose {
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
        return Ok((region, upload_id));
    }
    anyhow::bail!("Stopped following redirects after 3 hops")
}

async fn upload(
    args: &Args,
    bucket: String,
    key: String,
    mut input: Box<dyn std::io::Read + Send + Sync>,
    num_tokens: usize,
    region: rusoto_core::Region,
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

        let region = region.clone();
        let max_retries = args.max_retries;
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let token_sender = token_sender.clone();
        tokio::spawn(async move {
            let client = S3Client::new(region);
            let mut retry_count = 0;
            let result = loop {
                match client
                    .upload_part(UploadPartRequest {
                        body: Some(buffer.clone().into()),
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.to_string(),
                        part_number,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(response) => {
                        break Ok(Some(CompletedPart {
                            e_tag: response.e_tag,
                            part_number: Some(part_number),
                        }))
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
    let client = S3Client::new(region);
    client
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(part_results),
            }),
            ..Default::default()
        })
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
    let (region, upload_id) = start_upload(&bucket, &key, args.verbose).await?;

    if let Err(e) = upload(
        args,
        bucket.clone(),
        key.clone(),
        input,
        num_tokens,
        region.clone(),
        upload_id.clone(),
    )
    .await
    {
        eprintln!("Aborting upload: {e}");
        let client = S3Client::new(region);
        client
            .abort_multipart_upload(AbortMultipartUploadRequest {
                bucket,
                key,
                upload_id,
                ..Default::default()
            })
            .await?;
        anyhow::bail!("Failed upload");
    }

    Ok(())
}

fn main() {
    let args = Args::from_args();

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

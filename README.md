# S3Get

[<img alt="build" src="https://img.shields.io/github/workflow/status/VeaaC/s3put/Shuffly%20CI/main?style=for-the-badge">](https://github.com/Veaac/s3put/actions?query=branch%3Amain)

Upload a single file to S3 using parallel uploads.

## Usage Examples

Upload a compressed archive and pack it on the fly

```sh
tar -cf - my_data | pzstd -d | s3put s3://my-bucket/my-key.tar.zstd
```

## Installation

The CLI app can be installed with [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```sh
cargo install s3put
```

## Why S3Get?

Because neither s5cmd, s3cmd, nor aws-cli can offer fast parallel uploads while piping from stdin
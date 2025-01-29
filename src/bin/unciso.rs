use maybe_async::maybe_async;
use std::io::Write;

#[cfg_attr(not(feature = "sync"), tokio::main)]
#[maybe_async]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = std::path::PathBuf::from(&args[1]);
    let output = file.with_extension("iso");

    let file_ext = file
        .extension()
        .and_then(|e| e.to_str())
        .expect("Input file should have extension");
    let file_base = file.with_extension("");
    let split = file_base.extension().is_some_and(|e| e == "1");

    let input: Box<dyn ciso::read::Read<ReadError = std::io::Error>> = if split {
        let mut files = Vec::new();
        for i in 1.. {
            let part = file_base.with_extension(format!("{}.{}", i, file_ext));
            if !part.exists() {
                break;
            }

            let part = std::fs::File::open(part).unwrap();
            files.push(part);
        }

        if files.is_empty() {
            panic!("File does not exist");
        }

        Box::from(ciso::split::SplitFileReader::new(files).await.unwrap())
    } else {
        Box::from(std::fs::File::open(file.clone()).unwrap())
    };
    let mut reader = ciso::read::CSOReader::new(input).await.unwrap();

    let mut output = std::fs::File::create(output).unwrap();

    let mut buf = vec![0; 2048].into_boxed_slice();
    let mut bytes_read = 0;

    while bytes_read < reader.file_size() {
        reader.read_offset(bytes_read, &mut buf).await.unwrap();
        bytes_read += buf.len() as u64;

        output.write_all(&buf).unwrap();
    }
}

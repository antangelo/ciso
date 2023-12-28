use maybe_async::maybe_async;

struct SplitStdFs;

type BufFile = std::io::BufWriter<std::fs::File>;

#[maybe_async]
impl ciso::split::SplitFilesystem<std::io::Error, BufFile> for SplitStdFs {
    async fn create_file(&mut self, name: &std::ffi::OsStr) -> Result<BufFile, std::io::Error> {
        let file = std::fs::File::create(name)?;
        let bf: BufFile = std::io::BufWriter::new(file);
        Ok(bf)
    }

    async fn close(&mut self, _: BufFile) {}
}

#[cfg_attr(not(feature = "sync"), tokio::main)]
#[maybe_async]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = std::path::PathBuf::from(&args[1]);
    let output = file.with_extension("cso");

    if file == output {
        panic!("Input and output cannot be the same!");
    }

    let mut input = std::fs::File::open(file.clone()).unwrap();
    let mut output = ciso::split::SplitOutput::new(SplitStdFs, file);

    ciso::write::write_ciso_image(&mut input, &mut output, |_| {})
        .await
        .unwrap();
}

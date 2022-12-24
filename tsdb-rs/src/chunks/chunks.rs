use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};

fn sequence_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
    if !dir.as_ref().is_dir() {
        return Err(anyhow!("{:?} is not directory", dir.as_ref()));
    }

    let mut ret = vec![];
    for entry in fs::read_dir(dir).map_err(|e| anyhow!(e))? {
        if let Ok(ent) = entry {
            if let Some(Ok(_)) = ent.file_name().to_str().map(|f| f.parse::<u64>()) {
                ret.push(ent.path())
            }
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger::Env;

    fn init() {
        let env = Env::default().default_filter_or("debug");
        let _ = env_logger::Builder::from_env(env).is_test(true).try_init();
    }

    #[test]
    fn test_sequence_files() {
        init();

        let path = Path::new("tests/index_format_v1/chunks");
        assert_eq!(
            vec![PathBuf::from("tests/index_format_v1/chunks/000001")],
            sequence_files(&path).unwrap()
        );
    }
}

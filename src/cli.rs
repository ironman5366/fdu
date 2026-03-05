use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "fdu", about = "Fast disk usage analyzer", version)]
pub struct Cli {
    /// Directory to scan (default: current directory)
    #[arg(default_value = ".")]
    pub path: PathBuf,

    /// Disable cache (force fresh scan, don't save results)
    #[arg(long = "no-cache")]
    pub no_cache: bool,

    /// Export results as JSON to the given file
    #[arg(long = "export", value_name = "FILE")]
    pub export: Option<PathBuf>,

    /// Stay on the same filesystem (do not cross mount points)
    #[arg(short = 'x', long = "one-file-system")]
    pub same_filesystem: bool,

    /// Number of threads for parallel stat calls (default: 128)
    #[arg(short = 't', long = "threads", value_name = "N")]
    pub threads: Option<usize>,
}

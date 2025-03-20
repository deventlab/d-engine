use clap::{command, Parser};


#[derive(Parser)]
#[command(name = "benchmark")]
#[command(version = "0.0.1")]
#[command(about = "Benchmark util for d-engine", long_about = None)]
pub struct Args {
    #[arg(long, default_value_t=1)]
    pub conns: usize,

    #[arg(long, default_value_t=1)]
    pub clients: usize,

    #[arg(short, long)]
    pub endpoints: Vec<String>,

    #[arg(long, default_value_t=8)]
    pub key_size: usize,

    #[arg(long, default_value_t=256)]
    pub value_size: usize,

    #[arg(long, default_value_t=1000)]
    pub total: usize,

    #[arg(long, default_value_t=false)]
    pub batch: bool,
}


impl Default for Args {
    fn default() -> Self {
        Self {
            endpoints: vec![
                "http://127.0.0.1:9083".to_string(),
                "http://127.0.0.1:9082".to_string(),
                "http://127.0.0.1:9081".to_string(),
            ],
            conns: 1,
            clients: 1,
            key_size: 1,
            value_size: 1,
            total: 1,
            batch: false,
        }
    }
}
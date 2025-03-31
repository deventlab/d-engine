mod append_entries;
mod cluster_start_stop;
mod commons;

static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
    env_logger::init();
});

pub fn enable_logger() {
    *LOGGER_INIT;
    println!("setup logger for unit test.");
}

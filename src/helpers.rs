use tracing::level_filters::LevelFilter;

/// Helper function to setup a tracing fmt subscriber with the given level filer.
pub fn setup_fmt_subscriber(level: LevelFilter) {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

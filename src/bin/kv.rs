use std::{env, io};

use kv::Pageserver;

fn main() -> io::Result<()> {
    let mut params: Vec<String> = env::args().collect();
    params = params[1..].to_vec();

    let mut pserver = Pageserver::new();
    pserver.load_waldump(params)?;
    pserver.spill_layers();

    Ok(())
}
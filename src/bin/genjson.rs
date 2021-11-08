use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use std::{env, fs, io};
use std::collections::{BTreeMap, HashSet};
use serde::Serialize;

use kv::ChunkBounds;


#[derive(Serialize)]
struct ChunkHistory {
    chunk_info: ChunkBounds,
    layers: Vec<Layer>,
}

#[derive(Serialize)]
struct Layer {
    prev_lsn: usize,
    lsn: usize,
    updated_pages: u32,
    prev_delta_layer_size: u32,
}

fn get_delta_info(path: PathBuf) -> io::Result<(u32,u32)> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut uniq_pages = HashSet::new();

    let size = reader
        .lines()
        .map(|line| {
            let line = line.unwrap();
            let parts = line.split(" ").collect::<Vec<_>>();
            let pageno = parts.first().unwrap().parse::<u32>().unwrap();
            let sz = parts.last().unwrap().parse::<u32>().unwrap();
            (pageno, sz)
        })
        .map(|(pageno, sz)| {
            uniq_pages.insert(pageno); // consume pageno
            sz
        })
        .sum();

    Ok((uniq_pages.len() as u32, size))
}

fn main() -> io::Result<()>{
    let dir = env::args().nth(1).unwrap();

    let mut layer_map: BTreeMap<ChunkBounds, Vec<Layer>> = BTreeMap::new();

    let entries = fs::read_dir(dir)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    for path_buf in entries {
        let fname = path_buf.file_name().unwrap().to_str().unwrap();

        // go over delta layers only, e.g. 277-0-1280-9408660832:10974583264.delta
        if !fname.contains(":") {
            continue;
        }

        let fname = path_buf.file_name().unwrap().to_str().unwrap();
        let parts = fname.split("-").collect::<Vec<_>>();
        println!("{:?}", parts);

        let entry = layer_map
            .entry(ChunkBounds{
                relnum: parts[0].parse().unwrap(),
                start: parts[1].parse().unwrap(),
                end: parts[2].parse().unwrap(),
            })
            .or_insert(Vec::new());

            let lsns = parts[3]
                .trim_end_matches(".delta")
                .split(":")
                .map(|s| s.parse::<usize>().unwrap())
                .collect::<Vec<_>>();

            assert!(lsns.len() == 2);

            let (updated_pages, prev_delta_layer_size) = get_delta_info(path_buf).unwrap();

            let layer = Layer {
                prev_lsn: lsns[0],
                lsn: lsns[1],
                updated_pages,
                prev_delta_layer_size,
            };

            entry.push(layer);

    }

    for (_,  layers) in &mut layer_map {
        layers.sort_by_key(|layer| layer.lsn);
    }

    let result = layer_map
        .into_iter()
        .map(|(chunk_info, layers)| {
            ChunkHistory{
                chunk_info,
                layers,
            }
        })
        .collect::<Vec<_>>();

    let serialized = serde_json::to_string(&result).unwrap();
    fs::write("chunks.json", serialized).unwrap();

    println!("Done!");
    Ok(())
}

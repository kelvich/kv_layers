use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, prelude::*, BufReader};
use std::process::{Command, Stdio};
use serde::Serialize;

#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
struct Key {
    relnum: u32,
    blkno: u32,
    lsn: usize
}

pub struct Pageserver {
    store: BTreeMap<Key,String>,
    rel_remap: HashMap<String, u32>,
    current_lsn: usize,

    chunks: BTreeMap<ChunkBounds, ChunkInfo>,
}

// 1280 pages per chunk. That corresponds to a 10MB full layer.
const CHUNK_SIZE: usize = 1280;

// Limit max size of delta layer to 10MB too. So each new full layer would be written
// each time the delta layer is full.
const DELTA_SIZE: usize = 10 * 1024 * 1024;

#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize)]
pub struct ChunkBounds {
    pub relnum: u32,
    pub start: u32,
    pub end: u32
}

impl ChunkBounds {
    fn as_key_range(&self) -> std::ops::Range<Key> {
        let k1 = Key { relnum: self.relnum, blkno: self.start, lsn: 0 };
        let k2 = Key { relnum: self.relnum, blkno: self.end, lsn: 0 };
        k1..k2
    }
}

#[derive(Debug)]
struct ChunkInfo {
    latest_lsn: usize,
    s3_lsn: usize,
    wal_size_since_last_layer: usize,
    layer_lsn_marks: Vec<usize>,
}

struct WalDumpEntry {
    reltag_str: String,
    blkno: u32,
    lsn: usize,
    size: u32
}

type Lsn = usize;

impl Pageserver {
    pub fn new() -> Pageserver {
        Pageserver {
            store: BTreeMap::new(),
            rel_remap: HashMap::new(),
            current_lsn: 0,
            chunks: BTreeMap::new(),
        }
    }

    fn add_wal_record(&mut self, reltag_str: String, blkno: u32, lsn: usize, size: u32) {
        // remap rels to incremental numbers to save few bytes
        let nrels = self.rel_remap.len();
        let relnum = *self.rel_remap.entry(reltag_str).or_insert(nrels as u32 + 1);
    
        self.store.insert(Key {
            relnum,
            blkno,
            lsn: lsn,
        }, format!("{} {} {}", blkno, lsn_fmt(lsn), size));


        let bounds = self.page_to_bounds(relnum, blkno);
        let chunk = self.chunks.entry(bounds).or_insert(ChunkInfo {
            latest_lsn: lsn,
            s3_lsn: 0,
            wal_size_since_last_layer: 0,
            layer_lsn_marks: Vec::new(),
        });

        chunk.latest_lsn = lsn;
        chunk.wal_size_since_last_layer += size as usize;
        assert!(size > 0);

        if chunk.wal_size_since_last_layer > DELTA_SIZE {
            chunk.layer_lsn_marks.push(lsn);
            chunk.wal_size_since_last_layer = 0;
            
        }
    }

    pub fn load_waldump(&mut self, waldump_args: Vec<String>) -> io::Result<()> {
        let process = match Command::new("pg_waldump")
                                    .args(waldump_args)
                                    .stdin(Stdio::piped())
                                    .stdout(Stdio::piped())
                                    .spawn() {
            Err(why) => panic!("couldn't spawn pg_waldump: {}", why),
            Ok(process) => process,
        };

        let reader = BufReader::new(process.stdout.unwrap());
        let mut current_wal_segment = 1;
        for line in reader.lines() {
            let line = line?;
            let records = self.parse_waldump_line(&line);

            records.iter().for_each(|record| {
                self.add_wal_record(record.reltag_str.clone(), record.blkno, record.lsn, record.size);
                self.current_lsn = record.lsn;
            });

            if current_wal_segment != self.current_lsn / 0x1000000 {
                current_wal_segment = self.current_lsn / 0x1000000;
                self.dump_stats();
            }
        }

        self.dump_stats();
        self.dump_chunk_stats();
        Ok(())
    }

    fn page_to_bounds(&self, relnum: u32, pagetag: u32) -> ChunkBounds {
        let start = (pagetag / CHUNK_SIZE as u32) * CHUNK_SIZE as u32;
        let end = start + CHUNK_SIZE as u32;
        ChunkBounds {
            relnum,
            start,
            end
        }
    }

    fn parse_waldump_line(&self, line: &String) -> Vec<WalDumpEntry> {
        let mut entries = Vec::new();

        let parts: Vec<&str> = line.split(" ").filter(|&x| !x.is_empty()).collect();

        // get size
        let size_part = parts[5];
        let size = size_part.trim_end_matches(',').parse::<u32>().unwrap();

        // get lsn
        let lsn_part = parts[9];
        let lsn = lsn_from_str(&lsn_part.trim_end_matches(',')).unwrap();

        // get blkrefs
        let mut i = 1;
        while i < parts.len() {
            // "blkref #0: rel 1663/14021/16396 blk 1204"
            if parts[i] != "blkref" {
                i += 1;
                continue;
            } else {
                let mut reltag_str = parts[i+3].to_string();
                if parts[i+4] == "fork" {
                    reltag_str += ".";
                    reltag_str += parts[i+5];
                    i += 2;
                }

                let blkno = parts[i+5].trim_end_matches(',').parse::<u32>().unwrap();
                i += 6;

                // println!("{} {} {} = {}", reltag, blkno, lsn, size);
                entries.push(WalDumpEntry {
                    reltag_str,
                    blkno,
                    lsn,
                    size
                });
            }
        }

        entries
    }

    pub fn spill_layers(&mut self) {

        for (bounds, info) in self.chunks.iter_mut() {
            assert!(info.latest_lsn >= info.s3_lsn);
            if info.latest_lsn == info.s3_lsn {
                assert!(info.layer_lsn_marks.len() == 0);
                continue;
            }

            if !(info.layer_lsn_marks.len() > 0 && *info.layer_lsn_marks.last().unwrap() == info.latest_lsn) {
                info.layer_lsn_marks.push(info.latest_lsn);
            }

            println!("Spilling chunk {:?} -> {:?}", bounds, info);

            assert!(info.s3_lsn != info.layer_lsn_marks[0]);
            let mut marks = vec![info.s3_lsn];
            marks.append(&mut info.layer_lsn_marks.clone());
            assert!(marks.len() >= 2);

            let mut full_layers: HashMap<Lsn, String> = HashMap::new();
            let mut delta_layers: HashMap<(Lsn, Lsn), String> = HashMap::new();

            for i in 1..marks.len() {
                let (lsn1,lsn2) = (marks[i-1], marks[i]);
                delta_layers.insert((lsn1, lsn2),
                    format!("{}-{}-{}-{}:{}.delta", bounds.relnum, bounds.start, bounds.end, lsn1, lsn2));
                full_layers.insert(lsn2,
                    format!("{}-{}-{}-{}.layer", bounds.relnum, bounds.start, bounds.end, lsn2));
            }

            // open files
            let mut open_files = HashMap::new();
            for (_,v) in full_layers.iter() {
                open_files.insert(v.clone(), File::create(format!("layers/{}",v)).unwrap());
            }
            for (_,v) in delta_layers.iter() {
                open_files.insert(v.clone(), File::create(format!("layers/{}",v)).unwrap());
            }

            let mut iter = self.store.range(bounds.as_key_range()).peekable();
            // for (key, payload) in iter {
            while let Some((key, payload)) = iter.next() {
                // write delta layers
                let dest_delta_layer = delta_layers
                    .keys()
                    .filter(|(lsn1, lsn2)| *lsn1 <= key.lsn && key.lsn <= *lsn2)
                    .next()
                    .unwrap();

                let delta_layer_name = delta_layers.get(dest_delta_layer).unwrap();
                let data = payload.to_string() + "\n";

                let mut f = open_files.get(delta_layer_name).unwrap();
                f.write_all(data.as_bytes()).unwrap();

                // write full layers
                let last_version = iter.peek().is_none() || iter.peek().unwrap().0.blkno != key.blkno;
                let delta_to_next = if last_version {
                    (key.lsn, usize::MAX)
                } else {
                    (key.lsn, iter.peek().unwrap().0.lsn)
                };

                let dest_full_layers = full_layers
                    .keys()
                    .filter(|&lsn| delta_to_next.0 <= *lsn && *lsn < delta_to_next.1)
                    .collect::<Vec<_>>();

                for lsn in dest_full_layers {
                    let full_layer_name = full_layers.get(lsn).unwrap();
                    let data = payload.to_string() + "\n";

                    let mut f = open_files.get(full_layer_name).unwrap();
                    f.write_all(data.as_bytes()).unwrap();
                }
            }
        }

    }

    fn dump_stats(&self) {
        println!("n_keys = {}, current_lsn = {}, n_rels = {}", self.store.len(), lsn_fmt(self.current_lsn), self.rel_remap.len());
    }

    fn dump_chunk_stats(&self) {
        for (bounds, info) in self.chunks.iter() {
            println!("{:?} -> {:?}", bounds, info);
        }
    }
}

fn lsn_from_str(s: &str) -> io::Result<usize> {
    let mut splitter = s.split('/');
    if let (Some(left), Some(right), None) = (splitter.next(), splitter.next(), splitter.next())
    {
        let left_num = u32::from_str_radix(left, 16).map_err(|_|
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid lsn")
        )?;
        let right_num = u32::from_str_radix(right, 16).map_err(|_|
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid lsn")
        )?;
        Ok((left_num as usize) << 32 | right_num as usize)
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid lsn"))
    }
}

fn lsn_fmt(lsn: usize) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffffffff)
}

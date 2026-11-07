//! TIMING-level regression test for two dispatch defects that made every
//! packet-fed consumer, and every factor-1 node reading a few cells of a wide
//! predecessor, wait for the WHOLE frame (found on the QEC streaming-decode
//! benchmark, `bench/qec-bench/README.md` "Phase 5"):
//!
//! - **Fanout-bulk on `$network` edges**: a node whose single `$res`
//!   predecessor is the `$network` node with an equal factor (the ordinary
//!   1:1 "one instance per packet" shape) was marked fanout-bulk eligible
//!   (`runtime::init`, Upgrade 5). Fanout-bulk holds every ready instance until
//!   ALL `factor` instances have arrived, then dispatches them as
//!   `min(workers, factor)` chunks — so the consumer of packet 0 waited for the
//!   frame's LAST packet. Fixed by excluding `$network`-fed nodes.
//!
//! - **Factor-1 multi-cell reads counted the full predecessor**: a factor-1
//!   node reading cells `{0, 1}` of a factor-4 predecessor (`$res "0"` +
//!   `$barrier "1"` — a fusion-tree fuse over two leaves) got no index filter
//!   (`Predecessor::index_filter` required `range_len == succ_factor`), so its
//!   dependency count fell back to all 4 predecessor instances. Fixed by
//!   filtering a contiguous absolute index set for factor-1 successors.
//!
//! Shape (per frame): packets 0 and 1 are sent, then the sender sleeps
//! `GAP`, then packets 2 and 3. `fuse01` reads leaves {0,1}, `fuse23` leaves
//! {2,3}, `top` both fuses. With either defect present, leaf 0/1 or `fuse01`
//! cannot start until packet 2 has been sent, i.e. at least `GAP` late; the
//! test asserts they all started before packet 2's send instant (a `GAP` of
//! 300 ms makes this robust to scheduling jitter), and checks every value
//! (each instance read its own cell) so the narrower dependency count is
//! shown to be correct, not just early. Builds a real plugin + `main`, like
//! `network_edge_barrier_values.rs` (in-crate tests resolve plugin functions
//! to noop stubs and cannot observe values or timing).

use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::UdpSocket;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LEAVES_PER_FRAME: usize = 4;
const FRAMES: usize = 3;
const START_PORT: u16 = 19740;
const GAP: Duration = Duration::from_millis(300);

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_network_streaming_{tag}_{}_{}",
        std::process::id(),
        n
    ))
}

fn run(cmd: &mut Command, what: &str) {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {what}: {e}"));
    assert!(
        out.status.success(),
        "{what} failed (status {:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn spawn_and_wait_for_bind(
    main_bin: &PathBuf,
    graph_path: &PathBuf,
    dylib: &PathBuf,
    workers: usize,
    port: u16,
) -> Child {
    let mut child = Command::new(main_bin)
        .arg("--json")
        .arg(graph_path)
        .arg("--dylib")
        .arg(dylib)
        .arg("--workers")
        .arg(workers.to_string())
        .arg("--core-offset")
        .arg("1")
        .arg("--slots")
        .arg("2")
        .arg("--max-frames")
        .arg(FRAMES.to_string())
        .arg("--max-runtime")
        .arg("30")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn main (port {port}): {e}"));

    let (tx, rx) = std::sync::mpsc::channel::<String>();
    for stream_kind in ["stdout", "stderr"] {
        let tx = tx.clone();
        let reader: Box<dyn Read + Send> = if stream_kind == "stdout" {
            Box::new(child.stdout.take().unwrap())
        } else {
            Box::new(child.stderr.take().unwrap())
        };
        std::thread::spawn(move || {
            for line in BufReader::new(reader).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bound = false;
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(line) if line.contains("bound UDP sockets") => {
                bound = true;
                break;
            }
            Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    assert!(bound, "port {port}: never observed \"bound UDP sockets\"");
    // Keep draining so a chatty child never blocks on a full pipe.
    std::thread::spawn(move || while rx.recv().is_ok() {});
    child
}

/// Sends every frame as two halves `GAP` apart; returns, per frame, the
/// wall-clock ns just BEFORE packet 2 (the second half) was sent.
fn send_packets(port: u16) -> Vec<u128> {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind sender socket");
    let dest = format!("127.0.0.1:{port}");
    let send = |frame_id: usize, leaf_idx: usize| {
        let mut pkt = [0u8; 8];
        pkt[0..4].copy_from_slice(&(frame_id as u32).to_le_bytes());
        pkt[4..8].copy_from_slice(&(leaf_idx as u32).to_le_bytes());
        sock.send_to(&pkt, &dest).expect("send packet");
    };
    let mut second_half_ns = Vec::new();
    for frame_id in 0..FRAMES {
        send(frame_id, 0);
        send(frame_id, 1);
        std::thread::sleep(GAP);
        second_half_ns.push(now_ns());
        send(frame_id, 2);
        send(frame_id, 3);
        // Let the frame drain before the next one (isolated frames).
        std::thread::sleep(Duration::from_millis(50));
    }
    second_half_ns
}

#[test]
fn network_fed_and_factor1_partial_reads_dispatch_without_waiting_for_whole_frame() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();
    let log = plugin_dir.join("events.log");

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "netstreaming"
version = "0.1.0"
edition = "2021"

[lib]
name = "netstreaming"
crate-type = ["dylib", "rlib"]

[dependencies]
tomii-types = {{ path = {tomii_types:?} }}
tomii-macro = {{ path = {tomii_macro:?} }}
"#
        ),
    )
    .unwrap();

    fs::write(
        plugin_dir.join("src/lib.rs"),
        r#"
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

static LOCK: Mutex<()> = Mutex::new(());

fn log(path: &str, line: String) {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let _g = LOCK.lock().unwrap();
    let mut f = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(f, "{line} {t}").unwrap();
}

pub struct Pkt {
    pub frame_id: usize,
    pub leaf_idx: usize,
}

#[no_mangle]
pub fn extract_pkt(bytes_cm: &CmTypes) -> CmTypes {
    if let CmTypes::Bytes(arc) = bytes_cm {
        let b = arc.as_slice();
        CmTypes::from_any(Pkt {
            frame_id: u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize,
            leaf_idx: u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize,
        })
    } else {
        panic!("extract_pkt: expected Bytes, got {:?}", bytes_cm)
    }
}

#[no_mangle]
pub fn pkt_frame_id(p: &CmTypes) -> usize {
    p.with_any(|p: &Pkt| p.frame_id).expect("pkt_frame_id: wrong type")
}

#[no_mangle]
pub fn pkt_leaf_idx(p: &CmTypes) -> usize {
    p.with_any(|p: &Pkt| p.leaf_idx).expect("pkt_leaf_idx: wrong type")
}

/// Value = frame * 100 + leaf index, so every consumer can check it read
/// exactly the cell it declared.
#[tomii_export]
pub fn leaf_fn(out_path: String, own_index: usize, packet: &Pkt) -> usize {
    log(&out_path, format!("L {} {} {}", packet.frame_id, own_index, packet.leaf_idx));
    packet.frame_id * 100 + packet.leaf_idx
}

/// Reads its left leaf's value via `$res`; the right leaf is a `$barrier`.
#[tomii_export]
pub fn fuse_fn(out_path: String, unit: usize, left: usize) -> usize {
    log(&out_path, format!("F {} {} {}", left / 100, unit, left % 100));
    left
}

#[tomii_export]
pub fn top_fn(out_path: String, a: usize, b: usize) -> usize {
    log(&out_path, format!("T {} {} {}", a / 100, a % 100, b % 100));
    a
}
"#,
    )
    .unwrap();

    let plugin_target = plugin_dir.join("target");
    let func_path = plugin_dir.join("src/lib.rs");
    run(
        Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--manifest-path")
            .arg(plugin_dir.join("Cargo.toml"))
            .env("FUNC_PATH", &func_path)
            .env("CARGO_TARGET_DIR", &plugin_target),
        "cargo build (plugin)",
    );
    let dylib = plugin_target.join("release/libnetstreaming.so");
    assert!(dylib.exists(), "expected plugin dylib at {dylib:?}");

    let main_target = unique_dir("main_target");
    run(
        Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("-p")
            .arg("tomii-core")
            .arg("--bin")
            .arg("main")
            .env("FUNC_PATH", &func_path)
            .env("CARGO_TARGET_DIR", &main_target)
            .current_dir(&manifest_dir),
        "cargo build (main)",
    );
    let main_bin = main_target.join("release/main");
    assert!(main_bin.exists(), "expected runtime binary at {main_bin:?}");

    let workers = 4;
    let port = START_PORT;
    let log_s = log.to_str().unwrap();
    let fuse = |name: &str, unit: usize, l: usize, r: usize| {
        format!(
            r#"{{ "name": "{name}", "factor": 1, "function": "fuse_fn",
                 "args": [
                     {{ "type": "String", "value": {log_s:?} }},
                     {{ "type": "usize", "value": "{unit}" }},
                     {{ "type": "$res", "predecessor": {{ "name": "leaf", "indexes": "{l}" }} }},
                     {{ "type": "$barrier", "predecessor": {{ "name": "leaf", "indexes": "{r}" }} }}
                 ] }}"#
        )
    };
    let graph_json = format!(
        r#"
        {{
            "nodes": [
                {{ "name": "leaf", "factor": {p}, "function": "leaf_fn",
                   "args": [
                       {{ "type": "String", "value": {log_s:?} }},
                       {{ "type": "$ref", "value": "$index" }},
                       {{ "type": "$res", "predecessor": {{ "name": "$network", "indexes": "0-{p_minus_1}" }} }}
                   ] }},
                {fuse01},
                {fuse23},
                {{ "name": "top", "factor": 1, "function": "top_fn",
                   "args": [
                       {{ "type": "String", "value": {log_s:?} }},
                       {{ "type": "$res", "predecessor": {{ "name": "fuse01", "indexes": "0" }} }},
                       {{ "type": "$res", "predecessor": {{ "name": "fuse23", "indexes": "0" }} }}
                   ] }}
            ],
            "network_config": {{
                "socket_type": "udp",
                "num_sockets": 1,
                "packet_length": 8,
                "frame_packets": {p},
                "buffer_depth": 128,
                "address": "127.0.0.1",
                "start_port": {port},
                "extract_packet_func": "extract_pkt",
                "id_function": "pkt_frame_id",
                "index_function": {{ "function": "pkt_leaf_idx", "args": [] }}
            }}
        }}
        "#,
        p = LEAVES_PER_FRAME,
        p_minus_1 = LEAVES_PER_FRAME - 1,
        fuse01 = fuse("fuse01", 4, 0, 1),
        fuse23 = fuse("fuse23", 5, 2, 3),
    );
    let graph_path = plugin_dir.join("graph.json");
    fs::write(&graph_path, graph_json).unwrap();

    let mut child = spawn_and_wait_for_bind(&main_bin, &graph_path, &dylib, workers, port);
    let second_half_ns = send_packets(port);
    let status = child.wait().expect("wait on main");
    assert!(status.success(), "main exited with {status:?}");

    let contents =
        fs::read_to_string(&log).unwrap_or_else(|e| panic!("missing event log {log:?}: {e}"));
    // (kind, frame, a, b) -> wall-clock ns
    let mut leaf_t = vec![[0u128; LEAVES_PER_FRAME]; FRAMES];
    let mut fuse01_t = vec![0u128; FRAMES];
    let mut tops = 0usize;
    for line in contents.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        let frame: usize = f[1].parse().unwrap();
        let a: usize = f[2].parse().unwrap();
        let b: usize = f[3].parse().unwrap();
        let t: u128 = f[4].parse().unwrap();
        assert!(frame < FRAMES, "unexpected frame {frame}: {line}");
        match f[0] {
            "L" => {
                assert_eq!(a, b, "leaf instance {a} read packet {b} (frame {frame})");
                leaf_t[frame][a] = t;
            }
            "F" => {
                // unit 4 = fuse01 must read leaf 0's cell, unit 5 = fuse23 leaf 2's.
                let want = if a == 4 { 0 } else { 2 };
                assert_eq!(b, want, "fuse unit {a} (frame {frame}) read leaf {b}");
                if a == 4 {
                    fuse01_t[frame] = t;
                }
            }
            "T" => {
                assert_eq!((a, b), (0, 2), "top (frame {frame}) read leaves {a},{b}");
                tops += 1;
            }
            other => panic!("unexpected log line kind {other}: {line}"),
        }
    }
    assert_eq!(tops, FRAMES, "every frame must complete:\n{contents}");

    for frame in 0..FRAMES {
        let cut = second_half_ns[frame];
        for leaf in 0..2 {
            assert!(
                leaf_t[frame][leaf] != 0 && leaf_t[frame][leaf] < cut,
                "frame {frame}: leaf {leaf} ran {} ms after packet 2 was sent — a \
                 `$network`-fed instance must dispatch on its own packet, not wait for \
                 the whole frame (fanout-bulk on a network edge)",
                (leaf_t[frame][leaf] as i128 - cut as i128) / 1_000_000
            );
        }
        assert!(
            fuse01_t[frame] != 0 && fuse01_t[frame] < cut,
            "frame {frame}: fuse01 (reads leaves 0 and 1 only) ran {} ms after packet 2 \
             was sent — a factor-1 node must wait only for the cells it declares, not \
             for every instance of its predecessor",
            (fuse01_t[frame] as i128 - cut as i128) / 1_000_000
        );
    }

    let _ = fs::remove_dir_all(&plugin_dir);
    let _ = fs::remove_dir_all(&main_target);
}

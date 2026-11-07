//! VALUE/ORDERING-level regression test for two `$network`-edge correctness
//! bugs found while investigating the QEC benchmark's "clear_frame barrier"
//! finding (see `bench/qec-bench/README.md`'s "Known caveats"):
//!
//! - **Bug A** (suspected): a node with both a 1:1 `$res` edge from the
//!   virtual `$network` node AND a `$barrier` edge from a DIFFERENT,
//!   no-predecessor producer ("clear") dispatches without ever waiting for
//!   the barrier. Root-caused as **not a real correctness defect**: the
//!   dependency count (`Graph::dependency_count_vec`) and the shared
//!   `NodeDependencyEntry::has_barrier` release-together counter both
//!   correctly require `clear`'s completion before releasing ANY instance of
//!   the `$network`-fed node — `clear`'s decrement just lands via
//!   `task_execution::worker_resolve_successors` (a plain compute root/
//!   initial node dispatched to a worker thread), which previously had no
//!   "DECREMENT" trace line, unlike `batch_resolution::process_batch_inner`
//!   (used for network-packet-triggered completions on the resolution
//!   thread) — creating a false impression that the barrier was never
//!   counted. This test asserts the REAL invariant directly: `clear` must
//!   have already run for a given frame before any `leaf` instance of that
//!   frame observes its packet.
//!
//! - **Bug B** (confirmed, real, FIXED): `arg_resolution.rs`'s
//!   `per_instance_dep_idx` fast path assumes that when `pred_factor ==
//!   node_factor` for a per-instance `$res` list, the triggering `pred_index`
//!   already identifies the correct predecessor cell for the firing
//!   successor instance (true for a genuine 1:1 trigger-paired dispatch).
//!   That assumption breaks when the SAME node also has an unrelated
//!   `$barrier` from a different predecessor forcing `has_barrier` — every
//!   instance in the batch is released together carrying the SAME triggering
//!   `pred_index` (whichever predecessor completion zeroed the shared
//!   counter), so every consumer instance read the SAME predecessor cell
//!   instead of its own declared one. Fixed by also gating on
//!   `node_has_barrier` (a new `ResPredCache` field mirroring
//!   `buffers::node_dep::NodeDepMap::new`'s own `has_barrier`) and falling
//!   back to the declared per-instance index whenever it is set — safe
//!   unconditionally in that case, since the shared counter cannot reach
//!   zero until every declared predecessor cell has already completed.
//!
//! This test builds a small, real plugin (network callbacks + two trivial
//! kernels) and the `main` runtime binary (mirrors
//! `same_pred_multi_arg_values.rs`'s approach: `tomii-core`'s own `cargo
//! test` build resolves every plugin function to a `noop` stub, so no
//! in-crate test can observe real per-instance values), sends real UDP
//! packets at a tiny embedded sender, and asserts on the logged, ACTUAL
//! per-instance values and ordering — exactly the kind of regression a
//! routing/dependency-count change can get wrong that a noop-kernel test
//! cannot catch.

use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::net::UdpSocket;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Packets per frame == `leaf`'s factor, deliberately small (4) to match the
/// original minimal repro's shape most likely to expose Bug B.
const LEAVES_PER_FRAME: usize = 4;
const FRAMES: usize = 16;
const START_PORT: u16 = 19700;

fn unique_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tomii_network_edge_barrier_{tag}_{}_{}",
        std::process::id(),
        n
    ))
}

fn run(cmd: &mut Command, what: &str) -> String {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {what}: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "{what} failed (status {:?}):\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status
    );
    stdout + &stderr
}

/// Spawn `main`, wait (polling its combined stdout for the receiver's "bound
/// UDP sockets" line) until the socket is actually open, then return the
/// child plus its combined-output reader thread's join handle.
fn spawn_and_wait_for_bind(
    main_bin: &PathBuf,
    graph_path: &PathBuf,
    dylib: &PathBuf,
    workers: usize,
    port: u16,
) -> (Child, std::sync::mpsc::Receiver<String>) {
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
            let buf = BufReader::new(reader);
            for line in buf.lines() {
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
            Ok(line) => {
                if line.contains("bound UDP sockets") {
                    bound = true;
                    break;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    assert!(
        bound,
        "port {port}: never observed \"bound UDP sockets\" from main within 10s"
    );
    (child, rx)
}

fn send_packets(port: u16) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind sender socket");
    let dest = format!("127.0.0.1:{port}");
    for frame_id in 0..FRAMES {
        for leaf_idx in 0..LEAVES_PER_FRAME {
            let mut pkt = [0u8; 8];
            pkt[0..4].copy_from_slice(&(frame_id as u32).to_le_bytes());
            pkt[4..8].copy_from_slice(&(leaf_idx as u32).to_le_bytes());
            sock.send_to(&pkt, &dest).expect("send packet");
        }
        // Small gap: not a timing-sensitive test (the bug reproduces
        // regardless of gap size), just avoids bursting the receiver.
        std::thread::sleep(Duration::from_micros(500));
    }
}

#[test]
fn network_res_edge_plus_unrelated_barrier_dispatches_correctly() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let tomii_types = PathBuf::from(&manifest_dir).join("../tomii-types");
    let tomii_macro = PathBuf::from(&manifest_dir).join("../tomii-macro");

    let plugin_dir = unique_dir("plugin");
    fs::create_dir_all(plugin_dir.join("src")).unwrap();
    let leaf_log = plugin_dir.join("leaf.log");

    fs::write(
        plugin_dir.join("Cargo.toml"),
        format!(
            r#"
[package]
name = "netedgebarrier"
version = "0.1.0"
edition = "2021"

[lib]
name = "netedgebarrier"
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
#![allow(clippy::too_many_arguments)]
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

static LOCK: Mutex<()> = Mutex::new(());
// Mirrors the real QEC `clear_frame` design (see bench/qec-bench/README.md,
// "Frame id derivation"): a root (no-predecessor) node derives the frame id
// itself from a global counter, since Tomii dispatches every root instance
// for frame F together before frame F+1's.
static FRAME_COUNTER: AtomicUsize = AtomicUsize::new(0);
// Set of frame ids `clear_fn` has already run for — read by `leaf_fn` to
// assert the barrier actually gated it (Bug A's claim, checked directly
// rather than inferred from counter arithmetic).
static CLEARED: Mutex<Vec<usize>> = Mutex::new(Vec::new());

fn append_line(path: &str, line: &str) {
    let _g = LOCK.lock().unwrap();
    let mut f = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(f, "{line}").unwrap();
}

pub struct Pkt {
    pub frame_id: usize,
    pub leaf_idx: usize,
}

impl Pkt {
    fn from_bytes(b: &[u8]) -> Self {
        let frame_id = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
        let leaf_idx = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
        Self { frame_id, leaf_idx }
    }
}

#[no_mangle]
pub fn extract_pkt(bytes_cm: &CmTypes) -> CmTypes {
    if let CmTypes::Bytes(arc) = bytes_cm {
        CmTypes::from_any(Pkt::from_bytes(arc.as_slice()))
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

/// Root node (factor=1, no predecessors): derives its own frame id from a
/// global counter and records that this frame has been cleared.
#[tomii_export]
pub fn clear_fn() -> usize {
    let frame = FRAME_COUNTER.fetch_add(1, Ordering::SeqCst);
    CLEARED.lock().unwrap().push(frame);
    frame
}

/// `own_index` (via `$ref: $index`) is this dispatched instance's OWN node
/// index — independent of anything read via `$res`. `packet` is read via the
/// `$network` `$res` edge. For a correctly-dispatched instance these must be
/// EQUAL (the sender sent packet `leaf_idx=k` expecting instance `k` to both
/// be dispatched by it AND read exactly its own decoded value back via
/// `$res` — Bug B made every instance read whichever packet's completion
/// happened to zero the barrier-forced shared counter instead).
#[tomii_export]
pub fn leaf_fn(out_path: String, own_index: usize, packet: &Pkt) -> i64 {
    let cleared = CLEARED.lock().unwrap().contains(&packet.frame_id);
    append_line(
        &out_path,
        &format!("{} {} {} {}", packet.frame_id, own_index, packet.leaf_idx, cleared as u8),
    );
    packet.leaf_idx as i64
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
    let dylib = plugin_target.join("release/libnetedgebarrier.so");
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

    for (run_idx, workers) in [2usize, 4].into_iter().enumerate() {
        let _ = fs::remove_file(&leaf_log);
        let port = START_PORT + run_idx as u16;

        let graph_json = format!(
            r#"
            {{
                "nodes": [
                    {{ "name": "clear", "factor": 1, "function": "clear_fn", "args": [] }},
                    {{ "name": "leaf", "factor": {p}, "function": "leaf_fn",
                       "args": [
                           {{ "type": "String", "value": {leaf_log:?} }},
                           {{ "type": "$ref", "value": "$index" }},
                           {{ "type": "$res", "predecessor": {{ "name": "$network", "indexes": "0-{p_minus_1}" }} }},
                           {{ "type": "$barrier", "predecessor": {{ "name": "clear", "indexes": "0-0" }} }}
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
            leaf_log = leaf_log.to_str().unwrap(),
            port = port,
        );
        let graph_path = plugin_dir.join(format!("graph_{run_idx}.json"));
        fs::write(&graph_path, graph_json).unwrap();

        let (mut child, _rx) =
            spawn_and_wait_for_bind(&main_bin, &graph_path, &dylib, workers, port);
        send_packets(port);

        let status = child
            .wait()
            .unwrap_or_else(|e| panic!("workers={workers}: failed to wait on main: {e}"));
        assert!(
            status.success(),
            "workers={workers}: main exited with {status:?}"
        );

        let contents = fs::read_to_string(&leaf_log)
            .unwrap_or_else(|e| panic!("workers={workers}: missing {leaf_log:?}: {e}"));
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            FRAMES * LEAVES_PER_FRAME,
            "workers={workers}: expected {} leaf_fn invocations, got {}:\n{contents}",
            FRAMES * LEAVES_PER_FRAME,
            lines.len()
        );

        let mut seen_by_frame: Vec<Vec<usize>> = vec![Vec::new(); FRAMES];
        for line in &lines {
            let f: Vec<&str> = line.split_whitespace().collect();
            let frame_id: usize = f[0].parse().unwrap();
            let own_index: usize = f[1].parse().unwrap();
            let packet_leaf_idx: usize = f[2].parse().unwrap();
            let cleared: u8 = f[3].parse().unwrap();

            // Bug A check: `clear` must have already run for this frame by the
            // time `leaf` observes it, for EVERY instance of EVERY frame — the
            // $barrier edge from a different, no-predecessor producer must
            // actually gate dispatch.
            assert_eq!(
                cleared, 1,
                "workers={workers}: leaf(frame={frame_id}, own_index={own_index}) ran \
                 before `clear` had run for this frame — the $barrier from a \
                 different predecessor did not gate dispatch (Bug A)"
            );

            // Bug B check: this instance's `$res` read of the $network edge
            // must be ITS OWN packet, not whichever packet's completion
            // happened to trip the shared barrier-forced counter.
            assert_eq!(
                own_index, packet_leaf_idx,
                "workers={workers}: leaf instance {own_index} (frame {frame_id}) read \
                 packet leaf_idx={packet_leaf_idx} via $res — every instance must read \
                 its own declared predecessor cell, not whichever one tripped the \
                 barrier-forced shared counter (Bug B)"
            );

            assert!(
                frame_id < FRAMES,
                "workers={workers}: unexpected frame_id {frame_id}"
            );
            seen_by_frame[frame_id].push(own_index);
        }

        for (frame_id, mut idxs) in seen_by_frame.into_iter().enumerate() {
            idxs.sort_unstable();
            assert_eq!(
                idxs,
                (0..LEAVES_PER_FRAME).collect::<Vec<_>>(),
                "workers={workers}: frame {frame_id} did not dispatch exactly one \
                 instance per declared index 0..{LEAVES_PER_FRAME}: got {idxs:?}"
            );
        }
    }

    let _ = fs::remove_dir_all(&plugin_dir);
}

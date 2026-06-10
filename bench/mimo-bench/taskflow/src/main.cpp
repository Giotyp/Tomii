/**
 * tf_mimo — Taskflow MIMO benchmark (fft → csi → beam → demul)
 *
 * Receives real OFDM uplink packets from an Agora UDP sender
 * (~/Agora/run_sender.sh), runs the same four processing kernels as the
 * Tomii MIMO benchmark, and measures per-frame latency.
 *
 * Socket layout: one UDP socket per antenna, port = bs_server_port + ant_id
 * (same convention as Tomii's network.rs bind_udp_socket_range).
 *
 * Taskflow DAG per frame:
 *
 *   fft[0..total_ul_symbols]  ─────────────────────────────────────┐
 *                                                                   ├──► demul[...]
 *   csi[0..total_pilot_syms] ──► beam[0..beam_events_per_symbol] ──┘
 *
 * Dependencies encoded via tf::Task::precede():
 *   - Every fft task precedes every demul task.
 *   - Every beam task precedes every demul task.
 *   - Every csi task precedes every beam task.
 */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <complex>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <mkl_dfti.h>
#include <taskflow/taskflow.hpp>

#include "helpers.hpp"

// ---------------------------------------------------------------------------
// C ABI declarations for the precompiled .so kernels
// ---------------------------------------------------------------------------
extern "C" {

// libfftfuncs.so
void PartialTranspose(
    void* out_buffer,
    size_t ant_id,
    size_t bs_ant_num,
    SymbolType symbol_type,
    size_t ofdm_data_num,
    size_t ofdm_data_start,
    const void* fft_inout,
    const void* pilots_sgn,
    size_t TransposeBlockSize,
    size_t SCsPerCacheline);

void SimdConvertShortToFloat(
    const void* in_buf,
    void* out_buf,
    size_t n_elems);

void expand_csi(
    size_t ofdm_data_num,
    size_t bs_ant_num,
    size_t ue_ant_num,
    size_t frame_slot,
    size_t ant_id,
    const void* src_buf,
    size_t TransposeBlockSize,
    void** dst_bufs_ptr,
    size_t dst_bufs_len);

// libbeamfuncs.so
void Precoder(
    void* csi_gather_mem,
    void* ul_beam_mem,
    size_t bs_ant_num,
    size_t num_streams,
    size_t ue_num);

void PartialTransposeGather(
    size_t cur_sc_id,
    const void* src,
    void* dst,
    size_t bs_ant_num,
    bool UseSIMDGather,
    size_t TransposeBlockSize);

// libdemod.so
void Equalization(
    void* equal_buf,
    const void* data_gather_buf,
    size_t n_users,
    const void* ul_beam_buf,
    size_t bs_ant_num);

void Demod_wrap(
    size_t n_users,
    void* equaled_buffer_temp,
    void* equaled_buffer_temp_transposed,
    size_t max_sc_ite,
    size_t total_symbol_idx_ul,
    size_t mod_order,
    bool hard_demod,
    void** demod_bufs_ptr,
    size_t demod_bufs_len);

void DemulGather(
    size_t TransposeBlockSize,
    size_t base_sc_id,
    const void* data_buf,
    void* data_gather_buffer,
    bool UseSIMDGather,
    size_t SCsPerCacheline,
    size_t i,
    size_t bs_ant_num,
    size_t partial_transpose_block_base);

}  // extern "C"

// ---------------------------------------------------------------------------
// Per-slot MKL FFT descriptor (one per concurrent slot since DftiComputeForward
// is not thread-safe on the same handle)
// ---------------------------------------------------------------------------
struct MklFft {
    DFTI_DESCRIPTOR_HANDLE handle = nullptr;

    explicit MklFft(size_t ofdm_ca_num) {
        DftiCreateDescriptor(&handle, DFTI_SINGLE, DFTI_COMPLEX, 1,
                             static_cast<MKL_LONG>(ofdm_ca_num));
        DftiCommitDescriptor(handle);
    }
    ~MklFft() {
        if (handle) DftiFreeDescriptor(&handle);
    }
    MklFft(const MklFft&) = delete;
    MklFft& operator=(const MklFft&) = delete;
};

// ---------------------------------------------------------------------------
// Kernel implementations
// ---------------------------------------------------------------------------

// Thread-local FFT state: each worker thread gets its own descriptor and scratch
// buffers so concurrent do_fft / do_csi calls on the same slot don't race.
static DFTI_DESCRIPTOR_HANDLE get_tl_fft_handle(size_t ofdm_ca_num) {
    thread_local DFTI_DESCRIPTOR_HANDLE tl_handle = nullptr;
    thread_local size_t tl_fft_size = 0;
    if (tl_fft_size != ofdm_ca_num || tl_handle == nullptr) {
        if (tl_handle) DftiFreeDescriptor(&tl_handle);
        DftiCreateDescriptor(&tl_handle, DFTI_SINGLE, DFTI_COMPLEX, 1,
                             static_cast<MKL_LONG>(ofdm_ca_num));
        DftiCommitDescriptor(tl_handle);
        tl_fft_size = ofdm_ca_num;
    }
    return tl_handle;
}

// SimdConvertShortToFloat and PartialTranspose require 64-byte aligned buffers.
// std::vector only guarantees alignof(complex<float>) = 4 bytes, so we use
// posix_memalign to allocate thread-local scratch with the required alignment.
struct AlignedBuf {
    void* ptr = nullptr;
    size_t cap = 0;
    ~AlignedBuf() { if (ptr) std::free(ptr); }
    float* data(size_t n_floats) {
        if (n_floats > cap) {
            if (ptr) std::free(ptr);
            // Round up to multiple of 64 bytes (16 floats)
            size_t bytes = ((n_floats * sizeof(float) + 63) / 64) * 64;
            posix_memalign(&ptr, 64, bytes);
            cap = bytes / sizeof(float);
        }
        return reinterpret_cast<float*>(ptr);
    }
};

static std::complex<float>* get_tl_fft_inout(size_t n) {
    thread_local AlignedBuf buf;
    return reinterpret_cast<std::complex<float>*>(buf.data(n * 2));
}

static std::complex<float>* get_tl_fft_shift(size_t n) {
    thread_local AlignedBuf buf;
    return reinterpret_cast<std::complex<float>*>(buf.data(n * 2));
}

// do_fft: mirrors fft_op in fft_lib.rs
// Reads raw packet bytes, computes FFT, calls PartialTranspose into fft_buf.
static void do_fft(const void* raw_pkt, const Config& cfg,
                   SlotBuffers& slot) {
    const Packet* hdr = packet_hdr(raw_pkt);
    size_t frame_id  = hdr->frame_id;
    size_t ant_id    = hdr->ant_id;
    size_t symbol_id = hdr->symbol_id;
    SymbolType sym_type = get_symbol_type(cfg, symbol_id);
    DFTI_DESCRIPTOR_HANDLE handle = get_tl_fft_handle(cfg.ofdm_ca_num);
    std::complex<float>* fft_inout = get_tl_fft_inout(cfg.ofdm_ca_num);
    std::complex<float>* fft_shift = get_tl_fft_shift(cfg.ofdm_ca_num / 2);

    // Convert short -> float
    const int16_t* samples = packet_data(raw_pkt) + 2 * cfg.ofdm_rx_zero_prefix_bs;
    SimdConvertShortToFloat(samples, fft_inout, cfg.ofdm_ca_num * 2);

    // FFT in-place
    DftiComputeForward(handle, fft_inout);

    // fftshift
    size_t half = cfg.ofdm_ca_num / 2;
    std::memcpy(fft_shift,          fft_inout,        half * sizeof(std::complex<float>));
    std::memcpy(fft_inout,          fft_inout + half, half * sizeof(std::complex<float>));
    std::memcpy(fft_inout + half,   fft_shift,        half * sizeof(std::complex<float>));

    // PartialTranspose into fft_buf row
    size_t sym_idx_ul    = cfg.GetUlSymbolIdx(symbol_id);
    size_t total_sym_idx = cfg.GetTotalSymbolIdxUl(frame_id, sym_idx_ul);
    void*  fft_dst = slot.fft_row(total_sym_idx);

    PartialTranspose(
        fft_dst,
        ant_id,
        cfg.bs_ant_num,
        sym_type,
        cfg.ofdm_data_num,
        cfg.ofdm_data_start,
        fft_inout,
        cfg.pilots_sgn.data(),
        Config::TRANSPOSE_BLOCK_SIZE,
        Config::SCS_PER_CACHELINE);
}

// do_csi: mirrors csi_op in csi_lib.rs
static void do_csi(const void* raw_pkt, const Config& cfg,
                   SlotBuffers& slot) {
    const Packet* hdr  = packet_hdr(raw_pkt);
    size_t frame_id    = hdr->frame_id;
    size_t frame_slot  = frame_id % Config::FRAME_WND;
    size_t ant_id      = hdr->ant_id;
    size_t symbol_id   = hdr->symbol_id;
    SymbolType sym_type = get_symbol_type(cfg, symbol_id);

    DFTI_DESCRIPTOR_HANDLE handle = get_tl_fft_handle(cfg.ofdm_ca_num);
    std::complex<float>* fft_inout = get_tl_fft_inout(cfg.ofdm_ca_num);
    std::complex<float>* fft_shift = get_tl_fft_shift(cfg.ofdm_ca_num / 2);

    const int16_t* samples = packet_data(raw_pkt) + 2 * cfg.ofdm_rx_zero_prefix_bs;
    SimdConvertShortToFloat(samples, fft_inout, cfg.ofdm_ca_num * 2);

    DftiComputeForward(handle, fft_inout);

    size_t half = cfg.ofdm_ca_num / 2;
    std::memcpy(fft_shift,        fft_inout,        half * sizeof(std::complex<float>));
    std::memcpy(fft_inout,        fft_inout + half, half * sizeof(std::complex<float>));
    std::memcpy(fft_inout + half, fft_shift,        half * sizeof(std::complex<float>));

    size_t pilot_sym_idx = cfg.GetPilotSymbolIdx(symbol_id);
    void* csi_dst = slot.csi_cell(frame_slot, pilot_sym_idx, cfg.ue_ant_num);

    PartialTranspose(
        csi_dst,
        ant_id,
        cfg.bs_ant_num,
        sym_type,
        cfg.ofdm_data_num,
        cfg.ofdm_data_start,
        fft_inout,
        cfg.pilots_sgn.data(),
        Config::TRANSPOSE_BLOCK_SIZE,
        Config::SCS_PER_CACHELINE);

    // expand_csi for freq-orthogonal pilots
    if (cfg.freq_orth_pilot &&
        pilot_sym_idx == cfg.NumPilotSyms() - 1) {
        const void* src = slot.csi_cell(frame_slot, 0, cfg.ue_ant_num);
        std::vector<void*> dst_bufs(cfg.ue_ant_num);
        for (size_t ue = 0; ue < cfg.ue_ant_num; ++ue) {
            dst_bufs[ue] = slot.csi_cell(frame_slot, ue, cfg.ue_ant_num);
        }
        expand_csi(
            cfg.ofdm_data_num,
            cfg.bs_ant_num,
            cfg.ue_ant_num,
            frame_slot,
            ant_id,
            src,
            Config::TRANSPOSE_BLOCK_SIZE,
            dst_bufs.data(),
            dst_bufs.size());
    }
}

// do_beam: mirrors beam_op in beam_lib.rs (one subcarrier block)
// node_index: which beam block (0 .. beam_events_per_symbol)
static void do_beam(const Config& cfg, SlotBuffers& slot,
                    size_t frame_id, size_t node_index) {
    size_t frame_slot = frame_id % Config::FRAME_WND;
    size_t beam_block = cfg.beam_block_size;
    size_t base_sc_id = node_index * beam_block;

    size_t last_sc_id = base_sc_id +
        std::min(beam_block, cfg.ofdm_data_num - base_sc_id);

    size_t sc_inc   = 1;
    size_t start_sc = base_sc_id;
    if (cfg.freq_orth_pilot) {
        sc_inc = cfg.pilot_sc_group_size;
        size_t rem = start_sc % cfg.pilot_sc_group_size;
        if (rem != 0) start_sc += (cfg.pilot_sc_group_size - rem);
    }

    for (size_t cur_sc_id = start_sc; cur_sc_id < last_sc_id; cur_sc_id += sc_inc) {
        auto ue_list = cfg.ScheduledUeList(frame_id, cur_sc_id);
        size_t num_streams = ue_list.size();
        if (num_streams == 0) continue;

        // Gather CSI into csi_gather
        for (size_t selected_ue_idx = 0; selected_ue_idx < num_streams; ++selected_ue_idx) {
            size_t ue_idx = ue_list[selected_ue_idx];
            void* csi_src = slot.csi_cell(frame_slot, ue_idx, cfg.ue_ant_num);
            void* gather_dst = slot.csi_gather.data() + cfg.bs_ant_num * selected_ue_idx;

            PartialTransposeGather(
                cur_sc_id,
                csi_src,
                gather_dst,
                cfg.bs_ant_num,
                Config::SIMD_GATHER,
                Config::TRANSPOSE_BLOCK_SIZE);
        }

        void* ul_buf = slot.beam_cell(frame_slot, cur_sc_id, cfg.ofdm_data_num);
        Precoder(
            slot.csi_gather.data(),
            ul_buf,
            cfg.bs_ant_num,
            num_streams,
            cfg.ue_ant_num);
    }
}

// do_demul: mirrors demul_op in demul_lib.rs (one subcarrier block / UL symbol)
// node_index: global demul task index (0 .. total_demul_tasks)
// demul_events: cfg.demul_events_per_symbol
static void do_demul(const Config& cfg, SlotBuffers& slot,
                     size_t frame_id, size_t symbol_id,
                     size_t node_index, size_t demul_events) {
    size_t frame_slot = frame_id % Config::FRAME_WND;
    size_t base_sc_id = (node_index % demul_events) * cfg.demul_block_size;

    size_t sym_idx_ul = cfg.GetUlSymbolIdx(symbol_id);
    size_t total_sym_idx_ul = cfg.GetTotalSymbolIdxUl(frame_id, sym_idx_ul);

    // client_ul_pilot_symbols = 0 in our 4x4 config
    size_t data_sym_idx_ul = sym_idx_ul;  // sym_idx_ul - client_ul_pilot_symbols

    size_t max_sc_ite = std::min(cfg.demul_block_size,
                                  cfg.ofdm_data_num - base_sc_id);
    assert(max_sc_ite % Config::SCS_PER_CACHELINE == 0);

    // Thread-local working buffers — mirrors AlignedVec in Rust demul_op.
    // Sizes: data_gather = SCS_PER_CACHELINE * bs_ant_num,
    //        equaled_{temp,trans} = demul_block * num_spatial_streams.
    static thread_local AlignedBuf tl_data_gather;
    static thread_local AlignedBuf tl_equaled_temp;
    static thread_local AlignedBuf tl_equaled_trans;
    size_t dg_floats   = Config::SCS_PER_CACHELINE * cfg.bs_ant_num * 2;
    size_t eq_floats   = cfg.demul_block_size * cfg.num_spatial_streams * 2;
    auto* data_gather  = reinterpret_cast<std::complex<float>*>(tl_data_gather.data(dg_floats));
    auto* equaled_temp = reinterpret_cast<std::complex<float>*>(tl_equaled_temp.data(eq_floats));
    auto* equaled_trans= reinterpret_cast<std::complex<float>*>(tl_equaled_trans.data(eq_floats));

    const void* fft_data = slot.fft_row_const(total_sym_idx_ul);

    for (size_t i = 0; i < max_sc_ite; i += Config::SCS_PER_CACHELINE) {
        size_t pt_block_base =
            ((base_sc_id + i) / Config::TRANSPOSE_BLOCK_SIZE) *
            (Config::TRANSPOSE_BLOCK_SIZE * cfg.bs_ant_num);
        DemulGather(
            Config::TRANSPOSE_BLOCK_SIZE,
            base_sc_id,
            fft_data,
            data_gather,
            Config::SIMD_GATHER,
            Config::SCS_PER_CACHELINE,
            i,
            cfg.bs_ant_num,
            pt_block_base);

        for (size_t j = 0; j < Config::SCS_PER_CACHELINE; ++j) {
            size_t cur_sc_id = base_sc_id + i + j;
            size_t offset = j * cfg.bs_ant_num;

            const void* beam_sc =
                slot.beam_cell_const(frame_slot,
                                     cfg.GetBeamScId(cur_sc_id),
                                     cfg.ofdm_data_num);

            size_t eq_offset = (cur_sc_id - base_sc_id) * cfg.num_spatial_streams;
            Equalization(
                equaled_temp + eq_offset,
                data_gather + offset,
                cfg.num_spatial_streams,
                beam_sc,
                cfg.bs_ant_num);
        }
    }

    // Demodulation (only for data symbols — client_ul_pilot_symbols = 0)
    std::vector<void*> demod_ptrs(cfg.num_spatial_streams);
    for (size_t ss_id = 0; ss_id < cfg.num_spatial_streams; ++ss_id) {
        int8_t* demod_base =
            slot.demod_cell(frame_slot, data_sym_idx_ul, ss_id,
                            cfg.ul_symbols.size());
        demod_ptrs[ss_id] =
            demod_base + cfg.ul_mod_order_bits * base_sc_id;
    }

    Demod_wrap(
        cfg.num_spatial_streams,
        equaled_temp,
        equaled_trans,
        max_sc_ite,
        total_sym_idx_ul,
        cfg.ul_mod_order_bits,
        Config::UPLINK_HARD_DEMOD,
        demod_ptrs.data(),
        demod_ptrs.size());
}

// ---------------------------------------------------------------------------
// UDP receiver: one socket per antenna, poll with recvmmsg
// ---------------------------------------------------------------------------
struct UdpReceiver {
    std::vector<int> fds;
    size_t pkt_len;
    size_t n_antennas;

    void init(const Config& cfg) {
        pkt_len    = cfg.packet_length();
        n_antennas = cfg.bs_ant_num;
        fds.resize(n_antennas, -1);

        for (size_t i = 0; i < n_antennas; ++i) {
            int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
            if (fd < 0) throw std::runtime_error("socket()");

            // Increase socket receive buffer (16 MiB to handle 16×16 bursts)
            int rcvbuf = 1 << 24;  // 16 MiB
            ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

            sockaddr_in addr{};
            addr.sin_family      = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port        = htons(
                static_cast<uint16_t>(cfg.bs_server_port + static_cast<int>(i)));

            if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                ::close(fd);
                throw std::runtime_error(
                    "bind() port " +
                    std::to_string(cfg.bs_server_port + static_cast<int>(i)));
            }
            // 50ms receive timeout: prevents blocking forever when sender
            // stops, allowing the main loop to drain completed futures.
            struct timeval tv{0, 50000};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            fds[i] = fd;
        }
    }

    ~UdpReceiver() {
        for (int fd : fds) if (fd >= 0) ::close(fd);
    }

    // Receive one packet from any socket (round-robin, blocking recvfrom).
    // Returns the raw byte buffer (packet_length bytes).
    // Caller provides the storage; returns pointer to it on success.
    // Uses mmsg batch on the individual socket that has data.
    bool recv_one(std::vector<uint8_t>& buf, size_t& ant_out) {
        // Round-robin across antennas using a simple static counter
        // to avoid unfair blocking on a single fd.
        static size_t rr = 0;
        for (size_t tries = 0; tries < n_antennas * 4; ++tries) {
            size_t ant = rr % n_antennas;
            ++rr;

            ssize_t n = ::recv(fds[ant], buf.data(), buf.size(), MSG_DONTWAIT);
            if (n > 0) {
                ant_out = ant;
                return true;
            }
        }
        // Blocking receive on current round-robin socket as fallback
        size_t ant = rr % n_antennas;
        ++rr;
        ssize_t n = ::recv(fds[ant], buf.data(), buf.size(), 0);
        if (n > 0) { ant_out = ant; return true; }
        return false;
    }
};

// ---------------------------------------------------------------------------
// Per-frame packet store: holds all received packets for one frame.
// Indexed by (symbol_id * bs_ant_num + ant_id).
// ---------------------------------------------------------------------------
struct FramePackets {
    size_t n_slots;   // packets_per_frame
    size_t pkt_len;
    std::vector<std::vector<uint8_t>> pkts;
    std::vector<bool> received;

    void init(size_t n, size_t len) {
        n_slots = n;
        pkt_len = len;
        pkts.assign(n, std::vector<uint8_t>(len, 0));
        received.assign(n, false);
    }

    void reset() {
        std::fill(received.begin(), received.end(), false);
    }

    bool store(const uint8_t* data, size_t symbol_id, size_t ant_id,
               size_t bs_ant_num) {
        size_t idx = symbol_id * bs_ant_num + ant_id;
        if (idx >= n_slots) return false;
        std::memcpy(pkts[idx].data(), data, pkt_len);
        received[idx] = true;
        return true;
    }

    const uint8_t* get(size_t symbol_id, size_t ant_id, size_t bs_ant_num) const {
        return pkts[symbol_id * bs_ant_num + ant_id].data();
    }
};

// ---------------------------------------------------------------------------
// Build the Taskflow DAG for one frame.
//
// Task graph:
//   fft[sym][ant]   — one task per (ul_symbol, antenna) packet
//   csi[sym][ant]   — one task per (pilot_symbol, antenna) packet
//   beam[b]         — one task per beam subcarrier block
//   demul[sym][b]   — one task per (ul_symbol, demul block)
//
// Dependencies:
//   All csi tasks → all beam tasks → all demul tasks
//   All fft tasks → all demul tasks (via fft buffer readiness)
//
// Note: because beam tasks share csi_gather and the Precoder buffer
// for a single slot, beam tasks are serialised via a single dependency
// chain: csi_done_gate → beam[0] → beam[1] → ... → beam_done_gate
// This matches the barrier semantics in the Tomii graph.
// ---------------------------------------------------------------------------
struct FrameFlow {
    tf::Taskflow flow;
    std::vector<tf::Task> fft_tasks;   // ul_syms × bs_ant_num
    std::vector<tf::Task> csi_tasks;   // pilot_syms × bs_ant_num
    std::vector<tf::Task> beam_tasks;  // beam_events_per_symbol
    std::vector<tf::Task> demul_tasks; // ul_syms × demul_events

    tf::Task csi_sync;   // barrier: all csi done
    tf::Task fft_sync;   // barrier: all fft done  (gate for demul)
    tf::Task beam_sync;  // barrier: all beam done (gate for demul)

    void build(
        const Config& cfg,
        SlotBuffers& slot,
        const FramePackets& frame_pkts,
        size_t frame_id)
    {
        flow.clear();
        fft_tasks.clear();
        csi_tasks.clear();
        beam_tasks.clear();
        demul_tasks.clear();

        // Synchronisation gates (empty tasks used as fan-in/fan-out points)
        csi_sync  = flow.emplace([](){}).name("csi_sync");
        fft_sync  = flow.emplace([](){}).name("fft_sync");
        beam_sync = flow.emplace([](){}).name("beam_sync");

        // --- FFT tasks: one per (ul_symbol_idx, ant_id) ---
        size_t ul_syms = cfg.NumUlSyms();
        fft_tasks.reserve(ul_syms * cfg.bs_ant_num);
        for (size_t si = 0; si < ul_syms; ++si) {
            size_t sym_id = cfg.ul_symbols[si];
            for (size_t ant = 0; ant < cfg.bs_ant_num; ++ant) {
                const uint8_t* pkt =
                    frame_pkts.get(sym_id, ant, cfg.bs_ant_num);
                auto t = flow.emplace([pkt, &cfg, &slot]() {
                    do_fft(pkt, cfg, slot);
                }).name("fft_s" + std::to_string(si) + "_a" + std::to_string(ant));
                t.precede(fft_sync);
                fft_tasks.push_back(t);
            }
        }

        // --- CSI tasks: one per (pilot_symbol_idx, ant_id) ---
        size_t pilot_syms = cfg.NumPilotSyms();
        csi_tasks.reserve(pilot_syms * cfg.bs_ant_num);
        for (size_t si = 0; si < pilot_syms; ++si) {
            size_t sym_id = cfg.pilot_symbols[si];
            for (size_t ant = 0; ant < cfg.bs_ant_num; ++ant) {
                const uint8_t* pkt =
                    frame_pkts.get(sym_id, ant, cfg.bs_ant_num);
                auto t = flow.emplace([pkt, &cfg, &slot]() {
                    do_csi(pkt, cfg, slot);
                }).name("csi_s" + std::to_string(si) + "_a" + std::to_string(ant));
                t.precede(csi_sync);
                csi_tasks.push_back(t);
            }
        }

        // --- Beam tasks: serialised chain (csi_sync → beam[0] → ... → beam_sync) ---
        // Beam tasks share csi_gather within the slot buffer; serialising them
        // avoids a per-task allocation while still letting the executor overlap
        // beam with FFT work on other frames.
        beam_tasks.reserve(cfg.beam_events_per_symbol);
        tf::Task prev = csi_sync;
        for (size_t b = 0; b < cfg.beam_events_per_symbol; ++b) {
            auto t = flow.emplace([b, frame_id, &cfg, &slot]() {
                do_beam(cfg, slot, frame_id, b);
            }).name("beam_" + std::to_string(b));
            prev.precede(t);
            prev = t;
            beam_tasks.push_back(t);
        }
        prev.precede(beam_sync);

        // --- Demul tasks: one per (ul_symbol_idx, demul_block) ---
        // Fired after fft_sync AND beam_sync.
        size_t demul_events = cfg.demul_events_per_symbol;
        demul_tasks.reserve(ul_syms * demul_events);
        for (size_t si = 0; si < ul_syms; ++si) {
            size_t sym_id = cfg.ul_symbols[si];
            for (size_t di = 0; di < demul_events; ++di) {
                size_t node_idx = si * demul_events + di;
                auto t = flow.emplace([node_idx, frame_id, sym_id, demul_events,
                                       &cfg, &slot]() {
                    do_demul(cfg, slot, frame_id, sym_id, node_idx, demul_events);
                }).name("demul_s" + std::to_string(si) + "_b" + std::to_string(di));
                // Wait for all FFT results and all beam weights
                fft_sync.precede(t);
                beam_sync.precede(t);
                demul_tasks.push_back(t);
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
static void print_usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "  --slots    N    concurrent slot count (default: 1)\n"
        << "  --workers  N    tf::Executor thread count (default: 4)\n"
        << "  --streams  N    frames to time after warmup (default: 2000)\n"
        << "  --warmup   N    warmup frames (default: 200)\n"
        << "  --output   FILE CSV output path (default: tf_mimo_sweep.csv)\n"
        << "  --config   FILE tddconfig JSON (default: ../tomii/graphs/tddconfig-4x4.json)\n";
}

int main(int argc, char* argv[]) {
    // --- Parse CLI ---
    size_t num_slots   = 1;
    size_t num_workers = 4;
    size_t num_streams = 2000;
    size_t num_warmup  = 200;
    std::string output_csv = "tf_mimo_sweep.csv";
    std::string config_path =
        std::string(argv[0]).substr(0, std::string(argv[0]).rfind('/'))
        + "/../../../tomii/graphs/tddconfig-4x4.json";

    // Determine default config path relative to binary location
    {
        // Try a relative path from CWD first
        std::ifstream probe("../tomii/graphs/tddconfig-4x4.json");
        if (probe.good()) {
            config_path = "../tomii/graphs/tddconfig-4x4.json";
        }
    }

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--slots" || arg == "-s") && i + 1 < argc)
            num_slots = std::stoull(argv[++i]);
        else if ((arg == "--workers" || arg == "-w") && i + 1 < argc)
            num_workers = std::stoull(argv[++i]);
        else if (arg == "--streams" && i + 1 < argc)
            num_streams = std::stoull(argv[++i]);
        else if (arg == "--warmup" && i + 1 < argc)
            num_warmup = std::stoull(argv[++i]);
        else if (arg == "--output" && i + 1 < argc)
            output_csv = argv[++i];
        else if (arg == "--config" && i + 1 < argc)
            config_path = argv[++i];
        else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    // --- Load config ---
    Config cfg;
    try {
        cfg = load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "Failed to load config '" << config_path << "': "
                  << e.what() << "\n";
        return 1;
    }

    std::cout << "Config: bs_ant=" << cfg.bs_ant_num
              << " ue_ant=" << cfg.ue_ant_num
              << " ofdm_ca=" << cfg.ofdm_ca_num
              << " ofdm_data=" << cfg.ofdm_data_num
              << " ul_syms=" << cfg.NumUlSyms()
              << " pilot_syms=" << cfg.NumPilotSyms()
              << " beam_events=" << cfg.beam_events_per_symbol
              << " demul_events=" << cfg.demul_events_per_symbol
              << " schedule=" << cfg.frame_schedule
              << "\n";
    std::cout << "Benchmark: slots=" << num_slots
              << " workers=" << num_workers
              << " streams=" << num_streams
              << " warmup=" << num_warmup << "\n";

    // --- Allocate per-slot buffers ---
    std::vector<SlotBuffers> slots(num_slots);
    for (size_t s = 0; s < num_slots; ++s) {
        slots[s].init(cfg);
    }

    // --- Init UDP receiver ---
    UdpReceiver rx;
    try {
        rx.init(cfg);
    } catch (const std::exception& e) {
        std::cerr << "UDP init failed: " << e.what() << "\n";
        return 1;
    }

    size_t pkt_len  = cfg.packet_length();
    size_t pkts_per_frame = cfg.packets_per_frame();

    // Per-slot packet stores
    std::vector<FramePackets> frame_stores(num_slots);
    for (size_t s = 0; s < num_slots; ++s) {
        frame_stores[s].init(pkts_per_frame, pkt_len);
    }

    // Shared receive buffer
    std::vector<uint8_t> recv_buf(pkt_len + 128);

    // --- Taskflow executor ---
    tf::Executor executor(num_workers);

    // Pre-built flows (one per slot, reused per frame)
    std::vector<FrameFlow> flows(num_slots);

    // --- Frame tracking ---
    // frame_id -> slot assignment: slot_id = frame_id % num_slots
    // packet_count[slot]: how many packets received for current frame in slot
    std::vector<size_t> pkt_count(num_slots, 0);
    std::vector<size_t> slot_frame(num_slots, SIZE_MAX);  // which frame_id owns this slot
    std::vector<std::future<void>> futures(num_slots);
    std::vector<bool> slot_in_flight(num_slots, false);

    size_t total_frames = num_warmup + num_streams;
    size_t frames_submitted = 0;
    size_t frames_completed = 0;

    // CSV output: write header if file does not exist yet
    bool write_header = false;
    {
        std::ifstream probe(output_csv);
        write_header = !probe.good();
    }
    std::ofstream csv(output_csv, std::ios::app);
    if (!csv) {
        std::cerr << "Cannot open output CSV: " << output_csv << "\n";
        return 1;
    }
    if (write_header) {
        csv << "system,slots,workers,streams,ms_per_slot\n";
    }

    // Timing methodology (matches Tomii's "Avg Time Per Stream"):
    //   slot_first_pkt_time: when the FIRST packet of a frame arrives at a slot.
    //   slot_submit_time:    when the LAST packet arrives and the DAG is submitted.
    //   Latency reported = t_complete - slot_first_pkt_time (first-pkt → done).
    // This is the same window Tomii measures: from when the slot first receives
    // data for a stream to when all tasks finish — a true end-to-end pipeline view.
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    std::vector<TimePoint> slot_submit_time(num_slots);
    std::vector<TimePoint> slot_first_pkt_time(num_slots);
    std::vector<bool> slot_first_pkt_set(num_slots, false);
    double total_pipeline_ms = 0.0;   // first-pkt → complete (primary metric)
    double total_compute_ms = 0.0;    // submit → complete (reference)
    size_t timed_frames = 0;
    // Wall-clock timing (total elapsed / total streams)
    Clock::time_point t_start;
    bool timing_started = false;

    std::cout << "Waiting for packets on port " << cfg.bs_server_port
              << " .. " << cfg.bs_server_port + static_cast<int>(cfg.bs_ant_num) - 1
              << "\n" << std::flush;

    // Stall detection: if no frame completes for 30 seconds, sender has stopped.
    auto last_progress = Clock::now();

    // --- Main receive loop ---
    while (frames_completed < total_frames) {
        // Check stall timeout
        auto now = Clock::now();
        if (std::chrono::duration<double>(now - last_progress).count() > 30.0) {
            std::cerr << "Stall timeout: no progress for 30s ("
                      << frames_completed << "/" << total_frames
                      << " frames). Sender may have stopped.\n";
            break;
        }

        // Drain any completed slots
        for (size_t s = 0; s < num_slots; ++s) {
            if (slot_in_flight[s]) {
                if (futures[s].wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready) {
                    futures[s].get();
                    auto t_complete = Clock::now();
                    slot_in_flight[s] = false;
                    ++frames_completed;
                    last_progress = t_complete;

                    // Accumulate per-frame latency (post-warmup)
                    if (frames_completed > num_warmup) {
                        double compute_ms = std::chrono::duration<double, std::milli>(
                            t_complete - slot_submit_time[s]).count();
                        total_compute_ms += compute_ms;
                        if (slot_first_pkt_set[s]) {
                            double pipeline_ms = std::chrono::duration<double, std::milli>(
                                t_complete - slot_first_pkt_time[s]).count();
                            total_pipeline_ms += pipeline_ms;
                        }
                        ++timed_frames;
                    }
                    slot_first_pkt_set[s] = false;

                    if (frames_completed == num_warmup) {
                        t_start = Clock::now();
                        timing_started = true;
                    }
                    frame_stores[s].reset();
                    pkt_count[s] = 0;
                    slot_frame[s] = SIZE_MAX;
                }
            }
        }

        // Receive one packet
        size_t ant_out = 0;
        if (!rx.recv_one(recv_buf, ant_out)) continue;

        const Packet* hdr = packet_hdr(recv_buf.data());
        size_t frame_id  = hdr->frame_id;
        size_t symbol_id = hdr->symbol_id;
        size_t ant_id    = hdr->ant_id;
        size_t slot_id   = frame_id % num_slots;

        // Only accept packets whose slot is not currently in-flight
        if (slot_in_flight[slot_id]) continue;

        // Assign or validate slot ownership
        if (slot_frame[slot_id] == SIZE_MAX) {
            // New frame for this slot
            if (frames_submitted >= total_frames) continue;
            slot_frame[slot_id] = frame_id;
            pkt_count[slot_id]  = 0;
            // Record first-packet timestamp for this frame (matches Tomii's slot activation time)
            slot_first_pkt_time[slot_id] = Clock::now();
            slot_first_pkt_set[slot_id]  = true;
        } else if (slot_frame[slot_id] != frame_id) {
            // Different frame trying to use same slot — skip (old/future frame)
            continue;
        }

        // Store packet indexed by (symbol_id * bs_ant_num + ant_id)
        frame_stores[slot_id].store(recv_buf.data(), symbol_id, ant_id, cfg.bs_ant_num);
        ++pkt_count[slot_id];

        // When all packets for this frame have arrived, launch the DAG
        if (pkt_count[slot_id] >= pkts_per_frame) {
            ++frames_submitted;
            slot_in_flight[slot_id] = true;
            slot_submit_time[slot_id] = Clock::now();

            // Build and submit DAG for this slot/frame
            flows[slot_id].build(
                cfg,
                slots[slot_id],
                frame_stores[slot_id],
                frame_id);

            futures[slot_id] = executor.run(flows[slot_id].flow);
        }
    }

    // Wait for any remaining in-flight slots
    for (size_t s = 0; s < num_slots; ++s) {
        if (slot_in_flight[s]) {
            futures[s].get();
            auto t_complete = Clock::now();
            ++frames_completed;
            if (frames_completed > num_warmup) {
                double compute_ms = std::chrono::duration<double, std::milli>(
                    t_complete - slot_submit_time[s]).count();
                total_compute_ms += compute_ms;
                if (slot_first_pkt_set[s]) {
                    double pipeline_ms = std::chrono::duration<double, std::milli>(
                        t_complete - slot_first_pkt_time[s]).count();
                    total_pipeline_ms += pipeline_ms;
                }
                ++timed_frames;
            }
        }
    }

    // Per-frame pipeline latency: first-packet → last-task-done (matches Tomii's metric)
    double ms_per_slot_pipeline = (timed_frames > 0)
        ? (total_pipeline_ms / static_cast<double>(timed_frames))
        : 0.0;
    // Per-frame compute latency: last-packet-submit → last-task-done (reference)
    double ms_per_slot_compute = (timed_frames > 0)
        ? (total_compute_ms / static_cast<double>(timed_frames))
        : 0.0;
    // Wall-clock: total elapsed / total streams (sender-rate limited)
    double ms_per_slot_wall = 0.0;
    if (timing_started && num_streams > 0) {
        auto t_end = Clock::now();
        double elapsed_ms =
            std::chrono::duration<double, std::milli>(t_end - t_start).count();
        ms_per_slot_wall = elapsed_ms / static_cast<double>(num_streams);
    }

    std::cout << "Done. " << timed_frames << " frames timed. "
              << ms_per_slot_pipeline << " ms/slot (pipeline, first-pkt→done) | "
              << ms_per_slot_compute  << " ms/slot (compute, submit→done) | "
              << ms_per_slot_wall     << " ms/slot (wall)\n";

    // Pipeline latency matches Tomii's "Avg Time Per Stream" (first-pkt → last-task-done).
    csv << "taskflow," << num_slots << "," << num_workers << ","
        << num_streams << "," << ms_per_slot_pipeline << "\n";

    return 0;
}

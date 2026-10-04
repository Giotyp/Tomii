// kernels.hpp — the four MIMO uplink kernels shared by every C++ baseline.
//
// Bodies are the ones from taskflow/src/main.cpp (which mirror the Tomii plugin
// in tomii/src/{fft,csi,beam,demul}_lib.rs) and call the SAME precompiled
// C-ABI kernels (libfftfuncs.so, libbeamfuncs.so, libdemod.so, MKL DFTI) that
// the Tomii plugin calls. Two deliberate changes vs. main.cpp, both of which
// bring the C++ side to parity with the Rust plugin:
//   * do_beam uses a THREAD-LOCAL csi_gather scratch (Rust: TL_CSI_GATHER), so
//     beam tasks of one frame may run in parallel (main.cpp serialised all beam
//     tasks of a frame in a chain because they shared one per-slot scratch);
//   * do_beam zeroes the gather region before filling it (Rust does the same;
//     a no-op for the all-UEs-scheduled configs used here).
#pragma once
#include <algorithm>
#include <cassert>
#include <complex>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <mkl_dfti.h>

#include "helpers.hpp"

extern "C" {
void PartialTranspose(void* out_buffer, size_t ant_id, size_t bs_ant_num, SymbolType symbol_type,
                      size_t ofdm_data_num, size_t ofdm_data_start, const void* fft_inout,
                      const void* pilots_sgn, size_t TransposeBlockSize, size_t SCsPerCacheline);
void SimdConvertShortToFloat(const void* in_buf, void* out_buf, size_t n_elems);
void expand_csi(size_t ofdm_data_num, size_t bs_ant_num, size_t ue_ant_num, size_t frame_slot,
                size_t ant_id, const void* src_buf, size_t TransposeBlockSize, void** dst_bufs_ptr,
                size_t dst_bufs_len);
void Precoder(void* csi_gather_mem, void* ul_beam_mem, size_t bs_ant_num, size_t num_streams,
              size_t ue_num);
void PartialTransposeGather(size_t cur_sc_id, const void* src, void* dst, size_t bs_ant_num,
                            bool UseSIMDGather, size_t TransposeBlockSize);
void Equalization(void* equal_buf, const void* data_gather_buf, size_t n_users,
                  const void* ul_beam_buf, size_t bs_ant_num);
void Demod_wrap(size_t n_users, void* equaled_buffer_temp, void* equaled_buffer_temp_transposed,
                size_t max_sc_ite, size_t total_symbol_idx_ul, size_t mod_order, bool hard_demod,
                void** demod_bufs_ptr, size_t demod_bufs_len);
void DemulGather(size_t TransposeBlockSize, size_t base_sc_id, const void* data_buf,
                 void* data_gather_buffer, bool UseSIMDGather, size_t SCsPerCacheline, size_t i,
                 size_t bs_ant_num, size_t partial_transpose_block_base);
}

namespace e1 {

struct AlignedBuf {
    void* ptr = nullptr;
    size_t cap = 0;
    ~AlignedBuf() { if (ptr) std::free(ptr); }
    float* data(size_t n_floats) {
        if (n_floats > cap) {
            if (ptr) std::free(ptr);
            size_t bytes = ((n_floats * sizeof(float) + 63) / 64) * 64;
            if (posix_memalign(&ptr, 64, bytes) != 0) std::abort();
            cap = bytes / sizeof(float);
        }
        return reinterpret_cast<float*>(ptr);
    }
};

inline DFTI_DESCRIPTOR_HANDLE tl_fft_handle(size_t n) {
    thread_local DFTI_DESCRIPTOR_HANDLE h = nullptr;
    thread_local size_t sz = 0;
    if (sz != n || h == nullptr) {
        if (h) DftiFreeDescriptor(&h);
        // Parenthesised name = call the generic variadic DftiCreateDescriptor
        // entry point, exactly what the Tomii plugin's Rust binding calls,
        // instead of the mkl_dfti.h macro (which dispatches to
        // DftiCreateDescriptor_s_1d). The two commit different FFT kernels whose
        // outputs differ in the last ulp (tested: 1927/2048 bins at N=2048), which
        // propagated to +/-1 LLR differences in the demod output at 16x16/64x16.
        // Cost: ~0.07 us per 2048-pt FFT slower than the macro path (1.03 vs
        // 0.97 us), i.e. the same kernel Tomii pays.
        (DftiCreateDescriptor)(&h, DFTI_SINGLE, DFTI_COMPLEX, 1, static_cast<MKL_LONG>(n));
        DftiCommitDescriptor(h);
        sz = n;
    }
    return h;
}
inline std::complex<float>* tl_fft_inout(size_t n) {
    thread_local AlignedBuf b;
    return reinterpret_cast<std::complex<float>*>(b.data(n * 2));
}
inline std::complex<float>* tl_fft_shift(size_t n) {
    thread_local AlignedBuf b;
    return reinterpret_cast<std::complex<float>*>(b.data(n * 2));
}

// Shared FFT front half of do_fft/do_csi.
inline std::complex<float>* fft_front(const void* raw_pkt, const Config& cfg) {
    DFTI_DESCRIPTOR_HANDLE handle = tl_fft_handle(cfg.ofdm_ca_num);
    std::complex<float>* io = tl_fft_inout(cfg.ofdm_ca_num);
    std::complex<float>* sh = tl_fft_shift(cfg.ofdm_ca_num / 2);
    const int16_t* samples = packet_data(raw_pkt) + 2 * cfg.ofdm_rx_zero_prefix_bs;
    SimdConvertShortToFloat(samples, io, cfg.ofdm_ca_num * 2);
    DftiComputeForward(handle, io);
    size_t half = cfg.ofdm_ca_num / 2;
    std::memcpy(sh, io, half * sizeof(std::complex<float>));
    std::memcpy(io, io + half, half * sizeof(std::complex<float>));
    std::memcpy(io + half, sh, half * sizeof(std::complex<float>));
    return io;
}

inline void do_fft(const void* raw_pkt, const Config& cfg, SlotBuffers& slot) {
    const Packet* hdr = packet_hdr(raw_pkt);
    std::complex<float>* io = fft_front(raw_pkt, cfg);
    size_t sym_idx_ul = cfg.GetUlSymbolIdx(hdr->symbol_id);
    size_t total_sym_idx = cfg.GetTotalSymbolIdxUl(hdr->frame_id, sym_idx_ul);
    PartialTranspose(slot.fft_row(total_sym_idx), hdr->ant_id, cfg.bs_ant_num,
                     get_symbol_type(cfg, hdr->symbol_id), cfg.ofdm_data_num, cfg.ofdm_data_start,
                     io, cfg.pilots_sgn.data(), Config::TRANSPOSE_BLOCK_SIZE,
                     Config::SCS_PER_CACHELINE);
}

inline void do_csi(const void* raw_pkt, const Config& cfg, SlotBuffers& slot) {
    const Packet* hdr = packet_hdr(raw_pkt);
    size_t frame_slot = hdr->frame_id % Config::FRAME_WND;
    std::complex<float>* io = fft_front(raw_pkt, cfg);
    size_t pilot_sym_idx = cfg.GetPilotSymbolIdx(hdr->symbol_id);
    PartialTranspose(slot.csi_cell(frame_slot, pilot_sym_idx, cfg.ue_ant_num), hdr->ant_id,
                     cfg.bs_ant_num, get_symbol_type(cfg, hdr->symbol_id), cfg.ofdm_data_num,
                     cfg.ofdm_data_start, io, cfg.pilots_sgn.data(), Config::TRANSPOSE_BLOCK_SIZE,
                     Config::SCS_PER_CACHELINE);
    if (cfg.freq_orth_pilot && pilot_sym_idx == cfg.NumPilotSyms() - 1) {
        const void* src = slot.csi_cell(frame_slot, 0, cfg.ue_ant_num);
        void* dst[64];
        for (size_t ue = 0; ue < cfg.ue_ant_num; ++ue)
            dst[ue] = slot.csi_cell(frame_slot, ue, cfg.ue_ant_num);
        expand_csi(cfg.ofdm_data_num, cfg.bs_ant_num, cfg.ue_ant_num, frame_slot, hdr->ant_id, src,
                   Config::TRANSPOSE_BLOCK_SIZE, dst, cfg.ue_ant_num);
    }
}

inline void do_beam(const Config& cfg, SlotBuffers& slot, size_t frame_id, size_t node_index) {
    // Rust: thread_local RefCell<Vec<Complex32>> of MaxAntennas*MaxUEs (malloc
    // alignment, not 64 B) — mirrored so Precoder sees the same operand layout.
    thread_local std::vector<std::complex<float>> tl_gather(64 * 64);
    auto* gather = tl_gather.data();
    size_t frame_slot = frame_id % Config::FRAME_WND;
    size_t base_sc_id = node_index * cfg.beam_block_size;
    size_t last_sc_id = base_sc_id + std::min(cfg.beam_block_size, cfg.ofdm_data_num - base_sc_id);
    size_t sc_inc = 1, start_sc = base_sc_id;
    if (cfg.freq_orth_pilot) {
        sc_inc = cfg.pilot_sc_group_size;
        size_t rem = start_sc % cfg.pilot_sc_group_size;
        if (rem != 0) start_sc += (cfg.pilot_sc_group_size - rem);
    }
    for (size_t sc = start_sc; sc < last_sc_id; sc += sc_inc) {
        auto ue_list = cfg.ScheduledUeList(frame_id, sc);
        size_t ns = ue_list.size();
        if (ns == 0) continue;
        std::memset(gather, 0, cfg.bs_ant_num * cfg.ue_ant_num * sizeof(std::complex<float>));
        for (size_t k = 0; k < ns; ++k) {
            PartialTransposeGather(sc, slot.csi_cell(frame_slot, ue_list[k], cfg.ue_ant_num),
                                   gather + cfg.bs_ant_num * k, cfg.bs_ant_num,
                                   Config::SIMD_GATHER, Config::TRANSPOSE_BLOCK_SIZE);
        }
        Precoder(gather, slot.beam_cell(frame_slot, sc, cfg.ofdm_data_num), cfg.bs_ant_num, ns,
                 cfg.ue_ant_num);
    }
}

inline void do_demul(const Config& cfg, SlotBuffers& slot, size_t frame_id, size_t symbol_id,
                     size_t node_index, size_t demul_events) {
    size_t frame_slot = frame_id % Config::FRAME_WND;
    size_t base_sc_id = (node_index % demul_events) * cfg.demul_block_size;
    size_t sym_idx_ul = cfg.GetUlSymbolIdx(symbol_id);
    size_t total_sym_idx_ul = cfg.GetTotalSymbolIdxUl(frame_id, sym_idx_ul);
    size_t data_sym_idx_ul = sym_idx_ul;
    size_t max_sc_ite = std::min(cfg.demul_block_size, cfg.ofdm_data_num - base_sc_id);
    assert(max_sc_ite % Config::SCS_PER_CACHELINE == 0);

    thread_local AlignedBuf tl_dg, tl_eq, tl_eqt;
    size_t dg_floats = Config::SCS_PER_CACHELINE * cfg.bs_ant_num * 2;
    size_t eq_floats = cfg.demul_block_size * cfg.num_spatial_streams * 2;
    auto* dg = reinterpret_cast<std::complex<float>*>(tl_dg.data(dg_floats));
    auto* eq = reinterpret_cast<std::complex<float>*>(tl_eq.data(eq_floats));
    auto* eqt = reinterpret_cast<std::complex<float>*>(tl_eqt.data(eq_floats));
    const void* fft_data = slot.fft_row_const(total_sym_idx_ul);

    for (size_t i = 0; i < max_sc_ite; i += Config::SCS_PER_CACHELINE) {
        size_t ptb = ((base_sc_id + i) / Config::TRANSPOSE_BLOCK_SIZE) *
                     (Config::TRANSPOSE_BLOCK_SIZE * cfg.bs_ant_num);
        DemulGather(Config::TRANSPOSE_BLOCK_SIZE, base_sc_id, fft_data, dg, Config::SIMD_GATHER,
                    Config::SCS_PER_CACHELINE, i, cfg.bs_ant_num, ptb);
        for (size_t j = 0; j < Config::SCS_PER_CACHELINE; ++j) {
            size_t sc = base_sc_id + i + j;
            const std::complex<float>* beam_sc = slot.beam_cell_const(
                frame_slot, cfg.GetBeamScId(sc), cfg.ofdm_data_num);
            // Mirror the Rust plugin exactly: it hands Equalization freshly
            // heap-allocated copies (`.to_vec()`) of the gathered samples and of
            // the beam cell. Besides costing the same work, this reproduces the
            // operands' (malloc) alignment, which MKL's small-matrix paths are
            // sensitive to — required for byte-identical output vs. Tomii.
            std::vector<std::complex<float>> data_vec(dg + j * cfg.bs_ant_num,
                                                      dg + (j + 1) * cfg.bs_ant_num);
            std::vector<std::complex<float>> beam_vec(
                beam_sc, beam_sc + cfg.bs_ant_num * cfg.ue_ant_num);
            Equalization(eq + (sc - base_sc_id) * cfg.num_spatial_streams, data_vec.data(),
                         cfg.num_spatial_streams, beam_vec.data(), cfg.bs_ant_num);
        }
    }
    void* demod_ptrs[64];
    for (size_t ss = 0; ss < cfg.num_spatial_streams; ++ss) {
        int8_t* base = slot.demod_cell(frame_slot, data_sym_idx_ul, ss, cfg.ul_symbols.size());
        demod_ptrs[ss] = base + cfg.ul_mod_order_bits * base_sc_id;
    }
    Demod_wrap(cfg.num_spatial_streams, eq, eqt, max_sc_ite, total_sym_idx_ul,
               cfg.ul_mod_order_bits, Config::UPLINK_HARD_DEMOD, demod_ptrs,
               cfg.num_spatial_streams);
}

}  // namespace e1

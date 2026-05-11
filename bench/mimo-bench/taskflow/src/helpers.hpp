#pragma once
#include <algorithm>
#include <complex>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Packet layout
// Matches Rust Packet #[repr(C, align(64))]:
//   frame_id (u32), symbol_id (u32), cell_id (u32), ant_id (u32),
//   fill[12] × u32  ==> 4 + 12 = 16 × 4 = 64 bytes header
//   then raw i16 IQ samples follow immediately in the network buffer.
// ---------------------------------------------------------------------------
static constexpr size_t PACKET_HEADER_SIZE = 64;  // bytes before data

struct alignas(64) Packet {
    uint32_t frame_id;
    uint32_t symbol_id;
    uint32_t cell_id;
    uint32_t ant_id;
    uint32_t fill[12];
    // data follows immediately after in the raw receive buffer — not stored
    // as a struct member. Use packet_data() to obtain the i16 pointer.
};
static_assert(sizeof(Packet) == PACKET_HEADER_SIZE,
              "Packet header must be exactly 64 bytes");

inline const int16_t* packet_data(const void* raw_buf) {
    return reinterpret_cast<const int16_t*>(
        static_cast<const uint8_t*>(raw_buf) + PACKET_HEADER_SIZE);
}

inline const Packet* packet_hdr(const void* raw_buf) {
    return reinterpret_cast<const Packet*>(raw_buf);
}

// ---------------------------------------------------------------------------
// Config: parameters derived from the tddconfig JSON.
// Only the fields actually used by the four kernels are stored here.
// Parsing is a simple hand-written JSON walk that matches what Rust's
// Config::new() does (same key names, same defaults).
// ---------------------------------------------------------------------------
struct Config {
    // --- Antenna / UE counts ---
    size_t bs_ant_num         = 4;
    size_t ue_ant_num         = 4;
    size_t num_spatial_streams = 4;   // defaults to ue_ant_num if absent

    // --- OFDM parameters ---
    size_t ofdm_ca_num        = 256;  // fft_size
    size_t ofdm_data_num      = 192;
    size_t ofdm_rx_zero_prefix_bs = 0;
    size_t ofdm_data_start    = 32;   // (ofdm_ca_num - ofdm_data_num) / 2
    size_t cp_len             = 0;
    size_t ofdm_tx_zero_prefix = 0;
    size_t ofdm_tx_zero_postfix = 0;

    // --- Frame schedule ---
    std::string frame_schedule = "PUUUUUUUUUUUUU";

    // --- Task block sizes ---
    size_t beam_block_size    = 1;
    size_t demul_block_size   = 4;
    size_t beam_events_per_symbol = 192;   // = ceil(ofdm_data_num / beam_block_size)
    size_t demul_events_per_symbol = 48;   // = ceil(ofdm_data_num / demul_block_size)

    // --- Modulation ---
    size_t ul_mod_order_bits  = 4;   // 16-QAM default

    // --- Pilot / freq-orth ---
    bool   freq_orth_pilot    = false;
    size_t pilot_sc_group_size = 8;   // TransposeBlockSize default

    // --- Network ---
    int    bs_server_port     = 8000;
    std::string bs_server_addr = "127.0.0.1";

    // --- Derived: pilots_sgn for PartialTranspose ---
    // pilots_sgn[sc] = conj(zc_seq[sc]) / |zc_seq[sc]|^2
    // We generate it the same way mimolib does (Zadoff-Chu + cyclic shift).
    // For the benchmark binary we carry it as a flat complex<float> array.
    std::vector<std::complex<float>> pilots_sgn;

    // --- Scheduling buffer (ScheduledUeList) ---
    std::vector<size_t> schedule_buffer_index;
    size_t sched_rows = 0;
    size_t sched_cols = 0;

    // --- Frame-stats derived fields ---
    std::vector<size_t> pilot_symbols;
    std::vector<size_t> ul_symbols;

    // -----------------------------------------------------------------------
    // Helper: return ScheduledUeList for (frame_id, sc_id)
    // Mirrors Config::ScheduledUeList in config.rs
    // -----------------------------------------------------------------------
    std::vector<size_t> ScheduledUeList(size_t frame_id, size_t sc_id) const {
        size_t gp = frame_id % sched_rows;
        std::vector<size_t> result(num_spatial_streams);
        for (size_t i = 0; i < num_spatial_streams; ++i) {
            result[i] = schedule_buffer_index[
                gp * sched_cols + num_spatial_streams * sc_id + i];
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    size_t GetBeamScId(size_t sc_id) const {
        if (freq_orth_pilot) {
            return sc_id - (sc_id % pilot_sc_group_size);
        }
        return sc_id;
    }

    size_t GetDataOffset(size_t frame_slot, size_t symbol_id) const {
        // frame_slot * num_ul_syms + GetUlSymbolIdx(symbol_id)
        size_t ul_sym_idx = GetUlSymbolIdx(symbol_id);
        return frame_slot * ul_symbols.size() + ul_sym_idx;
    }

    size_t GetTotalSymbolIdxUl(size_t frame_id, size_t symbol_idx_ul) const {
        size_t frame_slot = frame_id % FRAME_WND;
        return frame_slot * ul_symbols.size() + symbol_idx_ul;
    }

    size_t GetUlSymbolIdx(size_t symbol_id) const {
        auto it = std::lower_bound(ul_symbols.begin(), ul_symbols.end(), symbol_id);
        if (it == ul_symbols.end() || *it != symbol_id) return SIZE_MAX;
        return static_cast<size_t>(it - ul_symbols.begin());
    }

    size_t GetPilotSymbolIdx(size_t symbol_id) const {
        auto it = std::lower_bound(pilot_symbols.begin(), pilot_symbols.end(), symbol_id);
        if (it == pilot_symbols.end() || *it != symbol_id) return SIZE_MAX;
        return static_cast<size_t>(it - pilot_symbols.begin());
    }

    size_t NumUlSyms()    const { return ul_symbols.size(); }
    size_t NumPilotSyms() const { return pilot_symbols.size(); }

    // -----------------------------------------------------------------------
    // Derived constants used throughout
    // -----------------------------------------------------------------------
    static constexpr size_t FRAME_WND = 2;       // mimolib FrameWnd
    static constexpr size_t TRANSPOSE_BLOCK_SIZE = 8;
    static constexpr size_t SCS_PER_CACHELINE    = 8;  // 64/(2*4)
    static constexpr size_t MAX_MOD_TYPE         = 8;
    static constexpr bool   UPLINK_HARD_DEMOD    = false;
    static constexpr bool   SIMD_GATHER          = true;

    // Packet length: PACKET_HEADER_SIZE + iq_bytes (4 bytes/sample, 16-bit I+Q)
    size_t packet_length() const {
        size_t samps = ofdm_tx_zero_prefix + ofdm_ca_num + cp_len + ofdm_tx_zero_postfix;
        return PACKET_HEADER_SIZE + 4 * samps;  // 4 = sizeof(int16_t) * 2 (I+Q)
    }

    size_t schedule_length() const { return frame_schedule.size(); }
    size_t packets_per_frame() const { return schedule_length() * bs_ant_num; }

    // -----------------------------------------------------------------------
    // Build frame stats from frame_schedule string
    // -----------------------------------------------------------------------
    void build_frame_stats() {
        pilot_symbols.clear();
        ul_symbols.clear();
        for (size_t i = 0; i < frame_schedule.size(); ++i) {
            char c = frame_schedule[i];
            if (c == 'P') pilot_symbols.push_back(i);
            if (c == 'U') ul_symbols.push_back(i);
        }
    }

    // -----------------------------------------------------------------------
    // Build schedule buffer (ScheduleInit equivalent)
    // -----------------------------------------------------------------------
    void schedule_init() {
        size_t num_groups = (num_spatial_streams == ue_ant_num) ? 1 : ue_ant_num;
        sched_rows = num_groups;
        sched_cols = ofdm_data_num * num_spatial_streams;
        schedule_buffer_index.assign(sched_rows * sched_cols, 0);
        for (size_t gp = 0; gp < num_groups; ++gp) {
            for (size_t sc = 0; sc < ofdm_data_num; ++sc) {
                for (size_t ue = gp; ue < gp + num_spatial_streams; ++ue) {
                    size_t cur_ue = ue % ue_ant_num;
                    size_t offset = gp * sched_cols + (ue - gp) + num_spatial_streams * sc;
                    schedule_buffer_index[offset] = cur_ue;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Generate pilots_sgn (Zadoff-Chu conjugate sign sequence)
    // Mirrors comms_lib get_sequence + DoubleToCFloat + seq_cyclic_shift + pilots_sgn
    // -----------------------------------------------------------------------
    void gen_pilots() {
        // Zadoff-Chu root-1 sequence of length N = ofdm_data_num
        // zc[k] = exp(-j * pi * k * (k+1) / N)
        const double PI = 3.14159265358979323846;
        size_t N = ofdm_data_num;
        std::vector<std::complex<double>> zc(N);
        for (size_t k = 0; k < N; ++k) {
            double phase = -PI * k * (k + 1) / N;
            zc[k] = std::complex<double>(std::cos(phase), std::sin(phase));
        }
        // Cyclic shift by pi/4
        double shift = PI / 4.0;
        pilots_sgn.resize(N);
        for (size_t i = 0; i < N; ++i) {
            // common_pilot[i] = zc[i] * exp(j*shift)
            std::complex<double> cp = zc[i] * std::complex<double>(std::cos(shift), std::sin(shift));
            float re = static_cast<float>(cp.real());
            float im = static_cast<float>(cp.imag());
            float norm_sq = re * re + im * im;
            if (norm_sq > 0.0f) {
                pilots_sgn[i] = std::complex<float>(re / norm_sq, im / norm_sq);
            } else {
                pilots_sgn[i] = std::complex<float>(0.0f, 0.0f);
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Minimal JSON parser: extracts top-level scalar values and nested ul_mcs.
// We only need a handful of fields so a full parser is not needed.
// ---------------------------------------------------------------------------
inline bool json_get_string(const std::string& json, const std::string& key,
                            std::string& out) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return false;
    pos = json.find(':', pos + search.size());
    if (pos == std::string::npos) return false;
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) return false;
    size_t end = json.find('"', pos + 1);
    if (end == std::string::npos) return false;
    out = json.substr(pos + 1, end - pos - 1);
    return true;
}

inline bool json_get_long(const std::string& json, const std::string& key,
                          long long& out) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return false;
    pos = json.find(':', pos + search.size());
    if (pos == std::string::npos) return false;
    // skip whitespace
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r'))
        ++pos;
    if (pos >= json.size()) return false;
    char* end_ptr = nullptr;
    long long v = std::strtoll(json.c_str() + pos, &end_ptr, 10);
    if (end_ptr == json.c_str() + pos) return false;
    out = v;
    return true;
}

inline bool json_get_bool(const std::string& json, const std::string& key,
                           bool& out) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return false;
    pos = json.find(':', pos + search.size());
    if (pos == std::string::npos) return false;
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
                                  json[pos] == '\n' || json[pos] == '\r'))
        ++pos;
    if (json.compare(pos, 4, "true") == 0)  { out = true;  return true; }
    if (json.compare(pos, 5, "false") == 0) { out = false; return true; }
    return false;
}

// Parse frame_schedule array: returns first element
inline bool json_get_frame_schedule(const std::string& json, std::string& out) {
    size_t pos = json.find("\"frame_schedule\"");
    if (pos == std::string::npos) return false;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return false;
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) return false;
    size_t end = json.find('"', pos + 1);
    if (end == std::string::npos) return false;
    out = json.substr(pos + 1, end - pos - 1);
    return true;
}

// Map modulation string to mod_order_bits (matches mimolib modulation.rs)
inline size_t mod_str_to_bits(const std::string& mod) {
    if (mod == "QPSK")    return 2;
    if (mod == "16QAM")   return 4;
    if (mod == "64QAM")   return 6;
    if (mod == "256QAM")  return 8;
    return 4;  // default 16QAM
}

// MCS index to mod_order_bits (simplified — covers typical 5G NR table)
// Rows 0-28 as per TS 38.214 table 5.1.3.1-1
inline size_t mcs_to_mod_bits(size_t mcs_index) {
    // QPSK: 0-9, 16QAM: 10-16, 64QAM: 17-27, 256QAM: 28
    if (mcs_index <= 9)  return 2;
    if (mcs_index <= 16) return 4;
    if (mcs_index <= 27) return 6;
    return 8;
}

inline Config load_config(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("Cannot open config: " + path);
    std::string json((std::istreambuf_iterator<char>(f)),
                     std::istreambuf_iterator<char>());

    Config cfg;

    // --- Antenna counts ---
    long long v = 0;
    if (json_get_long(json, "bs_radio_num", v)) cfg.bs_ant_num = static_cast<size_t>(v);
    if (json_get_long(json, "ue_radio_num", v)) cfg.ue_ant_num = static_cast<size_t>(v);
    cfg.num_spatial_streams = cfg.ue_ant_num;
    if (json_get_long(json, "num_spatial_streams", v))
        cfg.num_spatial_streams = static_cast<size_t>(v);

    // --- OFDM ---
    if (json_get_long(json, "fft_size",             v)) cfg.ofdm_ca_num = static_cast<size_t>(v);
    if (json_get_long(json, "ofdm_data_num",        v)) cfg.ofdm_data_num = static_cast<size_t>(v);
    if (json_get_long(json, "ofdm_rx_zero_prefix_bs", v))
        cfg.ofdm_rx_zero_prefix_bs = static_cast<size_t>(v);
    if (json_get_long(json, "cp_size",              v)) cfg.cp_len = static_cast<size_t>(v);
    if (json_get_long(json, "ofdm_tx_zero_prefix",  v))
        cfg.ofdm_tx_zero_prefix = static_cast<size_t>(v);
    if (json_get_long(json, "ofdm_tx_zero_postfix", v))
        cfg.ofdm_tx_zero_postfix = static_cast<size_t>(v);
    cfg.ofdm_data_start = (cfg.ofdm_ca_num - cfg.ofdm_data_num) / 2;

    // --- Frame schedule ---
    json_get_frame_schedule(json, cfg.frame_schedule);

    // --- Task block sizes ---
    if (json_get_long(json, "beam_block_size",  v)) cfg.beam_block_size  = static_cast<size_t>(v);
    if (json_get_long(json, "demul_block_size", v)) cfg.demul_block_size = static_cast<size_t>(v);
    cfg.beam_events_per_symbol  = 1 + (cfg.ofdm_data_num - 1) / cfg.beam_block_size;
    cfg.demul_events_per_symbol = 1 + (cfg.ofdm_data_num - 1) / cfg.demul_block_size;

    // --- Pilot / freq-orth ---
    bool b = false;
    if (json_get_bool(json, "freq_orthogonal_pilot", b)) cfg.freq_orth_pilot = b;
    if (json_get_long(json, "pilot_sc_group_size", v))
        cfg.pilot_sc_group_size = static_cast<size_t>(v);
    if (cfg.freq_orth_pilot && cfg.beam_block_size == 1)
        cfg.beam_block_size = cfg.pilot_sc_group_size;

    // --- Modulation ---
    std::string mod_str = "16QAM";
    // Check ul_mcs.mcs_index first, fall back to modulation string
    size_t mcs_idx_pos = json.find("\"mcs_index\"");
    if (mcs_idx_pos != std::string::npos) {
        long long idx = 0;
        // Find the number after "mcs_index":
        size_t colon = json.find(':', mcs_idx_pos);
        if (colon != std::string::npos) {
            size_t num_start = colon + 1;
            while (num_start < json.size() && (json[num_start] == ' ' ||
                   json[num_start] == '\t' || json[num_start] == '\n')) ++num_start;
            if (num_start < json.size() && std::isdigit(json[num_start])) {
                char* ep = nullptr;
                idx = std::strtoll(json.c_str() + num_start, &ep, 10);
                cfg.ul_mod_order_bits = mcs_to_mod_bits(static_cast<size_t>(idx));
            }
        }
    } else {
        // Look for ul_mcs.modulation
        size_t ul_mcs_pos = json.find("\"ul_mcs\"");
        if (ul_mcs_pos != std::string::npos) {
            // Search for "modulation" within the ul_mcs block
            size_t block_end = json.find('}', ul_mcs_pos);
            std::string mcs_block = json.substr(ul_mcs_pos,
                block_end - ul_mcs_pos + 1);
            json_get_string(mcs_block, "modulation", mod_str);
        }
        cfg.ul_mod_order_bits = mod_str_to_bits(mod_str);
    }

    // --- Network ---
    if (json_get_long(json,   "bs_server_port", v)) cfg.bs_server_port = static_cast<int>(v);
    json_get_string(json, "bs_server_addr", cfg.bs_server_addr);

    // --- Derived ---
    cfg.build_frame_stats();
    cfg.schedule_init();
    cfg.gen_pilots();

    return cfg;
}

// ---------------------------------------------------------------------------
// Symbol type enum (matches mimolib SymbolType)
// ---------------------------------------------------------------------------
enum class SymbolType : uint32_t {
    kBeacon  = 0,
    kControl = 1,
    kUL      = 2,
    kDL      = 3,
    kPilot   = 4,
    kCalDL   = 5,
    kCalUL   = 6,
    kGuard   = 7,
    kUnknown = 8,
};

inline SymbolType get_symbol_type(const Config& cfg, size_t symbol_id) {
    if (symbol_id >= cfg.frame_schedule.size()) return SymbolType::kUnknown;
    char c = cfg.frame_schedule[symbol_id];
    switch (c) {
        case 'B': return SymbolType::kBeacon;
        case 'C': return SymbolType::kControl;
        case 'U': return SymbolType::kUL;
        case 'D': return SymbolType::kDL;
        case 'P': return SymbolType::kPilot;
        case 'L': return SymbolType::kCalUL;
        case 'G': return SymbolType::kGuard;
        default:  return SymbolType::kUnknown;
    }
}

// ---------------------------------------------------------------------------
// 64-byte aligned heap buffer — mirrors Rust's AlignedVec<T> from structures.rs.
// std::vector only guarantees alignof(T) alignment; SIMD kernels (PartialTranspose,
// SimdConvertShortToFloat, etc.) require 64-byte aligned start addresses.
// ---------------------------------------------------------------------------
template <typename T>
struct AlignedVec {
    T*     ptr = nullptr;
    size_t n   = 0;

    AlignedVec() = default;
    ~AlignedVec() { std::free(ptr); }
    AlignedVec(const AlignedVec&) = delete;
    AlignedVec& operator=(const AlignedVec&) = delete;

    void assign(size_t count, T val) {
        std::free(ptr);
        n = count;
        size_t bytes = ((count * sizeof(T) + 63) / 64) * 64;
        posix_memalign(reinterpret_cast<void**>(&ptr), 64, bytes);
        std::fill(ptr, ptr + count, val);
    }

    T*       data()       { return ptr; }
    const T* data() const { return ptr; }
    size_t   size() const { return n;   }
    T& operator[](size_t i)       { return ptr[i]; }
    const T& operator[](size_t i) const { return ptr[i]; }
    T* begin() { return ptr; }
    T* end()   { return ptr + n; }
};

// ---------------------------------------------------------------------------
// Working buffers for one concurrent slot.
//
// Buffer layout mirrors Rust's buffer_lib.rs structures exactly:
//
//   FftBuffer:     Table<Complex32>[FrameWnd * ul_syms][bs_ant_num * ofdm_data_num]
//   CsiBuffer:     Grid<Complex32>[FrameWnd][ue_ant_num][bs_ant_num * ofdm_data_num]
//   UlBeamMatrix:  Grid<Complex32>[FrameWnd][ofdm_data_num][bs_ant_num * ue_ant_num]
//   DemodBuffer:   Cube<int8_t>[FrameWnd][ul_data_syms][num_spatial_streams][MaxModType * ofdm_data_num]
// ---------------------------------------------------------------------------
struct alignas(64) SlotBuffers {
    // fft_buf[frame_slot * ul_syms + sym_idx_ul][ant * ofdm_data_num .. ]
    AlignedVec<std::complex<float>> fft_buf;
    size_t fft_row_size;   // bs_ant_num * ofdm_data_num
    size_t fft_rows;       // FrameWnd * ul_syms

    // csi_buf[frame_slot][ue_idx][sc * bs_ant_num .. ] (partial-transposed layout)
    AlignedVec<std::complex<float>> csi_buf;
    size_t csi_row_size;   // bs_ant_num * ofdm_data_num

    // ul_beam_buf[frame_slot][sc_id][ue_idx * bs_ant_num .. ]
    AlignedVec<std::complex<float>> ul_beam_buf;
    size_t beam_row_size;  // bs_ant_num * ue_ant_num

    // demod_buf[frame_slot][data_sym_idx_ul][ss_id][mod_bits * ofdm_data_num]
    AlignedVec<int8_t> demod_buf;
    size_t demod_entry_size;  // MaxModType * ofdm_data_num

    // CSI-gather scratch (per slot, used only by beam tasks sequentially)
    AlignedVec<std::complex<float>> csi_gather;  // MaxAntennas * MaxUEs

    void init(const Config& cfg) {
        static constexpr size_t FrameWnd = Config::FRAME_WND;
        static constexpr size_t MaxMod   = Config::MAX_MOD_TYPE;
        static constexpr size_t MaxAnts  = 64;
        static constexpr size_t MaxUEs   = 64;

        size_t ul_syms       = cfg.NumUlSyms();
        size_t ul_data_syms  = ul_syms;  // client_ul_pilot_symbols = 0 in 4x4 config

        fft_row_size = cfg.bs_ant_num * cfg.ofdm_data_num;
        fft_rows     = FrameWnd * ul_syms;
        fft_buf.assign(fft_rows * fft_row_size, {0.0f, 0.0f});

        csi_row_size = cfg.bs_ant_num * cfg.ofdm_data_num;
        csi_buf.assign(FrameWnd * cfg.ue_ant_num * csi_row_size, {0.0f, 0.0f});

        beam_row_size = cfg.bs_ant_num * cfg.ue_ant_num;
        ul_beam_buf.assign(FrameWnd * cfg.ofdm_data_num * beam_row_size, {0.0f, 0.0f});

        demod_entry_size = MaxMod * cfg.ofdm_data_num;
        demod_buf.assign(FrameWnd * ul_data_syms * cfg.num_spatial_streams * demod_entry_size, 0);

        csi_gather.assign(MaxAnts * MaxUEs, {0.0f, 0.0f});
    }

    // --- Accessors matching Rust buffer_lib layout ---

    // FftBuffer::get(total_symbol_idx_ul)
    std::complex<float>* fft_row(size_t total_sym_idx_ul) {
        return fft_buf.data() + total_sym_idx_ul * fft_row_size;
    }
    const std::complex<float>* fft_row_const(size_t total_sym_idx_ul) const {
        return fft_buf.data() + total_sym_idx_ul * fft_row_size;
    }

    // CsiBuffer::get(frame_slot, ue_idx)
    std::complex<float>* csi_cell(size_t frame_slot, size_t ue_idx,
                                   size_t ue_ant_num) {
        size_t idx = (frame_slot * ue_ant_num + ue_idx) * csi_row_size;
        return csi_buf.data() + idx;
    }

    // UlBeamMatrix::get(frame_slot, sc_id)
    // Grid layout: (frame_slot * ofdm_data_num + sc_id) * beam_row_size
    std::complex<float>* beam_cell(size_t frame_slot, size_t sc_id,
                                    size_t ofdm_data_num) {
        size_t idx = (frame_slot * ofdm_data_num + sc_id) * beam_row_size;
        return ul_beam_buf.data() + idx;
    }
    const std::complex<float>* beam_cell_const(size_t frame_slot, size_t sc_id,
                                                size_t ofdm_data_num) const {
        size_t idx = (frame_slot * ofdm_data_num + sc_id) * beam_row_size;
        return ul_beam_buf.data() + idx;
    }

    // DemodBuffer::get(frame_slot, data_sym_idx_ul, ss_id)
    // Cube layout: (ss_id * FrameWnd * ul_data_syms + frame_slot * ul_data_syms + data_sym_idx) * entry_size
    int8_t* demod_cell(size_t frame_slot, size_t data_sym_idx_ul,
                        size_t ss_id, size_t ul_data_syms) {
        size_t idx = (ss_id * Config::FRAME_WND * ul_data_syms
                      + frame_slot * ul_data_syms
                      + data_sym_idx_ul) * demod_entry_size;
        return demod_buf.data() + idx;
    }
};

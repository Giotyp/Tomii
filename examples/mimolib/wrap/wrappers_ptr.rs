use libloading::{Library, Symbol};
use once_cell::sync::Lazy;
use synstream_types::*;

static DYN_LIB: Lazy<Library> = Lazy::new(|| {
    let path = std::env::var("PLUGIN_LIB").expect("PLUGIN_LIB must be set to your .so/.dll");
    unsafe { Library::new(path).expect("Failed to open plugin library") }
});

pub fn init_wrappers() {
    Lazy::force(&DYN_LIB);
}

macro_rules! cache_sym {
    ($vis:vis static $sym:ident : $typ:ty = $name:expr;) => {
        $vis static $sym: Lazy<$typ> = Lazy::new(|| {
            let lib = &*DYN_LIB;
            let sym: Symbol<$typ> =
                unsafe { lib.get($name) }
                    .unwrap_or_else(|e| panic!("couldn't load symbol {:?}: {}", $name, e));
            *sym
        });
    };
}

cache_sym! {
    pub(crate) static GET_FRAME_ID_SYM: fn(&CmTypes) -> usize
        = b"get_frame_id\0";
}
pub fn get_frame_id_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    CmTypes::Usize(GET_FRAME_ID_SYM(packet))
}

cache_sym! {
    pub(crate) static CREATE_CONFIG_SYM: fn(String) -> CmTypes
        = b"create_config\0";
}
pub fn create_config_wrap(args: &[CmTypes]) -> CmTypes {
    let config_file: String = match &args[0] {
        CmTypes::String(x) => x.to_string(),
        _ => panic!("Invalid argument type"),
    };

    CREATE_CONFIG_SYM(config_file)
}

cache_sym! {
    pub(crate) static CREATE_FRAMESTATS_SYM: fn(&CmTypes) -> CmTypes
        = b"create_framestats\0";
}
pub fn create_framestats_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_FRAMESTATS_SYM(config)
}

cache_sym! {
    pub(crate) static CREATE_PACKET_CONFIG_SYM: fn(&CmTypes) -> CmTypes
        = b"create_packet_config\0";
}
pub fn create_packet_config_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_PACKET_CONFIG_SYM(config)
}

cache_sym! {
    pub(crate) static GET_PACKETS_PER_FRAME_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"get_packets_per_frame\0";
}
pub fn get_packets_per_frame_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let packet_config = &args[1];

    CmTypes::Usize(GET_PACKETS_PER_FRAME_SYM(config, packet_config))
}

cache_sym! {
    pub(crate) static GET_PILOT_SYMBOLS_SYM: fn(&CmTypes) -> usize
        = b"get_pilot_symbols\0";
}
pub fn get_pilot_symbols_wrap(args: &[CmTypes]) -> CmTypes {
    let framestats = &args[0];

    CmTypes::Usize(GET_PILOT_SYMBOLS_SYM(framestats))
}

cache_sym! {
    pub(crate) static GET_UPLINK_SYMBOLS_SYM: fn(&CmTypes) -> usize
        = b"get_uplink_symbols\0";
}
pub fn get_uplink_symbols_wrap(args: &[CmTypes]) -> CmTypes {
    let framestats = &args[0];

    CmTypes::Usize(GET_UPLINK_SYMBOLS_SYM(framestats))
}

cache_sym! {
    pub(crate) static TOTAL_PILOT_SYMBOLS_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"total_pilot_symbols\0";
}
pub fn total_pilot_symbols_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CmTypes::Usize(TOTAL_PILOT_SYMBOLS_SYM(config, framestats))
}

cache_sym! {
    pub(crate) static TOTAL_UPLINK_SYMBOLS_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"total_uplink_symbols\0";
}
pub fn total_uplink_symbols_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CmTypes::Usize(TOTAL_UPLINK_SYMBOLS_SYM(config, framestats))
}

cache_sym! {
    pub(crate) static GET_ANTENNAS_SYM: fn(&CmTypes) -> usize
        = b"get_antennas\0";
}
pub fn get_antennas_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CmTypes::Usize(GET_ANTENNAS_SYM(config))
}

cache_sym! {
    pub(crate) static GET_PACKET_LENGTH_SYM: fn(&CmTypes) -> usize
        = b"get_packet_length\0";
}
pub fn get_packet_length_wrap(args: &[CmTypes]) -> CmTypes {
    let packet_config = &args[0];

    CmTypes::Usize(GET_PACKET_LENGTH_SYM(packet_config))
}

cache_sym! {
    pub(crate) static GET_SERVER_ADDRESS_SYM: fn(&CmTypes) -> CmTypes
        = b"get_server_address\0";
}
pub fn get_server_address_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    GET_SERVER_ADDRESS_SYM(config)
}

cache_sym! {
    pub(crate) static GET_BASE_PORT_SYM: fn(&CmTypes) -> CmTypes
        = b"get_base_port\0";
}
pub fn get_base_port_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    GET_BASE_PORT_SYM(config)
}

cache_sym! {
    pub(crate) static PROCESS_PACKET_SYM: fn(&[u8]) -> CmTypes
        = b"process_packet\0";
}
pub fn process_packet_wrap(args: &[CmTypes]) -> CmTypes {
    // Extract bytes via with_bytes — handles both CmTypes::Bytes (zero-copy, fast path)
    // and CmTypes::Any(Vec<u8>) (backward compatible, slower path)
    let result = args[0].with_bytes(|bytes: &[u8]| PROCESS_PACKET_SYM(bytes));

    result.expect("process_packet expects byte data (CmTypes::Bytes or CmTypes::Any(Vec<u8>))")
}

cache_sym! {
    pub(crate) static INIT_UDP_SOCKET_SYM: fn(&CmTypes, usize) -> CmTypes
        = b"init_udp_socket\0";
}
pub fn init_udp_socket_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let index = match args[1] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for index"),
    };

    INIT_UDP_SOCKET_SYM(config, index)
}

cache_sym! {
    pub(crate) static RECEIVE_PACKET_SYM: fn(CmTypes, usize) -> CmTypes
        = b"receive_packet\0";
}
pub fn receive_packet_wrap(args: &[CmTypes]) -> CmTypes {
    let udp_socket = args[0].clone();
    let packet_length = match args[1] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for packet length"),
    };

    RECEIVE_PACKET_SYM(udp_socket, packet_length)
}

cache_sym! {
    pub(crate) static CREATE_FFT_STRUCT_SYM: fn(&CmTypes) -> CmTypes
        = b"create_fft_struct\0";
}
pub fn create_fft_struct_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_FFT_STRUCT_SYM(config)
}

cache_sym! {
    pub(crate) static CREATE_BEAM_STRUCT_SYM: fn() -> CmTypes
        = b"create_beam_struct\0";
}
pub fn create_beam_struct_wrap(_args: &[CmTypes]) -> CmTypes {
    CREATE_BEAM_STRUCT_SYM()
}

cache_sym! {
    pub(crate) static CREATE_DEMUL_STRUCT_SYM: fn(&CmTypes) -> CmTypes
        = b"create_demul_struct\0";
}
pub fn create_demul_struct_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_DEMUL_STRUCT_SYM(config)
}

cache_sym! {
    pub(crate) static CREATE_DECODE_STRUCT_SYM: fn() -> CmTypes
        = b"create_decode_struct\0";
}
pub fn create_decode_struct_wrap(_args: &[CmTypes]) -> CmTypes {
    CREATE_DECODE_STRUCT_SYM()
}

cache_sym! {
    pub(crate) static CREATE_FFT_BUFFER_SYM: fn(&CmTypes, &CmTypes) -> CmTypes
        = b"create_fft_buffer\0";
}
pub fn create_fft_buffer_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CREATE_FFT_BUFFER_SYM(config, framestats)
}

cache_sym! {
    pub(crate) static CREATE_CSI_BUFFER_SYM: fn(&CmTypes) -> CmTypes
        = b"create_csi_buffer\0";
}
pub fn create_csi_buffer_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_CSI_BUFFER_SYM(config)
}

cache_sym! {
    pub(crate) static CREATE_DEMOD_BUFFERS_SYM: fn(&CmTypes, &CmTypes) -> CmTypes
        = b"create_demod_buffers\0";
}
pub fn create_demod_buffers_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CREATE_DEMOD_BUFFERS_SYM(config, framestats)
}

cache_sym! {
    pub(crate) static CREATE_DECODE_BUFFERS_SYM: fn(&CmTypes, &CmTypes) -> CmTypes
        = b"create_decode_buffers\0";
}
pub fn create_decode_buffers_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CREATE_DECODE_BUFFERS_SYM(config, framestats)
}

cache_sym! {
    pub(crate) static CREATE_UL_BEAM_MATRICES_SYM: fn(&CmTypes) -> CmTypes
        = b"create_ul_beam_matrices\0";
}
pub fn create_ul_beam_matrices_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_UL_BEAM_MATRICES_SYM(config)
}

cache_sym! {
    pub(crate) static CREATE_UL_BASE_SCS_SYM: fn(&CmTypes) -> CmTypes
        = b"create_ul_base_scs\0";
}
pub fn create_ul_base_scs_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CREATE_UL_BASE_SCS_SYM(config)
}

cache_sym! {
    pub(crate) static UL_BASE_SCS_LEN_SYM: fn(Vec<usize>) -> usize
        = b"ul_base_scs_len\0";
}
pub fn ul_base_scs_len_wrap(args: &[CmTypes]) -> CmTypes {
    match &args[0] {
        CmTypes::VecCmt(v) => CmTypes::Usize(v.len()),
        _ => panic!("ul_base_scs_len: expected VecCmt"),
    }
}

cache_sym! {
    pub(crate) static CREATE_DEMUL_BASE_SCS_SYM: fn(&CmTypes) -> Vec<usize>
        = b"create_demul_base_scs\0";
}
pub fn create_demul_base_scs_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    let res = CREATE_DEMUL_BASE_SCS_SYM(config);
    CmTypes::from_any(res)
}

cache_sym! {
    pub(crate) static GET_UL_SYMBOL_SYM: fn(&CmTypes, usize) -> usize
        = b"get_ul_symbol\0";
}
pub fn get_ul_symbol_wrap(args: &[CmTypes]) -> CmTypes {
    let framestats = &args[0];
    let index = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for index"),
    };

    CmTypes::Usize(GET_UL_SYMBOL_SYM(framestats, index))
}

cache_sym! {
    pub(crate) static TOTAL_DEMUL_TASKS_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"total_demul_tasks\0";
}
pub fn total_demul_tasks_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];

    CmTypes::Usize(TOTAL_DEMUL_TASKS_SYM(config, framestats))
}

cache_sym! {
    pub(crate) static BEAM_EVENTS_PER_SYMBOL_SYM: fn(&CmTypes) -> usize
        = b"beam_events_per_symbol\0";
}
pub fn beam_events_per_symbol_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    CmTypes::Usize(BEAM_EVENTS_PER_SYMBOL_SYM(config))
}

cache_sym! {
    pub(crate) static CREATE_CB_IDS_SYM: fn(&CmTypes) -> Vec<usize>
        = b"create_cb_ids\0";
}
pub fn create_cb_ids_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];

    let res = CREATE_CB_IDS_SYM(config);
    CmTypes::from_any(res)
}

cache_sym! {
    pub(crate) static CB_IDS_LEN_SYM: fn(Vec<usize>) -> usize
        = b"cb_ids_len\0";
}
pub fn cb_ids_len_wrap(args: &[CmTypes]) -> CmTypes {
    args[0]
        .with_any(|v: &Vec<usize>| CmTypes::Usize(v.len()))
        .expect("Failed to extract cb_ids as Vec<usize>")
}

cache_sym! {
    pub(crate) static PAIRED_CB_SYMBOL_SYM: fn(usize, usize) -> Vec<usize>
        = b"paired_cb_symbol\0";
}

pub fn paired_cb_symbol_wrap(args: &[CmTypes]) -> CmTypes {
    let total_symbols = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for total symbols"),
    };

    let cb_ids_len = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for cb_ids_len"),
    };

    let res = PAIRED_CB_SYMBOL_SYM(total_symbols, cb_ids_len);
    CmTypes::from_any(res)
}

cache_sym! {
    pub(crate) static TOTAL_DECODE_TASKS_SYM: fn(&CmTypes, usize) -> usize
        = b"total_decode_tasks\0";
}
pub fn total_decode_tasks_wrap(args: &[CmTypes]) -> CmTypes {
    let framestats = &args[0];
    let cb_ids_len = match args[1] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for cb_ids_len"),
    };

    CmTypes::Usize(TOTAL_DECODE_TASKS_SYM(framestats, cb_ids_len))
}

cache_sym! {
    pub(crate) static GET_PACKET_SLOT_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"get_packet_slot\0";
}
pub fn get_packet_slot_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    let config = &args[1];
    CmTypes::Usize(GET_PACKET_SLOT_SYM(packet, config))
}

cache_sym! {
    pub(crate) static GET_PILOT_PACKET_COUNT_SYM: fn(&CmTypes, &CmTypes) -> usize
        = b"get_pilot_packet_count\0";
}
pub fn get_pilot_packet_count_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];
    CmTypes::Usize(GET_PILOT_PACKET_COUNT_SYM(config, framestats))
}

cache_sym! {
    pub(crate) static DEMUL_EVENTS_PER_SYMBOL_SYM: fn(&CmTypes) -> usize
        = b"demul_events_per_symbol\0";
}
pub fn demul_events_per_symbol_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    CmTypes::Usize(DEMUL_EVENTS_PER_SYMBOL_SYM(config))
}

cache_sym! {
    pub(crate) static DECODE_TASKS_PER_SYMBOL_SYM: fn(usize) -> usize
        = b"decode_tasks_per_symbol\0";
}
pub fn decode_tasks_per_symbol_wrap(args: &[CmTypes]) -> CmTypes {
    let cb_ids_len = match args[0] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for cb_ids_len"),
    };
    CmTypes::Usize(DECODE_TASKS_PER_SYMBOL_SYM(cb_ids_len))
}

cache_sym! {
    pub(crate) static IS_PILOT: fn(&CmTypes, &CmTypes, usize) -> bool
        = b"is_pilot\0";
}
pub fn is_pilot_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    let framestats = &args[1];
    let index = match &args[2] {
        CmTypes::Usize(x) => *x,
        CmTypes::Ref(0) => {
            eprintln!("ERROR: $factor argument not replaced with runtime index!");
            eprintln!("args.len() = {}, args[2] = CmTypes::Ref(0)", args.len());
            panic!("$factor (Ref(0)) not replaced with actual node_index");
        }
        other => {
            eprintln!("ERROR: Invalid argument type for index");
            eprintln!("args.len() = {}", args.len());
            eprintln!("args[2] type: {:?}", other);
            panic!(
                "Invalid argument type for index: expected Usize, got {:?}",
                other
            );
        }
    };

    CmTypes::Bool(IS_PILOT(packet, framestats, index))
}

cache_sym! {
    pub(crate) static FFT_OP_SYM: fn(&CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes, usize) -> CmTypes
        = b"fft_op_ptr\0";
}
pub fn fft_op_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    let config = &args[1];
    let framestats = &args[2];
    let fft_struct = &args[3];
    let fft_buffer = &args[4];
    let index = match args[5] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for index"),
    };

    FFT_OP_SYM(packet, config, framestats, fft_struct, fft_buffer, index)
}

cache_sym! {
    pub(crate) static FFT_COMB_SYM: fn(&CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes, usize) -> CmTypes
        = b"fft_comb_ptr\0";
}
pub fn fft_comb_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    let config = &args[1];
    let framestats = &args[2];
    let fft_struct = &args[3];
    let fft_buffer = &args[4];
    let csi_buffer = &args[5];
    let index = match args[6] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for index"),
    };

    FFT_COMB_SYM(
        packet, config, framestats, fft_struct, fft_buffer, csi_buffer, index,
    )
}

cache_sym! {
    pub(crate) static CSI_OP_SYM: fn(&CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes) -> CmTypes
        = b"csi_op_ptr\0";
}
pub fn csi_op_wrap(args: &[CmTypes]) -> CmTypes {
    let packet = &args[0];
    let config = &args[1];
    let framestats = &args[2];
    let fft_struct = &args[3];
    let csi_buffer = &args[4];

    CSI_OP_SYM(packet, config, framestats, fft_struct, csi_buffer)
}

cache_sym! {
    pub(crate) static BEAM_OP_SYM: fn(&CmTypes, usize, &CmTypes, &CmTypes, &CmTypes, usize) -> usize
        = b"beam_op_ptr\0";
}
pub fn beam_op_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let ul_base_scs = &args[1];
    let beam_struct = &args[2];
    let csi_buffer = &args[3];
    let ul_beam_matrices = &args[4];
    let csi_res = match args[5] {
        CmTypes::Usize(frame_id) => frame_id,
        _ => panic!("Invalid argument type for csi_res"),
    };
    let node_index = match args[6] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for node index"),
    };

    let base_sc_id = match ul_base_scs {
        CmTypes::VecCmt(v) => match v[node_index % v.len()] {
            CmTypes::Usize(x) => x,
            _ => panic!("ul_base_scs: expected Usize elements"),
        },
        _ => panic!("ul_base_scs: expected VecCmt"),
    };

    CmTypes::Usize(BEAM_OP_SYM(
        config,
        base_sc_id,
        beam_struct,
        csi_buffer,
        ul_beam_matrices,
        csi_res,
    ))
}

cache_sym! {
    pub(crate) static DEMUL_OP_SYM: fn(&CmTypes, &CmTypes, &[usize], &CmTypes, &CmTypes, &CmTypes, &CmTypes, usize, usize, usize) -> CmTypes
        = b"demul_op_ptr\0";
}
pub fn demul_op_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];
    let demul_struct = &args[3];
    let fft_buffer = &args[4];
    let demod_buffers = &args[5];
    let ul_beam_matrices = &args[6];
    let fft_res = match &args[7] {
        CmTypes::Usize(frame_id) => *frame_id,
        other => panic!(
            "Invalid argument type for fft_res. Expected CmTypes::Usize, got: {:?}",
            other
        ),
    };
    let symbol_id = match args[8] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for symbol id"),
    };
    let node_index = match args[9] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for node index"),
    };

    args[2]
        .with_any(|demul_base_scs: &Vec<usize>| {
            DEMUL_OP_SYM(
                config,
                framestats,
                demul_base_scs.as_slice(),
                demul_struct,
                fft_buffer,
                demod_buffers,
                ul_beam_matrices,
                fft_res,
                symbol_id,
                node_index,
            )
        })
        .expect("Failed to extract demul_base_scs as Vec<usize>")
}

cache_sym! {
    pub(crate) static DECODE_OP_SYM: fn(&CmTypes, &CmTypes, &[usize], &CmTypes, &CmTypes, &CmTypes, &CmTypes, &[usize], usize) -> CmTypes
        = b"decode_op_ptr\0";
}
pub fn decode_op_wrap(args: &[CmTypes]) -> CmTypes {
    let config = &args[0];
    let framestats = &args[1];
    let decode_struct = &args[3];
    let decode_buffers = &args[4];
    let demod_buffers = &args[5];
    let demul_res = &args[6];
    let node_index = match args[8] {
        CmTypes::Usize(x) => x,
        _ => panic!("Invalid argument type for node index"),
    };

    args[2]
        .with_any(|cb_ids: &Vec<usize>| {
            args[7]
                .with_any(|pair_cb_symbol: &Vec<usize>| {
                    DECODE_OP_SYM(
                        config,
                        framestats,
                        cb_ids.as_slice(),
                        decode_struct,
                        decode_buffers,
                        demod_buffers,
                        demul_res,
                        pair_cb_symbol.as_slice(),
                        node_index,
                    )
                })
                .expect("Failed to extract paired_cb_symbol as Vec<usize>")
        })
        .expect("Failed to extract cb_ids as Vec<usize>")
}

cache_sym! {
    pub(crate) static WRITE_BUFFERS_TO_FILE_SYM: fn(String, &CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes, &CmTypes)
        = b"write_buffers_to_file\0";
}
pub fn write_buffers_to_file_wrap(args: &[CmTypes]) -> CmTypes {
    let file_name = match &args[0] {
        CmTypes::String(x) => x.clone(),
        _ => panic!("Invalid argument type for file name"),
    };
    let fft_buffer = &args[1];
    let csi_buffers = &args[2];
    let ul_beam_matrices = &args[3];
    let demod_buffers = &args[4];
    let decoded_buffers = &args[5];
    let config = &args[6];
    let framestats = &args[7];

    WRITE_BUFFERS_TO_FILE_SYM(
        file_name.to_string(),
        fft_buffer,
        csi_buffers,
        ul_beam_matrices,
        demod_buffers,
        decoded_buffers,
        config,
        framestats,
    );
    CmTypes::None
}

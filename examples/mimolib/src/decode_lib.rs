#![allow(non_upper_case_globals)]
use crate::bindings::phy_ldpc_decoder_5gnr_bindings::*;
use crate::bindings::scrambler_bindings::*;
use crate::buffer_lib::*;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::ldpc_config::LDPCconfig;
use crate::common::structures::AlignedVec;
use crate::common::symbols::{Direction, FrameWnd};
use crate::common::utils::roundup;
use synstream_types::CmTypes;

const VarNodesSize: usize = 1024 * 1024 * std::mem::size_of::<i16>();
struct Decode {
    resp_var_nodes_align: AlignedVec<i16>,
    scrambler: *mut libc::c_void,
}

impl Decode {
    pub fn new() -> Self {
        let resp_var_nodes_align: AlignedVec<i16> = AlignedVec::new(VarNodesSize, 64);
        let scrambler = unsafe { scrambler_new() };
        Self {
            resp_var_nodes_align,
            scrambler,
        }
    }
}

unsafe impl Send for Decode {}
unsafe impl Sync for Decode {}

#[no_mangle]
pub fn create_cb_ids(config: &CmTypes) -> Vec<usize> {
    config
        .with_any(|config_ref: &Config| {
            let mut cb_ids: Vec<usize> = Vec::new();
            let num_tasks = config_ref.num_spatial_streams()
                * config_ref
                    .LdpcConfig(Direction::Uplink)
                    .GetNumBlocksInSymbol();
            let mut num_blocks = num_tasks / config_ref.encode_block_size();
            let num_remainder = num_tasks % config_ref.encode_block_size();
            if num_remainder > 0 {
                num_blocks += 1;
            }
            let mut num_tags = config_ref.encode_block_size();
            let mut c_id = 0;

            for i in 0..num_blocks {
                if (i == num_blocks - 1) && num_remainder > 0 {
                    num_tags = num_remainder;
                }
                for _ in 0..num_tags {
                    cb_ids.push(c_id);
                    c_id += 1;
                }
            }
            cb_ids
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn cb_ids_len(cb_ids: Vec<usize>) -> usize {
    cb_ids.len()
}

#[no_mangle]
pub fn paired_cb_symbol(total_symbols: usize, cb_ids_len: usize) -> Vec<usize> {
    let mut paired_ids: Vec<usize> = Vec::new();
    for i in 0..total_symbols {
        for _ in 0..cb_ids_len {
            paired_ids.push(i + 1);
        }
    }
    paired_ids
}

#[no_mangle]
pub fn total_decode_tasks(framestats: &CmTypes, cb_ids_len: usize) -> usize {
    framestats
        .with_any(|framestats_ref: &FrameStats| framestats_ref.NumUlSyms() * cb_ids_len)
        .expect("Failed to access Framestats struct or wrong type")
}

#[no_mangle]
pub fn decode_tasks_per_symbol(cb_ids_len: usize) -> usize {
    cb_ids_len
}

#[no_mangle]
pub fn create_decode_struct() -> CmTypes {
    CmTypes::from_any(Decode::new())
}

#[no_mangle]
pub fn create_decode_buffers(config: &CmTypes, framestats: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let decode_buffers = DecodedBuffer::new(&config_ref, &framestats_ref);
                    CmTypes::from_any(decode_buffers)
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn decode_op(
    config: &CmTypes,
    framestats: &CmTypes,
    cb_ids: &[usize],
    decode_struct: &CmTypes,
    decode_buffer: &CmTypes,
    demod_buffers: &CmTypes,
    demul_res: &CmTypes,
    symbol_ids: &[usize],
    node_index: usize,
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    decode_struct
                        .with_any_mut(|decode_struct_mut: &mut Decode| {
                            decode_buffer
                                .with_any_mut(|decode_buffer_mut: &mut DecodedBuffer| {
                                    demod_buffers
                                        .with_any_mut(|demod_buffer_mut: &mut DemodBuffer| {
                                            demul_res
                                                .with_any(|demul_res_ref: &(usize, usize, usize)| {
                                                    let (frame_id, _, _) = demul_res_ref;
                                                    let frame_id = *frame_id as usize;
                                                    let frame_slot = frame_id % FrameWnd;

                                                    let resp_var_nodes = decode_struct_mut
                                                        .resp_var_nodes_align
                                                        .get_mut();

                                                    let ldpc_config: &LDPCconfig =
                                                        config_ref.LdpcConfig(Direction::Uplink);

                                                    let symbol_id = symbol_ids[node_index % symbol_ids.len()];
                                                    let cb_id = cb_ids[node_index % cb_ids.len()];

                                                    let cur_cb_id =
                                                        cb_id % ldpc_config.GetNumBlocksInSymbol();
                                                    let stream_id =
                                                        cb_id / ldpc_config.GetNumBlocksInSymbol();
                                                    let ue_id = config_ref
                                                        .ScheduledUeIndex(frame_id, 0, stream_id);
                                                    let num_bytes_per_cb =
                                                        config_ref.NumBytesPerCb(Direction::Uplink);

                                                    let data_symbol_idx_ul = framestats_ref
                                                        .GetUlSymbolIdx(symbol_id)
                                                        - config_ref.client_ul_pilot_symbols();

                                                    if symbol_id
                                                        >= framestats_ref.GetUlSymbol(
                                                            config_ref.client_ul_pilot_symbols(),
                                                        )
                                                    {
                                                        // Decoder setup
                                                        let num_filler_bits: i16 = 0;
                                                        let num_channel_llrs: i16 =
                                                            ldpc_config.num_cb_codew_len() as i16;

                                                        let offset = config_ref
                                                            .ModOrderBits(Direction::Uplink)
                                                            * (ldpc_config.num_cb_codew_len()
                                                                as usize
                                                                * cur_cb_id);

                                                        let demod_buf = demod_buffer_mut
                                                            .get()
                                                            .get(
                                                                frame_slot,
                                                                data_symbol_idx_ul,
                                                                stream_id,
                                                            );
                                                        let llr_buffer_ptr: *mut i8 = unsafe {
                                                            demod_buf.as_ptr().add(offset)
                                                                as *mut i8
                                                        };

                                                        let mut ldpc_decoder_request =
                                                            bblib_ldpc_decoder_5gnr_request {
                                                                Zc: ldpc_config.expansion_factor(),
                                                                baseGraph: ldpc_config.base_graph()
                                                                    as i32,
                                                                nRows: ldpc_config.num_rows()
                                                                    as i32,
                                                                varNodes: llr_buffer_ptr,
                                                                numChannelLlrs: num_channel_llrs,
                                                                numFillerBits: num_filler_bits,
                                                                maxIterations: ldpc_config
                                                                    .max_decoder_iter(),
                                                                enableEarlyTermination: ldpc_config
                                                                    .early_termination(),
                                                            };

                                                        let num_msg_bits = ldpc_config.num_cb_len()
                                                            - num_filler_bits as u32;

                                                        let decode_buf = decode_buffer_mut
                                                            .get_mut()
                                                            .get_mut(
                                                                frame_slot,
                                                                data_symbol_idx_ul,
                                                                ue_id,
                                                            );

                                                        let offset = cur_cb_id
                                                            * roundup(num_bytes_per_cb, 64);
                                                        let decode_ptr = unsafe {
                                                            decode_buf.as_mut_ptr().add(offset)
                                                                as *mut u8
                                                        };

                                                        let mut ldpc_decoder_response =
                                                            bblib_ldpc_decoder_5gnr_response {
                                                                varNodes: resp_var_nodes
                                                                    .as_mut_ptr()
                                                                    as *mut i16,
                                                                numMsgBits: num_msg_bits as i32,
                                                                compactedMessageBytes: decode_ptr,
                                                                iterationAtTermination:
                                                                    Default::default(),
                                                                parityPassedAtTermination:
                                                                    Default::default(),
                                                            };

                                                        // Call decoder
                                                        let request_ptr = &mut ldpc_decoder_request
                                                            as *mut bblib_ldpc_decoder_5gnr_request;
                                                        let response_ptr =
                                                            &mut ldpc_decoder_response as *mut bblib_ldpc_decoder_5gnr_response;

                                                        unsafe {
                                                            let res = bblib_ldpc_decoder_5gnr(request_ptr, response_ptr);
                                                            if res != 0 {
                                                                panic!("LDPC decoder failed");
                                                            }

                                                            if config_ref.scramble_enabled() {
                                                                scrambler_descramble(
                                                                    decode_struct_mut.scrambler,
                                                                    decode_ptr as *mut libc::c_void,
                                                                    num_bytes_per_cb,
                                                                );
                                                            }
                                                        }
                                                    }
                                                    // println!("Decode done for frame {}, symbol {}, ue_id {}",
                                                    //          frame_id, symbol_id, ue_id);
                                                    CmTypes::from_any((frame_id, symbol_id))
                                                })
                                                .expect(
                                                    "Failed to access BeamRes struct or wrong type",
                                                )
                                        })
                                        .expect(
                                            "Failed to access DemodBuffers struct or wrong type",
                                        )
                                })
                                .expect("Failed to access DecodedBuffer struct or wrong type")
                        })
                        .expect("Failed to access Decode struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ********* Unsafe Decode Operations *********
#[no_mangle]
pub fn decode_op_ptr(
    config: &CmTypes,
    framestats: &CmTypes,
    cb_ids: &[usize],
    decode_struct: &CmTypes,
    decode_buffer: &CmTypes,
    demod_buffers: &CmTypes,
    demul_res: &CmTypes,
    symbol_ids: &[usize],
    node_index: usize,
) -> CmTypes {
    let decode_struct_ptr = unsafe { decode_struct.as_mut_ptr::<Decode>().unwrap().0 as usize };
    let decode_buffer_ptr =
        unsafe { decode_buffer.as_mut_ptr::<DecodedBuffer>().unwrap().0 as usize };
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    demod_buffers
                        .with_any(|demod_buffer_ref: &DemodBuffer| {
                            demul_res
                                .with_any(|demul_res_ref: &(usize, usize, usize)| {
                                    let (frame_id, _, _) = demul_res_ref;
                                    let frame_id = *frame_id as usize;
                                    let frame_slot = frame_id % FrameWnd;

                                    let decode_struct_ptr = decode_struct_ptr as *mut Decode;
                                    let decode_struct_mut = unsafe { &mut *decode_struct_ptr };

                                    let decode_buffer_ptr = decode_buffer_ptr as *mut DecodedBuffer;
                                    let decode_buffer_mut = unsafe { &mut *decode_buffer_ptr };

                                    let resp_var_nodes =
                                        decode_struct_mut.resp_var_nodes_align.get_mut();

                                    let ldpc_config: &LDPCconfig =
                                        config_ref.LdpcConfig(Direction::Uplink);

                                    let symbol_id = symbol_ids[node_index % symbol_ids.len()];
                                    let cb_id = cb_ids[node_index % cb_ids.len()];

                                    let cur_cb_id = cb_id % ldpc_config.GetNumBlocksInSymbol();
                                    let stream_id = cb_id / ldpc_config.GetNumBlocksInSymbol();
                                    let ue_id = config_ref.ScheduledUeIndex(frame_id, 0, stream_id);
                                    let num_bytes_per_cb =
                                        config_ref.NumBytesPerCb(Direction::Uplink);

                                    let data_symbol_idx_ul = framestats_ref
                                        .GetUlSymbolIdx(symbol_id)
                                        - config_ref.client_ul_pilot_symbols();

                                    if symbol_id
                                        >= framestats_ref
                                            .GetUlSymbol(config_ref.client_ul_pilot_symbols())
                                    {
                                        // Decoder setup
                                        let num_filler_bits: i16 = 0;
                                        let num_channel_llrs: i16 =
                                            ldpc_config.num_cb_codew_len() as i16;

                                        let offset = config_ref.ModOrderBits(Direction::Uplink)
                                            * (ldpc_config.num_cb_codew_len() as usize * cur_cb_id);

                                        let demod_buf = demod_buffer_ref.get().get(
                                            frame_slot,
                                            data_symbol_idx_ul,
                                            stream_id,
                                        );
                                        let llr_buffer_ptr: *mut i8 =
                                            unsafe { demod_buf.as_ptr().add(offset) as *mut i8 };

                                        let mut ldpc_decoder_request =
                                            bblib_ldpc_decoder_5gnr_request {
                                                Zc: ldpc_config.expansion_factor(),
                                                baseGraph: ldpc_config.base_graph() as i32,
                                                nRows: ldpc_config.num_rows() as i32,
                                                varNodes: llr_buffer_ptr,
                                                numChannelLlrs: num_channel_llrs,
                                                numFillerBits: num_filler_bits,
                                                maxIterations: ldpc_config.max_decoder_iter(),
                                                enableEarlyTermination: ldpc_config
                                                    .early_termination(),
                                            };

                                        let num_msg_bits =
                                            ldpc_config.num_cb_len() - num_filler_bits as u32;

                                        let decode_buf = decode_buffer_mut.get_mut().get_mut(
                                            frame_slot,
                                            data_symbol_idx_ul,
                                            ue_id,
                                        );

                                        let offset = cur_cb_id * roundup(num_bytes_per_cb, 64);
                                        let decode_ptr = unsafe {
                                            decode_buf.as_mut_ptr().add(offset) as *mut u8
                                        };

                                        let mut ldpc_decoder_response =
                                            bblib_ldpc_decoder_5gnr_response {
                                                varNodes: resp_var_nodes.as_mut_ptr() as *mut i16,
                                                numMsgBits: num_msg_bits as i32,
                                                compactedMessageBytes: decode_ptr,
                                                iterationAtTermination: Default::default(),
                                                parityPassedAtTermination: Default::default(),
                                            };

                                        // Call decoder
                                        let request_ptr = &mut ldpc_decoder_request
                                            as *mut bblib_ldpc_decoder_5gnr_request;
                                        let response_ptr = &mut ldpc_decoder_response
                                            as *mut bblib_ldpc_decoder_5gnr_response;

                                        unsafe {
                                            let res =
                                                bblib_ldpc_decoder_5gnr(request_ptr, response_ptr);
                                            if res != 0 {
                                                panic!("LDPC decoder failed");
                                            }

                                            if config_ref.scramble_enabled() {
                                                scrambler_descramble(
                                                    decode_struct_mut.scrambler,
                                                    decode_ptr as *mut libc::c_void,
                                                    num_bytes_per_cb,
                                                );
                                            }
                                        }
                                    }
                                    // println!(
                                    //     "Decode done for frame {}, symbol {}, ue_id {}",
                                    //     frame_id, symbol_id, ue_id
                                    // );
                                    CmTypes::from_any((frame_id, symbol_id))
                                })
                                .expect("Failed to access BeamRes struct or wrong type")
                        })
                        .expect("Failed to access DemodBuffers struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[link(name = "scrambler")]
extern "C" {
    pub fn scrambler_new() -> *mut std::ffi::c_void;
    pub fn scrambler_free(scrambler: *mut std::ffi::c_void);

    pub fn scrambler_scramble(
        scrambler: *mut std::ffi::c_void,
        scrambled: *mut std::ffi::c_void,
        to_scramble: *const std::ffi::c_void,
        bytes_to_scramble: usize,
    );

    pub fn scrambler_descramble(
        scrambler: *mut std::ffi::c_void,
        inout_bytes: *mut std::ffi::c_void,
        bytes_to_descramble: usize,
    );
}

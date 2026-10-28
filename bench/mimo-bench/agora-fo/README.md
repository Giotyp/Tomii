# Agora-published 64x16 frame (frequency-orthogonal pilots)

`tddconfig-64x16-fo.json`: 1 pilot + 13 uplink symbols (896 packets/frame), 16 UEs on
frequency-orthogonal pilots in 16-subcarrier groups, `beam_block_size` 16 (75 ZF tasks per
frame, as in Agora's CoNEXT'20 Table 3).

Current Agora rejects this (`pilot_sc_group_size` must equal the 8-subcarrier transpose
block, so at most 8 UEs); the `conext` branch used for the paper had no such limit.
`relax-pilot-group-assert.patch` relaxes it to "a multiple of the transpose block". Apply it
to a separate Agora checkout, build `sender` and `data_generator`, then from that checkout:

    ./build/data_generator --conf_file=<path>/tddconfig-64x16-fo.json

and point the harness at it (`AGORA_DIR=<checkout>`, `CFG=<path>/tddconfig-64x16-fo.json`,
`E1_PKTS_PER_FRAME=896`). The Tomii plugin expands CSI for groups larger than a transpose
block in `csi_lib.rs::expand_csi_grouped`.

pub fn deserialize_u32_le(buf: &[u8]) -> u32 {
    assert_eq!(buf.len(), 4);
    let buf: [u8; 4] = buf.try_into().unwrap();
    u32::from_le_bytes(buf)
}

pub fn serialize_u32_le(int: u32, buf: &mut [u8]) {
    let bytes = int.to_le_bytes();
    buf[0..4].copy_from_slice(&bytes);
}

pub fn deserialize_u64_le(buf: &[u8]) -> u64 {
    assert_eq!(buf.len(), 8);
    let buf: [u8; 8] = buf.try_into().unwrap();
    u64::from_le_bytes(buf)
}

pub fn serialize_u64_le(int: u64, buf: &mut [u8]) {
    let bytes = int.to_le_bytes();
    buf[0..8].copy_from_slice(&bytes);
}

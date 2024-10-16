use crate::{Bitmap256, DoubleBitmap256};

#[test]
fn test_rank() {
    let mut bm = Bitmap256::new();
    for i in 0..=255 {
        bm.set(i, true);
        assert_eq!(bm.rank(i), i as usize + 1);
    }
    assert_eq!(bm.rank(255), 256);
    for i in 0..=127 {
        assert_eq!(bm.rank(255), 256 - i as usize);
        bm.set(i * 2, false);
    }
}

#[test]
fn test_double_bitmap() {
    let mut bm = DoubleBitmap256::new();

    let mut state = 1234;

    fn lcg_rand_step(state: &mut u32) {
        *state = (*state as u64 * 279470273u64 % 0xfffffffb) as u32;
    }

    for _i in 0..256_u32 {
        lcg_rand_step(&mut state);
        let i = state % 256;
        lcg_rand_step(&mut state);
        let val = state & 3;
        bm.set(i as u8, val as u8);
        assert_eq!(bm.get(i as u8), val as u8);
    }
}

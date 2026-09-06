use super::*;

#[test]
#[ignore = "local artifact-builder name and path admission campaign"]
fn artifact_path_admission_campaign() {
    let mut seed = 0x206a471fu64;
    for case in 0..128 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let stage = PathBuf::from("stage/\u{130}/nested/".repeat(case % 17));
        for short in [false, true] {
            name_trial(seed, case % 2 == 0, short);
            segment_trial(&stage, short);
            lexical_trial(&stage, seed, short);
        }
    }
    for case in 0..16 {
        update_trial(case);
    }
    eprintln!("artifact paths seed=0x206a471f cases=128 names=128 segments=128 lexical=128 exact=384 short=384 updates=16 published=4 rejected=12");
}

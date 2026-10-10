use skein::{
    SearchDocument, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};
use skein_qos::ProcessMemorySnapshot;
use serde_json::json;
use std::{collections::BTreeMap,num::NonZeroU64,time::Instant};

fn options() -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        max_segment_uncompressed_bytes:NonZeroU64::new(64*1024*1024).unwrap(),
        lexical_build_memory_bytes:NonZeroU64::new(8*1024*1024).unwrap(),
        rabitq_bit_width:skein_vector_projection::RaBitQBitWidth::Four,
        ..Default::default()
    }
}
fn embedding(ordinal:usize) -> Vec<f32> {
    (0..384).map(|column| {
        let mut value=(ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(column);
        value=(value^(value>>30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value=(value^(value>>27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value^=value>>31;
        (value>>40) as f32/8_388_608.0-1.0
    }).collect()
}
fn metadata() -> BTreeMap<String,String> {
    BTreeMap::from([("kind".into(),"memory".into()),("space_id".into(),"default".into())])
}
fn phase(name:&str,started:Instant) {
    let memory=ProcessMemorySnapshot::capture().expect("native process counters required");
    println!("historical_checkpoint_phase {}",json!({"phase":name,"elapsed_millis":started.elapsed().as_millis(),"resident_bytes":memory.resident_bytes,"lifetime_peak_resident_bytes":memory.peak_resident_bytes}));
}
fn main() {
    let mut args=std::env::args().skip(1);
    let root=args.next().expect("isolated fixture directory required");
    let documents:usize=args.next().expect("document count required").parse().unwrap();
    let touches:usize=args.next().expect("touch count required").parse().unwrap();
    assert!(documents>touches && touches>0);
    let seeded_documents=documents+32;
    let started=Instant::now();
    let mut content="bounded-search-mutation ".repeat(65536/24+1);
    content.truncate(65536);
    assert_eq!(content.len(),65536);
    phase("before_full_build",started);
    let mut writer=SearchOutOfCoreGenerationWriter::create(&root,options()).unwrap();
    for ordinal in 0..seeded_documents {
        writer.push(SearchDocument {id:format!("memory:{:016x}",ordinal*2),title:format!("Document {ordinal}"),content:content.clone(),embedding:Some(embedding(ordinal)),metadata:metadata()}).expect("each initial document must be admitted");
        if (ordinal+1)%32768==0 {
            println!("historical_checkpoint_progress {}",json!({"phase":"initial_push","documents":ordinal+1,"elapsed_millis":started.elapsed().as_millis()}));
        }
    }
    let initial=writer.finish().expect("initial full generation must publish");
    assert_eq!(initial.document_count,seeded_documents);
    assert_eq!(initial.vector_document_count,seeded_documents);
    println!("historical_checkpoint_initial {}",serde_json::to_string(&initial).unwrap());
    phase("after_full_build",started);
    let reader=SearchOutOfCoreReader::open(&root).expect("ordinary initial open must validate");
    assert_eq!(reader.document_count(),seeded_documents);
    phase("before_actual_checkpoint",started);
    let upserts=(0..touches).map(|ordinal| SearchProjectionRow {
        kind:SearchProjectionKind::Memory,
        external_id:format!("{:016x}",ordinal*2),title:format!("Updated document {ordinal}"),
        body:format!("{content} changed"),embedding:Some(embedding(documents+ordinal)),
        source_id:None,metadata:metadata(),
    }).collect();
    let checkpoint_started=Instant::now();
    let update=SearchOutOfCoreGenerationWriter::prepare_delta(&reader,SearchProjectionDelta {upserts,..Default::default()},options()).expect("historical whole-corpus replacement must prepare");
    phase("after_actual_prepare",started);
    let (delta,build,reads)=update.finish().expect("historical whole-corpus checkpoint must publish");
    let checkpoint_millis=checkpoint_started.elapsed().as_millis();
    assert_eq!(delta.upserted_documents,touches);
    assert_eq!(delta.deleted_documents,0);
    assert_eq!(build.document_count,seeded_documents);
    assert_eq!(build.vector_document_count,seeded_documents);
    assert_eq!(reads.hydration_segment_bytes_read,initial.document_payload_bytes);
    assert!(reads.segment_range_reads>0);
    phase("after_actual_checkpoint",started);
    drop(reader);
    let reopened=SearchOutOfCoreReader::open(&root).expect("ordinary completed generation must reopen");
    assert_eq!(reopened.generation(),build.generation);
    assert_eq!(reopened.document_count(),seeded_documents);
    let ids:Vec<_>=(0..touches).map(|ordinal|format!("memory:{:016x}",ordinal*2)).collect();
    let hydrated=reopened.hydrate_documents(&ids).expect("all changed documents must hydrate");
    assert_eq!(hydrated.documents.len(),touches);
    for (ordinal,document) in hydrated.documents.iter().enumerate() {
        assert_eq!(document.id,ids[ordinal]);
        assert_eq!(document.title,format!("Updated document {ordinal}"));
        assert_eq!(document.content,format!("{content} changed"));
        assert_eq!(document.embedding.as_ref().unwrap(),&embedding(documents+ordinal));
    }
    phase("after_reopen_and_changed_row_validation",started);
    println!("historical_checkpoint {}",json!({
        "revision":"8cbe16f8f76f3472149b971970bda7d7511da3b8","base_documents":documents,
        "logical_base_body_bytes":documents as u64*65536,"seed_documents":32,"visible_documents":seeded_documents,
        "content_bytes_per_document":65536,"embedding_dimension":384,"touches":touches,
        "initial_build":initial,"actual_checkpoint_build":build,
        "actual_checkpoint_elapsed_millis":checkpoint_millis,
        "source_reads":{"segment_bytes_read":reads.segment_bytes_read,"hydrated_documents":reads.hydrated_documents,"hydrated_bytes":reads.hydrated_bytes,"hydration_segment_bytes_read":reads.hydration_segment_bytes_read,"complete_source_payload_read":reads.hydration_segment_bytes_read==initial.document_payload_bytes,"counter_limit":"The historical visit does not increment hydrated_documents; payload bytes plus rebuilt document count establish full-source traversal."},
        "changed_rows_validated":touches,"ordinary_final_reopen":true,
        "historical_resource_limits":{"lexical_build_memory_bytes":8388608,"max_segment_uncompressed_bytes":67108864,"max_descriptor_working_bytes":268435456,"rabitq_build_memory_bytes":67108864,"rabitq_bit_width":4,"rabitq_transform_seed":0x534b_4549_4e56_5134u64,"rabitq_segment_rows":1024,"complete_operation_reservation":"not available in this historical generation writer API","project_descriptor_admission":"not available in this historical API"},
        "fixture_difference":"Same logical base and32seed rows as the candidate pre-update corpus. Historical initialization builds them together because32old append checkpoints would each rewrite the entire corpus. Persistent formats and locked dependencies are revision-specific; this measures old checkpoint bytes, not same-format latency or RSS equivalence."
    }));
}

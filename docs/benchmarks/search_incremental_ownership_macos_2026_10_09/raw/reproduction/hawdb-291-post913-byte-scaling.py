"""Require the three measured shapes to demonstrate bounded incremental bytes."""


def evaluate_byte_scaling(results):
    if len(results) != 3:
        return {'passed': False, 'checks': {'all_three_shapes_present': False}, 'ratios': {}}
    small, large, many = results
    shapes = [(81920, 10), (327680, 10), (327680, 100)]
    checks = {'all_three_shapes_present': all(
        row['document_count'] == documents and row['touches'] == touches
        and row['logical_corpus_body_bytes'] == documents * 65536
        for row, (documents, touches) in zip(results, shapes))}
    ratios = {
        'full_artifact_n4': large['full_generation_bytes'] / small['full_generation_bytes'],
        'upsert_fixed_k_n4': large['upsert_checkpoint_bytes'] / small['upsert_checkpoint_bytes'],
        'delete_fixed_k_n4': large['mutation_checkpoint_bytes'] / small['mutation_checkpoint_bytes'],
        'upsert_fixed_n_k10': many['upsert_checkpoint_bytes'] / large['upsert_checkpoint_bytes'],
        'delete_fixed_n_k10': many['mutation_checkpoint_bytes'] / large['mutation_checkpoint_bytes'],
        'vector_payload_fixed_n_k10': many['upsert_vector_payload_bytes'] / large['upsert_vector_payload_bytes'],
        'rabitq_payload_fixed_n_k10': many['upsert_rabitq_artifact_bytes'] / large['upsert_rabitq_artifact_bytes'],
    }
    checks.update({
        'full_artifact_tracks_fourfold_corpus': 3 <= ratios['full_artifact_n4'] <= 5,
        'fixed_k_bytes_grow_less_than_twofold_for_fourfold_corpus':
            0 < ratios['upsert_fixed_k_n4'] < 2 and 0 < ratios['delete_fixed_k_n4'] < 2,
        'tenfold_k_bytes_stay_within_tenfold_at_fixed_corpus':
            0 < ratios['upsert_fixed_n_k10'] <= 10 and 0 < ratios['delete_fixed_n_k10'] <= 10,
        'changed_vector_and_rabitq_payloads_grow_with_k':
            ratios['vector_payload_fixed_n_k10'] > 1 and ratios['rabitq_payload_fixed_n_k10'] > 1,
        'all_incremental_checkpoints_below_one_percent_of_full_artifact': all(
            0 < row['upsert_checkpoint_bytes'] < row['full_generation_bytes'] / 100
            and 0 < row['mutation_checkpoint_bytes'] < row['full_generation_bytes'] / 100
            for row in results),
        'no_base_document_hydration_for_incremental_checkpoints': all(
            row['source_hydrated_documents'] == row['upsert_source_hydrated_documents'] == 0
            for row in results),
    })
    return {'passed': all(checks.values()), 'checks': checks, 'ratios': ratios,
            'scope': 'Empirical gate for these fixed synthetic shapes, including manifest overhead. Not an asymptotic proof or a device-write metric.'}

// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Own canonical and spill files through safe execution-budget interruptions.

use super::*;
use crate::property_spill::{
    PersistentPropertySpillDescriptorTree, PreparedPropertySpillArtifact, PropertySpillConfig,
};

pub(crate) struct Preparation {
    pub(super) state: Option<checkpoint_writer::state::State>,
    pub(super) spill_writer: Option<PropertySpillWriter>,
    prepared_canonical: Option<PreparedCanonicalSegmentArtifact>,
    prepared_spill: Option<PreparedPropertySpillArtifact>,
    spill_output: Option<PropertySpillWriteOutput>,
    writer: CanonicalSegmentWriter,
    path: PathBuf,
    spill_path: PathBuf,
    identity: CanonicalWriteIdentity,
    spill_config: PropertySpillConfig,
    spill_tree: PersistentPropertySpillDescriptorTree,
    data_published: bool,
    finished: bool,
    // Struct fields drop in declaration order: all open files close first.
    temporary: CanonicalTemporaryData,
}

impl Preparation {
    pub(super) fn new(
        writer: &CanonicalSegmentWriter,
        path: &Path,
        generation: ManifestGeneration,
        spills: PropertySpillWriteOptions<'_>,
    ) -> Self {
        Self {
            state: None,
            spill_writer: None,
            prepared_canonical: None,
            prepared_spill: None,
            spill_output: None,
            writer: CanonicalSegmentWriter {
                config: writer.config,
                source_admits: writer.source_admits,
                work: writer.work.clone(),
            },
            path: path.to_path_buf(),
            spill_path: spills.artifact_path.to_path_buf(),
            identity: CanonicalWriteIdentity {
                generation,
                source_commit_epoch: spills.source_commit_epoch,
            },
            spill_config: spills.config,
            spill_tree: spills.descriptor_tree,
            data_published: false,
            finished: false,
            temporary: CanonicalTemporaryData {
                canonical: path.with_extension("hawdb.tmp"),
                property_spill: Some(spills.artifact_path.with_extension("hawdb.tmp")),
            },
        }
    }

    pub(crate) fn checkpoint(
        path: &Path,
        generation: ManifestGeneration,
        spills: PropertySpillWriteOptions<'_>,
    ) -> Self {
        let writer = CanonicalSegmentWriter {
            config: CanonicalSegmentConfig::default(),
            source_admits: true,
            work: None,
        };
        Self::new(&writer, path, generation, spills)
    }

    pub(crate) fn append_node(
        &mut self,
        node: &NodeRecord,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        self.initialize(work)?;
        let state = self.state.as_mut().expect("initialized writer");
        state.encode_node(node, self.spill_writer.as_mut(), work)?;
        state.append_node(node)
    }

    pub(crate) fn finish_nodes(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        self.initialize(work)?;
        self.state
            .as_mut()
            .expect("initialized writer")
            .finish_nodes()
    }

    pub(crate) fn append_relationship(
        &mut self,
        relationship: &RelRecord,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        self.initialize(work)?;
        let state = self.state.as_mut().expect("initialized writer");
        state.encode_relationship(relationship, self.spill_writer.as_mut(), work)?;
        state.append_relationship(relationship)
    }

    pub(super) fn initialize(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        work.checkpoint()?;
        if self.finished || self.prepared_canonical.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical preparation is already finishing".into(),
            ));
        }
        match &mut self.writer.work {
            Some(bound) => bound.rebind(work),
            None => self.writer.work = Some(work.clone()),
        }
        if self.state.is_none() {
            let tree = create_canonical_descriptor_tree(
                &self.path,
                self.identity.generation,
                self.identity.source_commit_epoch,
                work,
                self.writer.source_admits,
            )?;
            self.state = Some(checkpoint_writer::state::State::create(
                &self.writer,
                &self.temporary.canonical,
                self.identity,
                tree,
            )?);
        }
        if self.spill_writer.is_none() {
            let create = if self.writer.source_admits {
                PropertySpillWriter::create_checkpoint
            } else {
                PropertySpillWriter::create_with_work_context
            };
            self.spill_writer = Some(create(
                self.temporary
                    .property_spill
                    .as_ref()
                    .expect("spill temporary path"),
                self.identity.generation,
                self.identity.source_commit_epoch,
                self.spill_config,
                self.spill_tree.clone(),
                work.clone(),
            )?);
        }
        self.state
            .as_mut()
            .expect("initialized writer")
            .bind_attempt(work)?;
        self.spill_writer
            .as_mut()
            .expect("initialized spill writer")
            .bind_checkpoint_attempt(work);
        Ok(())
    }

    pub(crate) fn finish(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<(CheckpointCanonicalManifest, PropertySpillWriteOutput), CanonicalSegmentError>
    {
        work.checkpoint()?;
        if self.finished {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical preparation result already consumed".into(),
            ));
        }
        if self.prepared_canonical.is_none() {
            self.prepared_canonical = Some(
                self.state
                    .as_mut()
                    .ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical preparation not initialized".into(),
                        )
                    })?
                    .finish_with_work_context(work)?,
            );
        }
        // Completed artifacts now own their inventories. Close the writer
        // before renaming its data and refund unused accumulator capacity.
        self.state = None;
        if self.prepared_spill.is_none() {
            self.prepared_spill = Some(
                self.spill_writer
                    .as_mut()
                    .expect("initialized spill writer")
                    .finish_with_work_context(work)?,
            );
        }
        self.spill_writer = None;
        if self.spill_output.is_none() {
            self.spill_output = Some(
                self.prepared_spill
                    .as_mut()
                    .expect("completed spill")
                    .publish_with_work_context(&self.spill_path, work)?,
            );
        }
        if !self.data_published {
            let _wave = work.io_wave()?;
            durable_replace_file(&self.temporary.canonical, &self.path)?;
            self.data_published = true;
        }
        let manifest = self
            .prepared_canonical
            .as_mut()
            .expect("completed canonical")
            .publish_descriptor_tree_with_work_context(work)?;
        self.finished = true;
        Ok((manifest, self.spill_output.take().expect("published spill")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::CheckpointWorkProbe;
    use hawdb_core::RuntimeTaskContext;
    use std::sync::atomic::Ordering;

    #[test]
    fn owned_files_finish_and_publish_across_fixed_eight_unit_attempts() {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-owned-files-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&directory).unwrap();
        let ceiling = 16 * 1024 * 1024;
        let governor = checkpoint_writer::tests::governor(ceiling);
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(ceiling)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let scheduler = checkpoint_writer::tests::local();
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("payload".into(), Value::Binary(vec![0x53; 512 * 1024]))]),
        };
        let options = |name: &str| {
            PersistentPropertySpillDescriptorTree::new(
                GraphDescriptorTreePaths::new(
                    directory.join(format!("{name}-spill.pages.hawdb")),
                    directory.join(format!("{name}-spill.root.hawdb")),
                ),
                GraphDescriptorTreeBuildConfig::default(),
            )
        };
        let ordinary_path = directory.join("ordinary.hawdb");
        let ordinary_spill = directory.join("ordinary-spill.hawdb");
        let expected = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write_fallible_with_property_spills(
                &ordinary_path,
                ManifestGeneration(17),
                [Ok(node.clone())],
                std::iter::empty::<Result<RelRecord, CanonicalSegmentError>>(),
                PropertySpillWriteOptions {
                    artifact_path: &ordinary_spill,
                    source_commit_epoch: 17,
                    config: PropertySpillConfig::default(),
                    descriptor_tree: options("ordinary"),
                },
            )
            .unwrap();
        let path = directory.join("retained.hawdb");
        let spill_path = directory.join("retained-spill.hawdb");
        let writer = CanonicalSegmentWriter {
            config: CanonicalSegmentConfig::default(),
            source_admits: true,
            work: Some(work.clone()),
        };
        let mut preparation = Preparation::new(
            &writer,
            &path,
            ManifestGeneration(17),
            PropertySpillWriteOptions {
                artifact_path: &spill_path,
                source_commit_epoch: 17,
                config: PropertySpillConfig::default(),
                descriptor_tree: options("retained"),
            },
        );
        preparation.initialize(&work).unwrap();
        writer
            .append_inputs(
                preparation.state.as_mut().unwrap(),
                [Ok(Some(&node))],
                std::iter::empty::<Result<Option<&RelRecord>, CanonicalSegmentError>>(),
                preparation.spill_writer.as_mut(),
            )
            .unwrap();
        let mut result = None;
        let mut attempts = 0;
        for attempt in 1..=128 {
            attempts = attempt;
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(8, Ordering::SeqCst);
            scheduler.set_telemetry_sink(Some(probe.clone()));
            let fresh = CheckpointWorkContext::new(permit.bind_task_context(
                RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
            ))
            .with_scheduler(scheduler.clone());
            let outcome = preparation.finish(&fresh);
            probe.assert_released(&scheduler);
            assert!(probe.completed.load(Ordering::SeqCst) <= 8);
            match outcome {
                Ok(output) => {
                    result = Some(output);
                    break;
                }
                Err(
                    CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))
                    | CanonicalSegmentError::DescriptorTree(GraphDescriptorTreeError::Work(
                        CheckpointWorkError::Stopped(_),
                    ))
                    | CanonicalSegmentError::PropertySpill(PropertySpillError::Work(
                        CheckpointWorkError::Stopped(_),
                    ))
                    | CanonicalSegmentError::PropertySpill(PropertySpillError::DescriptorTree(
                        GraphDescriptorTreeError::Work(CheckpointWorkError::Stopped(_)),
                    )),
                ) => {}
                other => panic!("attempt={attempt}: {:?}", other.err()),
            }
        }
        assert!(attempts > 1);
        let (manifest, spill) =
            result.expect("fixed eight-unit attempts must complete all file publication phases");
        assert_eq!(manifest.manifest, expected.0);
        assert_eq!(spill, expected.1);
        assert!(preparation.finish(&work).is_err());
        assert_eq!(fs::read(&path).unwrap(), fs::read(&ordinary_path).unwrap());
        assert_eq!(
            fs::read(&spill_path).unwrap(),
            fs::read(&ordinary_spill).unwrap()
        );
        let (actual_tree, _) =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(&path, ManifestGeneration(17))
                .into_parts();
        let (expected_tree, _) = PersistentCanonicalSegmentDescriptorTree::for_artifact(
            &ordinary_path,
            ManifestGeneration(17),
        )
        .into_parts();
        for (actual, expected) in [
            (actual_tree.page_artifact, expected_tree.page_artifact),
            (actual_tree.root_manifest, expected_tree.root_manifest),
            (
                directory.join("retained-spill.pages.hawdb"),
                directory.join("ordinary-spill.pages.hawdb"),
            ),
            (
                directory.join("retained-spill.root.hawdb"),
                directory.join("ordinary-spill.root.hawdb"),
            ),
        ] {
            assert_eq!(fs::read(actual).unwrap(), fs::read(expected).unwrap());
        }
        drop(preparation);
        drop(manifest);
        assert_eq!(checkpoint_writer::tests::used(&task, ceiling), 0);
        drop(writer);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        fs::remove_dir_all(directory).unwrap();
    }
}

// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

const HEADER: &[u8; 8] = b"HDVIX001";

impl VectorIndexDescriptor {
    /// Encode after admitting `MAX_VECTOR_INDEX_DESCRIPTOR_BYTES` of scratch.
    /// The enclosing immutable artifact supplies integrity and source identity.
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(MAX_VECTOR_INDEX_DESCRIPTOR_BYTES);
        output.extend_from_slice(HEADER);
        write_name(&mut output, &self.name);
        match &self.target {
            VectorIndexTarget::Relational {
                table,
                column,
                column_ordinal,
            } => {
                output.push(0);
                write_name(&mut output, table);
                write_name(&mut output, column);
                output.extend_from_slice(&column_ordinal.to_le_bytes());
            }
            VectorIndexTarget::NodeProperty {
                table,
                property,
                table_id,
                property_id,
            } => {
                output.push(1);
                write_name(&mut output, table);
                write_name(&mut output, property);
                output.extend_from_slice(&table_id.0.to_le_bytes());
                output.extend_from_slice(&property_id.0.to_le_bytes());
            }
        }
        output.extend_from_slice(&self.dimension.get().to_le_bytes());
        output.push(match self.distance {
            VectorDistance::Cosine => 0,
            VectorDistance::Euclidean => 1,
        });
        match self.backend {
            VectorIndexBackend::DiskAnn {
                max_degree,
                build_beam,
                alpha_milli,
                lsg,
            } => {
                output.push(1);
                for value in [max_degree, build_beam, alpha_milli] {
                    output.extend_from_slice(&value.to_le_bytes());
                }
                output.push(u8::from(lsg.is_some()));
                if let Some(lsg) = lsg {
                    for value in [lsg.sample_size, lsg.neighbors, lsg.exponent_milli] {
                        output.extend_from_slice(&value.to_le_bytes());
                    }
                }
            }
            VectorIndexBackend::RaBitQ {
                bit_width,
                segment_rows,
                transform_seed,
            } => {
                output.push(0);
                output.push(bit_width);
                output.extend_from_slice(&segment_rows.to_le_bytes());
                output.extend_from_slice(&transform_seed.to_le_bytes());
            }
        }
        output.extend_from_slice(&self.schema_digest);
        output
    }

    /// Decode after admitting the descriptor bound. Unknown formats and trailing
    /// bytes are refused; partially understood definitions cannot serve queries.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_VECTOR_INDEX_DESCRIPTOR_BYTES {
            return Err(invalid("descriptor exceeds its encoded size bound"));
        }
        let mut input = Input(bytes);
        if &input.read::<8>()? != HEADER {
            return Err(invalid("unsupported descriptor format"));
        }
        let name = input.name(128)?;
        let kind = input.read::<1>()?[0];
        let table = input.name(1024)?;
        let field = input.name(1024)?;
        let target = match kind {
            0 => VectorIndexTarget::Relational {
                table,
                column: field,
                column_ordinal: u32::from_le_bytes(input.read()?),
            },
            1 => VectorIndexTarget::NodeProperty {
                table,
                property: field,
                table_id: TableId(u32::from_le_bytes(input.read()?)),
                property_id: PropertyId(u32::from_le_bytes(input.read()?)),
            },
            _ => return Err(invalid("unsupported target kind")),
        };
        let dimension = VectorDimension::new(u32::from_le_bytes(input.read()?))
            .ok_or_else(|| invalid("dimension must be positive"))?;
        let distance = match input.read::<1>()?[0] {
            0 => VectorDistance::Cosine,
            1 => VectorDistance::Euclidean,
            _ => return Err(invalid("unsupported index metric")),
        };
        let backend = match input.read::<1>()?[0] {
            1 => {
                let max_degree = u32::from_le_bytes(input.read()?);
                let build_beam = u32::from_le_bytes(input.read()?);
                let alpha_milli = u32::from_le_bytes(input.read()?);
                let lsg = match input.read::<1>()?[0] {
                    0 => None,
                    1 => Some(VectorIndexLsg {
                        sample_size: u32::from_le_bytes(input.read()?),
                        neighbors: u32::from_le_bytes(input.read()?),
                        exponent_milli: u32::from_le_bytes(input.read()?),
                    }),
                    _ => return Err(invalid("unsupported local scaling encoding")),
                };
                VectorIndexBackend::DiskAnn {
                    max_degree,
                    build_beam,
                    alpha_milli,
                    lsg,
                }
            }
            0 => VectorIndexBackend::RaBitQ {
                bit_width: input.read::<1>()?[0],
                segment_rows: u32::from_le_bytes(input.read()?),
                transform_seed: u64::from_le_bytes(input.read()?),
            },
            _ => return Err(invalid("unsupported index backend")),
        };
        let digest = input.read()?;
        if !input.0.is_empty() {
            return Err(invalid("trailing descriptor bytes"));
        }
        Self::new(name, target, dimension, distance, backend, digest)
    }
}

fn write_name(output: &mut Vec<u8>, name: &str) {
    let length = u16::try_from(name.len()).expect("validated descriptor name length");
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(name.as_bytes());
}

struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn read<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (value, rest) = self
            .0
            .split_at_checked(N)
            .ok_or_else(|| invalid("truncated descriptor"))?;
        self.0 = rest;
        Ok(value.try_into().expect("fixed-size descriptor field"))
    }

    fn name(&mut self, maximum: usize) -> Result<String> {
        let length = usize::from(u16::from_le_bytes(self.read()?));
        if length == 0 || length > maximum {
            return Err(invalid("descriptor name exceeds its size bound"));
        }
        let (value, rest) = self
            .0
            .split_at_checked(length)
            .ok_or_else(|| invalid("truncated descriptor name"))?;
        let value =
            std::str::from_utf8(value).map_err(|_| invalid("descriptor name is not UTF-8"))?;
        self.0 = rest;
        Ok(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(target: VectorIndexTarget) -> VectorIndexDescriptor {
        VectorIndexDescriptor::new(
            "memory.embedding".into(),
            target,
            VectorDimension::new(3).unwrap(),
            VectorDistance::Cosine,
            VectorIndexBackend::RaBitQ {
                bit_width: 4,
                segment_rows: 1024,
                transform_seed: 42,
            },
            [0x5a; 32],
        )
        .unwrap()
    }

    #[test]
    fn vector_index_binding_round_trip_and_truncation() {
        for target in [
            VectorIndexTarget::Relational {
                table: "memories".into(),
                column: "embedding".into(),
                column_ordinal: 2,
            },
            VectorIndexTarget::NodeProperty {
                table: "Memory".into(),
                property: "embedding".into(),
                table_id: TableId(7),
                property_id: PropertyId(19),
            },
        ] {
            let binding = descriptor(target);
            let encoded = binding.encode();
            assert!(encoded.len() <= MAX_VECTOR_INDEX_DESCRIPTOR_BYTES);
            assert_eq!(VectorIndexDescriptor::decode(&encoded).unwrap(), binding);
            for length in 0..encoded.len() {
                assert!(VectorIndexDescriptor::decode(&encoded[..length]).is_err());
            }
            let mut trailing = encoded.clone();
            trailing.push(0);
            assert!(VectorIndexDescriptor::decode(&trailing).is_err());
            let mut unknown = encoded;
            unknown[7] = b'2';
            assert!(VectorIndexDescriptor::decode(&unknown).is_err());
        }
    }

    #[test]
    fn rabitq_binding_seals_euclidean_metric() {
        let binding = VectorIndexDescriptor::new(
            "memory.embedding".into(),
            VectorIndexTarget::Relational {
                table: "memories".into(),
                column: "embedding".into(),
                column_ordinal: 1,
            },
            VectorDimension::new(3).unwrap(),
            VectorDistance::Euclidean,
            VectorIndexBackend::RaBitQ {
                bit_width: 4,
                segment_rows: 1024,
                transform_seed: 42,
            },
            [0x5a; 32],
        )
        .unwrap();
        assert_eq!(
            VectorIndexDescriptor::decode(&binding.encode()).unwrap(),
            binding
        );
        assert_eq!(binding.distance(), VectorDistance::Euclidean);
    }

    #[test]
    fn vector_index_binding_rejects_unsupported_build_parameters() {
        let target = VectorIndexTarget::Relational {
            table: "memories".into(),
            column: "embedding".into(),
            column_ordinal: 1,
        };
        let valid = descriptor(target.clone());
        for (metric, backend) in [
            (
                VectorDistance::Cosine,
                VectorIndexBackend::RaBitQ {
                    bit_width: 2,
                    segment_rows: 1024,
                    transform_seed: 0,
                },
            ),
            (
                VectorDistance::Cosine,
                VectorIndexBackend::RaBitQ {
                    bit_width: 4,
                    segment_rows: 0,
                    transform_seed: 0,
                },
            ),
        ] {
            assert!(VectorIndexDescriptor::new(
                "index".into(),
                target.clone(),
                valid.dimension(),
                metric,
                backend,
                [0; 32]
            )
            .is_err());
        }
        let mut oversized_name = valid.encode();
        oversized_name[8..10].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(VectorIndexDescriptor::decode(&oversized_name).is_err());
    }

    #[test]
    fn diskann_descriptor_roundtrips_metrics_lsg_and_rejects_truncation() {
        let target = VectorIndexTarget::Relational {
            table: "vectors".into(),
            column: "embedding".into(),
            column_ordinal: 1,
        };
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            for lsg in [
                None,
                Some(VectorIndexLsg {
                    sample_size: 256,
                    neighbors: 10,
                    exponent_milli: 1000,
                }),
            ] {
                let definition = VectorIndexDescriptor::new(
                    "vectors.embedding".into(),
                    target.clone(),
                    VectorDimension::new(3).unwrap(),
                    metric,
                    VectorIndexBackend::DiskAnn {
                        max_degree: 32,
                        build_beam: 128,
                        alpha_milli: 1200,
                        lsg,
                    },
                    [17; 32],
                )
                .unwrap();
                let encoded = definition.encode();
                assert_eq!(VectorIndexDescriptor::decode(&encoded).unwrap(), definition);
                for end in 0..encoded.len() {
                    assert!(VectorIndexDescriptor::decode(&encoded[..end]).is_err());
                }
            }
        }
        for backend in [
            VectorIndexBackend::DiskAnn {
                max_degree: 1,
                build_beam: 128,
                alpha_milli: 1200,
                lsg: None,
            },
            VectorIndexBackend::DiskAnn {
                max_degree: 32,
                build_beam: 16,
                alpha_milli: 1200,
                lsg: None,
            },
            VectorIndexBackend::DiskAnn {
                max_degree: 32,
                build_beam: 128,
                alpha_milli: 999,
                lsg: None,
            },
            VectorIndexBackend::DiskAnn {
                max_degree: 32,
                build_beam: 128,
                alpha_milli: 1200,
                lsg: Some(VectorIndexLsg {
                    sample_size: 16,
                    neighbors: 17,
                    exponent_milli: 1000,
                }),
            },
        ] {
            assert!(VectorIndexDescriptor::new(
                "invalid".into(),
                target.clone(),
                VectorDimension::new(3).unwrap(),
                VectorDistance::Cosine,
                backend,
                [0; 32]
            )
            .is_err());
        }
    }
}

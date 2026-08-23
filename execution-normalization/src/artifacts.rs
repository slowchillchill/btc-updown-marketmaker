use std::fs;
use std::path::{Path, PathBuf};

use contracts::Sha256Hex;
use contracts::execution_normalization::{
    NoeComponentCapabilityReceiptV1, NoePublishedContainerReferenceV1, NoeResultReferenceV1,
    NoeRunConsumptionManifestV1,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::durable::{DurableErrorV1, publish_artifact_no_replace};

pub const NOE_ARTIFACT_ENCODING_V1: &str = "serde_json_v1";
pub const NOE_TEST_EVIDENCE_CONTAINER_V1: &str = "noe_test_evidence_container_v1";
pub const NOE_RESOURCE_REPORT_CONTAINER_V1: &str = "noe_resource_report_container_v1";
pub const NOE_CONSUMER_FIXTURE_CONTAINER_V1: &str = "noe_consumer_fixture_container_v1";
pub const NOE_COMPONENT_RECEIPT_CONTAINER_V1: &str = "noe_component_receipt_container_v1";
pub const NOE_RUN_MANIFEST_CONTAINER_V1: &str = "noe_run_manifest_container_v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeTestEvidenceV1 {
    pub implementation_identity: String,
    pub commands: Vec<String>,
    pub skipped_tests: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeResourceReportV1 {
    pub implementation_identity: String,
    pub trajectory_batches: u64,
    pub elapsed_milliseconds: u64,
    pub throughput_batches_per_second_milli: u64,
    pub peak_resident_memory_bytes: u64,
    pub peak_pending_projection_batches: u64,
    pub durable_disk_bytes: u64,
    pub reopen_elapsed_microseconds: u64,
    pub durable_bytes_growth_per_batch_milli: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeConsumerFixtureV1 {
    pub result_reference: NoeResultReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ArtifactPayloadV1<T> {
    container_version: String,
    encoding: String,
    value: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ArtifactContainerV1<T> {
    payload: ArtifactPayloadV1<T>,
    boundary_sha256: Sha256Hex,
}

pub fn publish_noe_test_evidence_v1(
    path: impl AsRef<Path>,
    value: &NoeTestEvidenceV1,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    publish(path.as_ref(), NOE_TEST_EVIDENCE_CONTAINER_V1, value)
}

pub fn read_noe_test_evidence_v1(
    reference: &NoePublishedContainerReferenceV1,
) -> Result<NoeTestEvidenceV1, DurableErrorV1> {
    read(reference, NOE_TEST_EVIDENCE_CONTAINER_V1)
}

pub fn publish_noe_resource_report_v1(
    path: impl AsRef<Path>,
    value: &NoeResourceReportV1,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    publish(path.as_ref(), NOE_RESOURCE_REPORT_CONTAINER_V1, value)
}

pub fn read_noe_resource_report_v1(
    reference: &NoePublishedContainerReferenceV1,
) -> Result<NoeResourceReportV1, DurableErrorV1> {
    read(reference, NOE_RESOURCE_REPORT_CONTAINER_V1)
}

pub fn publish_noe_consumer_fixture_v1(
    path: impl AsRef<Path>,
    value: &NoeConsumerFixtureV1,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    publish(path.as_ref(), NOE_CONSUMER_FIXTURE_CONTAINER_V1, value)
}

pub fn read_noe_consumer_fixture_v1(
    reference: &NoePublishedContainerReferenceV1,
) -> Result<NoeConsumerFixtureV1, DurableErrorV1> {
    read(reference, NOE_CONSUMER_FIXTURE_CONTAINER_V1)
}

pub fn publish_noe_component_receipt_v1(
    path: impl AsRef<Path>,
    value: &NoeComponentCapabilityReceiptV1,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    publish(path.as_ref(), NOE_COMPONENT_RECEIPT_CONTAINER_V1, value)
}

pub fn read_noe_component_receipt_v1(
    reference: &NoePublishedContainerReferenceV1,
) -> Result<NoeComponentCapabilityReceiptV1, DurableErrorV1> {
    read(reference, NOE_COMPONENT_RECEIPT_CONTAINER_V1)
}

pub fn publish_noe_run_manifest_v1(
    path: impl AsRef<Path>,
    value: &NoeRunConsumptionManifestV1,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    publish(path.as_ref(), NOE_RUN_MANIFEST_CONTAINER_V1, value)
}

pub fn read_noe_run_manifest_v1(
    reference: &NoePublishedContainerReferenceV1,
) -> Result<NoeRunConsumptionManifestV1, DurableErrorV1> {
    read(reference, NOE_RUN_MANIFEST_CONTAINER_V1)
}

fn publish<T: Clone + Serialize>(
    path: &Path,
    container_version: &str,
    value: &T,
) -> Result<NoePublishedContainerReferenceV1, DurableErrorV1> {
    let payload = ArtifactPayloadV1 {
        container_version: container_version.to_owned(),
        encoding: NOE_ARTIFACT_ENCODING_V1.to_owned(),
        value: value.clone(),
    };
    let boundary_sha256 = payload_digest(&payload, path)?;
    let container = ArtifactContainerV1 {
        payload,
        boundary_sha256: boundary_sha256.clone(),
    };
    let bytes = serde_json::to_vec(&container).map_err(|error| container_decode(path, error))?;
    publish_artifact_no_replace(path, &bytes)?;
    Ok(NoePublishedContainerReferenceV1 {
        location: path.to_string_lossy().into_owned(),
        boundary_sha256,
    })
}

fn read<T: Serialize + DeserializeOwned>(
    reference: &NoePublishedContainerReferenceV1,
    container_version: &str,
) -> Result<T, DurableErrorV1> {
    let path = PathBuf::from(&reference.location);
    let bytes = fs::read(&path).map_err(|error| DurableErrorV1::Io {
        operation: "读取 NOE 产物容器",
        path: path.clone(),
        message: error.to_string(),
    })?;
    let container: ArtifactContainerV1<T> =
        serde_json::from_slice(&bytes).map_err(|error| container_decode(&path, error))?;
    if container.payload.container_version != container_version {
        return Err(DurableErrorV1::UnsupportedContainerVersion {
            path,
            found: container.payload.container_version,
        });
    }
    if container.payload.encoding != NOE_ARTIFACT_ENCODING_V1 {
        return Err(DurableErrorV1::UnsupportedEncoding {
            path,
            found: container.payload.encoding,
        });
    }
    let found = payload_digest(&container.payload, &path)?;
    if found != container.boundary_sha256 {
        return Err(DurableErrorV1::BoundaryDigestMismatch {
            path,
            expected: container.boundary_sha256,
            found,
        });
    }
    if container.boundary_sha256 != reference.boundary_sha256 {
        return Err(DurableErrorV1::BoundaryDigestMismatch {
            path,
            expected: reference.boundary_sha256.clone(),
            found: container.boundary_sha256,
        });
    }
    Ok(container.payload.value)
}

fn payload_digest<T: Serialize>(value: &T, path: &Path) -> Result<Sha256Hex, DurableErrorV1> {
    let bytes = serde_json::to_vec(value).map_err(|error| container_decode(path, error))?;
    Ok(Sha256Hex(format!("{:x}", Sha256::digest(bytes))))
}

fn container_decode(path: &Path, error: serde_json::Error) -> DurableErrorV1 {
    DurableErrorV1::ContainerDecode {
        path: path.to_path_buf(),
        message: error.to_string(),
    }
}

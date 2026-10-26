#[cfg(test)]
use crate::encoding::encode_wal_op;
use crate::encoding::{decode_wal_op, encode_wal_op_into};
use crate::error::EngineError;
use crate::types::{OpTag, WalOp};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
const WAL_FILENAME: &str = "wal_0.wal";
const WAL_MAGIC: [u8; 4] = *b"OVGR";
const WAL_VERSION: u32 = 3;
const WAL_HEADER_SIZE: usize = 8; // WAL_MAGIC (4) + WAL_VERSION (4)
/// Maximum size for a single WAL record payload (64 MB).
const MAX_WAL_RECORD_SIZE: usize = 64 * 1024 * 1024;

/// WAL record frame: [len:u32][crc32:u32][payload:bytes]
/// - len: byte length of payload (not including len or crc fields)
/// - crc32: CRC-32 of payload bytes
/// - payload: encoded WalOp
#[derive(Clone, Copy)]
struct WalReadAtomicBatch {
    first_seq: u64,
    op_count: u32,
    ops_read: u32,
}

impl WalReadAtomicBatch {
    fn new(first_seq: u64, op_count: u32) -> Option<Self> {
        if first_seq == 0 || op_count < 2 {
            return None;
        }
        Some(Self {
            first_seq,
            op_count,
            ops_read: 0,
        })
    }

    fn push_normal_op(&mut self, seq: u64) -> bool {
        if self.ops_read >= self.op_count {
            return false;
        }
        let Some(expected_seq) = self.first_seq.checked_add(self.ops_read as u64) else {
            return false;
        };
        if seq != expected_seq {
            return false;
        }
        self.ops_read += 1;
        true
    }

    fn matches_commit(&self, first_seq: u64, op_count: u32) -> bool {
        self.first_seq == first_seq && self.op_count == op_count && self.ops_read == op_count
    }
}

pub(crate) fn write_wal_header(writer: &mut impl Write) -> Result<(), EngineError> {
    writer.write_all(&WAL_MAGIC)?;
    writer.write_all(&WAL_VERSION.to_le_bytes())?;
    Ok(())
}

fn validate_wal_header(header: &[u8; WAL_HEADER_SIZE]) -> Result<(), EngineError> {
    if header[..4] != WAL_MAGIC {
        return Err(EngineError::CorruptWal(format!(
            "invalid WAL magic: expected {:?}, got {:?}",
            &WAL_MAGIC,
            &header[..4]
        )));
    }
    let version = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if version != WAL_VERSION {
        return Err(EngineError::CorruptWal(format!(
            "unsupported WAL version: expected {}, got {}",
            WAL_VERSION, version
        )));
    }
    Ok(())
}

/// Write-ahead log writer. Appends framed records to the WAL file.
pub struct WalWriter {
    writer: BufWriter<File>,
    encode_buf: Vec<u8>,
    #[cfg(all(test, feature = "production-concurrency"))]
    fault: Option<WalFault>,
}

#[cfg(all(test, feature = "production-concurrency"))]
#[derive(Clone, Copy)]
pub(crate) enum WalFault {
    AppendPrefix(usize),
    SyncBeforeFlush,
    SyncAfterFlush,
    SyncAfterFsync,
}

pub(crate) fn encode_v3_frame_into(
    encode_buf: &mut Vec<u8>,
    seq: u64,
    op: &WalOp,
    batch_buf: &mut Vec<u8>,
) -> Result<(), EngineError> {
    encode_wal_op_into(op, encode_buf)?;
    append_encoded_frame(encode_buf, seq, batch_buf)
}

fn append_encoded_frame(
    encoded: &[u8],
    seq: u64,
    batch_buf: &mut Vec<u8>,
) -> Result<(), EngineError> {
    let seq_bytes = seq.to_le_bytes();
    let len = encoded
        .len()
        .checked_add(8)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| EngineError::SerializationError("WAL frame exceeds u32 length".into()))?;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&seq_bytes);
    hasher.update(encoded);
    let crc = hasher.finalize();

    batch_buf.extend_from_slice(&len.to_le_bytes());
    batch_buf.extend_from_slice(&crc.to_le_bytes());
    batch_buf.extend_from_slice(&seq_bytes);
    batch_buf.extend_from_slice(encoded);
    Ok(())
}

pub(crate) fn prepare_v3_batch_into(
    ops: &[(u64, WalOp)],
    encode_buf: &mut Vec<u8>,
    batch_buf: &mut Vec<u8>,
) -> Result<(), EngineError> {
    prepare_v3_batch_with(ops, encode_buf, batch_buf, encode_v3_frame_into)
}

#[allow(dead_code)] // Private candidate adapter; public engine wiring remains gated.
pub(crate) fn prepare_v3_batch_bounded(
    ops: &[(u64, WalOp)],
    encode_buf: &mut Vec<u8>,
    batch_buf: &mut Vec<u8>,
) -> Result<(), EngineError> {
    prepare_v3_batch_with(ops, encode_buf, batch_buf, |encoded, seq, op, batch| {
        crate::encoding::encode_wal_op_bounded(op, encoded)?;
        append_encoded_frame(encoded, seq, batch)
    })
}

#[cfg(feature = "production-concurrency")]
pub(crate) fn prepare_v3_operations_bounded(
    ops: &[WalOp],
    first: u64,
    encode: &mut Vec<u8>,
    frames: &mut Vec<u8>,
    maximum: usize,
) -> Result<(), EngineError> {
    use crate::encoding::{encode_wal_op_to_limit, wal_byte_limit};
    frames.clear();
    encode.clear();
    let count = u32::try_from(ops.len())
        .map_err(|_| EngineError::InvalidOperation("atomic WAL op count exceeds u32".into()))?;
    if let Some(last) = ops.len().checked_sub(1) {
        first
            .checked_add(last as u64)
            .ok_or_else(|| EngineError::InvalidOperation("WAL sequence range overflow".into()))?;
    }
    // Every frame has a 16-byte envelope; each atomic marker has 13 payload bytes.
    let _overhead = request_frame_overhead(ops.len(), maximum)?;
    if ops.len() > 1 {
        encode_v3_frame_into(
            encode,
            first,
            &WalOp::BeginAtomicBatch {
                first_seq: first,
                op_count: count,
            },
            frames,
        )?;
    }
    for (i, op) in ops.iter().enumerate() {
        // Reserve this and all later envelopes plus the final marker before encoding.
        let remaining_overhead = (ops.len() - i) * 16 + if ops.len() > 1 { 29 } else { 0 };
        let committed = frames
            .len()
            .checked_add(remaining_overhead)
            .ok_or_else(|| wal_byte_limit(u64::MAX, maximum))?;
        let allowance = maximum
            .checked_sub(committed)
            .ok_or_else(|| wal_byte_limit(committed as u64, maximum))?;
        encode_wal_op_to_limit(op, encode, allowance).map_err(|error| match error {
            EngineError::ResourceLimit {
                resource: "max_batch_bytes",
                requested,
                ..
            } => wal_byte_limit((committed as u64).saturating_add(requested), maximum),
            other => other,
        })?;
        let frame_size = encode
            .len()
            .checked_add(16)
            .and_then(|n| frames.len().checked_add(n))
            .ok_or_else(|| wal_byte_limit(u64::MAX, maximum))?;
        if frame_size > maximum {
            return Err(wal_byte_limit(frame_size as u64, maximum));
        }
        append_encoded_frame(encode, first + i as u64, frames)?;
    }
    if ops.len() > 1 {
        encode_v3_frame_into(
            encode,
            first,
            &WalOp::CommitAtomicBatch {
                first_seq: first,
                op_count: count,
            },
            frames,
        )?;
    }
    assert!(
        frames.len() <= maximum,
        "bounded WAL frames exceeded request limit"
    );
    Ok(())
}

#[cfg(feature = "production-concurrency")]
fn request_frame_overhead(count: usize, maximum: usize) -> Result<usize, EngineError> {
    let markers = if count > 1 { 58 } else { 0 };
    let overhead = count
        .checked_mul(16)
        .and_then(|n| n.checked_add(markers))
        .ok_or_else(|| crate::encoding::wal_byte_limit(u64::MAX, maximum))?;
    if overhead > maximum {
        return Err(crate::encoding::wal_byte_limit(overhead as u64, maximum));
    }
    Ok(overhead)
}

fn prepare_v3_batch_with(
    ops: &[(u64, WalOp)],
    encode_buf: &mut Vec<u8>,
    batch_buf: &mut Vec<u8>,
    mut frame: impl FnMut(&mut Vec<u8>, u64, &WalOp, &mut Vec<u8>) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    batch_buf.clear();
    match ops {
        [] => {}
        [(seq, op)] => frame(encode_buf, *seq, op, batch_buf)?,
        _ => {
            let first_seq = ops[0].0;
            let op_count = u32::try_from(ops.len()).map_err(|_| {
                EngineError::InvalidOperation(
                    "atomic WAL batch op_count exceeds u32::MAX".to_string(),
                )
            })?;
            if first_seq == 0 {
                return Err(EngineError::InvalidOperation(
                    "atomic WAL batch first sequence must be nonzero".to_string(),
                ));
            }
            for (idx, (seq, _)) in ops.iter().enumerate() {
                let expected_seq = first_seq.checked_add(idx as u64).ok_or_else(|| {
                    EngineError::InvalidOperation(
                        "atomic WAL batch sequence range overflows u64".to_string(),
                    )
                })?;
                if *seq != expected_seq {
                    return Err(EngineError::InvalidOperation(
                        "atomic WAL batch sequences must be contiguous".to_string(),
                    ));
                }
            }
            let begin = WalOp::BeginAtomicBatch {
                first_seq,
                op_count,
            };
            frame(encode_buf, first_seq, &begin, batch_buf)?;
            for (seq, op) in ops {
                frame(encode_buf, *seq, op, batch_buf)?;
            }
            let commit = WalOp::CommitAtomicBatch {
                first_seq,
                op_count,
            };
            frame(encode_buf, first_seq, &commit, batch_buf)?;
        }
    }

    Ok(())
}

/// Exact allocation-free admission for the same frames as prepare_v3_batch_into.
#[cfg(feature = "production-concurrency")]
pub(crate) fn v3_batches_encoded_len(
    batches: &[&[(u64, WalOp)]],
    limit: usize,
) -> Result<usize, EngineError> {
    let mut bytes = WAL_HEADER_SIZE;
    if bytes > limit {
        return Err(EngineError::InvalidOperation(
            "WAL input exceeds admitted bytes".into(),
        ));
    }
    for batch in batches {
        if batch.len() > 1 {
            bytes = bytes
                .checked_add(2 * (16 + 13))
                .ok_or_else(|| EngineError::InvalidOperation("WAL size overflow".into()))?;
        }
        for (_, op) in *batch {
            let remaining = limit
                .checked_sub(bytes)
                .and_then(|n| n.checked_sub(16))
                .ok_or_else(|| {
                    EngineError::InvalidOperation("WAL input exceeds admitted bytes".into())
                })?;
            bytes = bytes
                .checked_add(16 + crate::encoding::wal_op_encoded_len(op, remaining)?)
                .ok_or_else(|| EngineError::InvalidOperation("WAL size overflow".into()))?;
        }
        if bytes > limit {
            return Err(EngineError::InvalidOperation(
                "WAL input exceeds admitted bytes".into(),
            ));
        }
    }
    Ok(bytes)
}

impl WalWriter {
    /// Open or create a WAL generation file for appending.
    pub fn open_generation(db_dir: &Path, gen_id: u64) -> Result<Self, EngineError> {
        let path = wal_generation_path(db_dir, gen_id);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let file_len = file.metadata()?.len();
        let needs_header = if file_len == 0 {
            true
        } else if file_len < WAL_HEADER_SIZE as u64 {
            return Err(EngineError::CorruptWal(
                "WAL file too small for header".into(),
            ));
        } else {
            let mut header = [0u8; WAL_HEADER_SIZE];
            (&file).read_exact(&mut header)?;
            validate_wal_header(&header)?;
            false
        };

        let mut writer = BufWriter::new(file);

        if needs_header {
            write_wal_header(&mut writer)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(WalWriter {
            writer,
            encode_buf: Vec::new(),
            #[cfg(all(test, feature = "production-concurrency"))]
            fault: None,
        })
    }

    /// Append a single WalOp to the WAL with its engine sequence number.
    /// The seq is stored as an 8-byte LE prefix inside the CRC-protected payload.
    /// Frame: [len:u32][crc:u32][seq:u64][walop_bytes]
    /// Returns the byte size written.
    pub(crate) fn append(&mut self, op: &WalOp, engine_seq: u64) -> Result<usize, EngineError> {
        encode_wal_op_into(op, &mut self.encode_buf)?;
        let seq_bytes = engine_seq.to_le_bytes();
        let total_payload = 8 + self.encode_buf.len();
        let len = total_payload as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&seq_bytes);
        hasher.update(&self.encode_buf);
        let crc = hasher.finalize();

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&seq_bytes)?;
        self.writer.write_all(&self.encode_buf)?;

        // 4 (len) + 4 (crc) + 8 (seq) + walop_bytes
        Ok(8 + total_payload)
    }

    /// Append multiple records through the shared production V3 encoder.
    pub(crate) fn append_batch(&mut self, ops: &[(u64, WalOp)]) -> Result<usize, EngineError> {
        let mut batch_buf = Vec::new();
        self.prepare_batch_into(ops, &mut batch_buf)?;
        self.append_prepared(&batch_buf)
    }

    pub(crate) fn prepare_batch_into(
        &mut self,
        ops: &[(u64, WalOp)],
        batch_buf: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        prepare_v3_batch_into(ops, &mut self.encode_buf, batch_buf)
    }

    #[allow(dead_code)] // Private candidate adapter; public engine wiring remains gated.
    pub(crate) fn prepare_batch_bounded(
        &mut self,
        ops: &[(u64, WalOp)],
        batch_buf: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        prepare_v3_batch_bounded(ops, &mut self.encode_buf, batch_buf)
    }

    /// The actual acceptance write. A returned error may leave real bytes in the
    /// buffer/file; callers must not infer rollback from this result.
    pub(crate) fn append_prepared(&mut self, batch_buf: &[u8]) -> Result<usize, EngineError> {
        #[cfg(all(test, feature = "production-concurrency"))]
        if let Some(WalFault::AppendPrefix(bytes)) = self.fault {
            self.fault = None;
            self.writer
                .write_all(&batch_buf[..bytes.min(batch_buf.len())])?;
            self.writer.flush()?;
            return Err(std::io::Error::other("injected WAL partial append").into());
        }
        self.writer.write_all(batch_buf)?;
        Ok(batch_buf.len())
    }

    /// Candidate shutdown closes the OS handle on its coordinator without a
    /// hidden BufWriter Drop flush. Already-accounted buffers remain registered
    /// until their guardian's worker destroys them, including on sync error.
    #[cfg(feature = "production-concurrency")]
    pub(crate) fn close_retaining_buffers(self) -> (Vec<u8>, Vec<u8>) {
        let Self {
            writer, encode_buf, ..
        } = self;
        let (file, buffered) = writer.into_parts();
        drop(file);
        (
            encode_buf,
            buffered.unwrap_or_else(|panicked| panicked.into_inner()),
        )
    }

    #[cfg(all(test, feature = "production-concurrency"))]
    pub(crate) fn inject_fault(&mut self, fault: WalFault) {
        self.fault = Some(fault);
    }

    #[cfg(test)]
    pub(crate) fn flush(&mut self) -> Result<(), EngineError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Sync the WAL to disk (flush + fsync).
    pub fn sync(&mut self) -> Result<(), EngineError> {
        #[cfg(all(test, feature = "production-concurrency"))]
        if matches!(self.fault, Some(WalFault::SyncBeforeFlush)) {
            self.fault = None;
            return Err(std::io::Error::other("injected WAL sync before flush").into());
        }
        self.writer.flush()?;
        #[cfg(all(test, feature = "production-concurrency"))]
        if matches!(self.fault, Some(WalFault::SyncAfterFlush)) {
            self.fault = None;
            return Err(std::io::Error::other("injected WAL sync after flush").into());
        }
        self.writer.get_ref().sync_all()?;
        #[cfg(all(test, feature = "production-concurrency"))]
        if matches!(self.fault, Some(WalFault::SyncAfterFsync)) {
            self.fault = None;
            return Err(std::io::Error::other("injected WAL sync after fsync").into());
        }
        Ok(())
    }
}

/// Write-ahead log reader. Reads framed records with CRC validation.
pub struct WalReader {
    #[cfg(test)]
    path: PathBuf,
}

pub(crate) struct WalReadResult {
    pub(crate) records: Vec<(u64, WalOp)>,
    pub(crate) durable_len: u64,
}

#[derive(Debug)]
pub(crate) struct WalScrubScan {
    pub(crate) records_checked: u64,
    pub(crate) bytes_checked: u64,
    pub(crate) durable_len: u64,
    pub(crate) file_len: u64,
    pub(crate) terminal: Option<WalScrubTerminal>,
}

#[derive(Debug)]
pub(crate) enum WalScrubTerminal {
    RecoverableTrailing { reason: String },
    Corrupt { reason: String },
}

struct CountedReadError {
    bytes_read: usize,
    error: std::io::Error,
}

pub(crate) fn scrub_generation_readonly(
    db_dir: &Path,
    gen_id: u64,
) -> Result<WalScrubScan, EngineError> {
    let path = wal_generation_path(db_dir, gen_id);
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalScrubScan {
                records_checked: 0,
                bytes_checked: 0,
                durable_len: 0,
                file_len: 0,
                terminal: None,
            });
        }
        Err(error) => {
            return Ok(WalScrubScan {
                records_checked: 0,
                bytes_checked: 0,
                durable_len: 0,
                file_len: 0,
                terminal: Some(WalScrubTerminal::Corrupt {
                    reason: format!("cannot stat WAL file: {error}"),
                }),
            });
        }
    };
    let file_len = metadata.len();
    if file_len == 0 {
        return Ok(WalScrubScan {
            records_checked: 0,
            bytes_checked: 0,
            durable_len: 0,
            file_len,
            terminal: None,
        });
    }
    if file_len < WAL_HEADER_SIZE as u64 {
        return Ok(WalScrubScan {
            records_checked: 0,
            bytes_checked: file_len,
            durable_len: 0,
            file_len,
            terminal: Some(WalScrubTerminal::Corrupt {
                reason: "WAL file too small for header".to_string(),
            }),
        });
    }

    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) => {
            return Ok(WalScrubScan {
                records_checked: 0,
                bytes_checked: 0,
                durable_len: 0,
                file_len,
                terminal: Some(WalScrubTerminal::Corrupt {
                    reason: format!("cannot open WAL file: {error}"),
                }),
            });
        }
    };
    let mut reader = BufReader::new(file);
    let mut bytes_checked = 0u64;

    let mut header = [0u8; WAL_HEADER_SIZE];
    let header_read = match read_exact_counted(&mut reader, &mut header) {
        Ok(bytes_read) => bytes_read,
        Err(error) => {
            bytes_checked += error.bytes_read as u64;
            return Ok(WalScrubScan {
                records_checked: 0,
                bytes_checked,
                durable_len: 0,
                file_len,
                terminal: Some(WalScrubTerminal::Corrupt {
                    reason: format!("I/O error reading WAL header: {}", error.error),
                }),
            });
        }
    };
    bytes_checked += header_read as u64;
    if header_read < WAL_HEADER_SIZE {
        return Ok(WalScrubScan {
            records_checked: 0,
            bytes_checked,
            durable_len: 0,
            file_len,
            terminal: Some(WalScrubTerminal::Corrupt {
                reason: "WAL file too small for header".to_string(),
            }),
        });
    }
    if let Err(error) = validate_wal_header(&header) {
        return Ok(WalScrubScan {
            records_checked: 0,
            bytes_checked,
            durable_len: 0,
            file_len,
            terminal: Some(WalScrubTerminal::Corrupt {
                reason: error.to_string(),
            }),
        });
    }

    let mut open_batch: Option<WalReadAtomicBatch> = None;
    let mut pos = WAL_HEADER_SIZE as u64;
    let mut durable_len = pos;
    let mut records_checked = 0u64;
    let mut terminal = None;

    loop {
        let frame_start = pos;
        let mut len_buf = [0u8; 4];
        let len_read = match read_exact_counted(&mut reader, &mut len_buf) {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                bytes_checked += error.bytes_read as u64;
                terminal = Some(WalScrubTerminal::Corrupt {
                    reason: format!("I/O error reading WAL frame length: {}", error.error),
                });
                break;
            }
        };
        bytes_checked += len_read as u64;
        if len_read == 0 {
            break;
        }
        if len_read < len_buf.len() {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "partial WAL frame length at end of file".to_string(),
            });
            break;
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "zero-length WAL frame".to_string(),
            });
            break;
        }
        if payload_len > MAX_WAL_RECORD_SIZE {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: format!(
                    "WAL frame payload length {payload_len} exceeds maximum {MAX_WAL_RECORD_SIZE}"
                ),
            });
            break;
        }
        let frame_end = frame_start + 8 + payload_len as u64;

        let mut crc_buf = [0u8; 4];
        let crc_read = match read_exact_counted(&mut reader, &mut crc_buf) {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                bytes_checked += error.bytes_read as u64;
                terminal = Some(WalScrubTerminal::Corrupt {
                    reason: format!("I/O error reading WAL frame CRC: {}", error.error),
                });
                break;
            }
        };
        bytes_checked += crc_read as u64;
        if crc_read < crc_buf.len() {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "partial WAL frame CRC at end of file".to_string(),
            });
            break;
        }
        let stored_crc = u32::from_le_bytes(crc_buf);

        let mut payload = vec![0u8; payload_len];
        let payload_read = match read_exact_counted(&mut reader, &mut payload) {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                bytes_checked += error.bytes_read as u64;
                terminal = Some(WalScrubTerminal::Corrupt {
                    reason: format!("I/O error reading WAL frame payload: {}", error.error),
                });
                break;
            }
        };
        bytes_checked += payload_read as u64;
        if payload_read < payload_len {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "partial WAL frame payload at end of file".to_string(),
            });
            break;
        }

        if crc32fast::hash(&payload) != stored_crc {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "WAL frame CRC mismatch".to_string(),
            });
            break;
        }

        if payload.len() < 8 {
            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                reason: "WAL frame is missing engine sequence prefix".to_string(),
            });
            break;
        }
        let engine_seq = u64::from_le_bytes(payload[..8].try_into().expect("8 bytes for seq"));
        let walop_bytes = &payload[8..];
        let recognized_tag = walop_bytes.first().and_then(|tag| OpTag::from_u8(*tag));

        match decode_wal_op(walop_bytes) {
            Ok(op) => match &op {
                WalOp::BeginAtomicBatch {
                    first_seq,
                    op_count,
                } => {
                    if open_batch.is_some() {
                        terminal = Some(WalScrubTerminal::RecoverableTrailing {
                            reason: "nested WAL atomic batch begin".to_string(),
                        });
                        break;
                    }
                    let Some(batch) = WalReadAtomicBatch::new(*first_seq, *op_count) else {
                        terminal = Some(WalScrubTerminal::RecoverableTrailing {
                            reason: "malformed WAL atomic batch begin marker".to_string(),
                        });
                        break;
                    };
                    open_batch = Some(batch);
                    records_checked += 1;
                    pos = frame_end;
                }
                WalOp::CommitAtomicBatch {
                    first_seq,
                    op_count,
                } => {
                    let Some(batch) = open_batch else {
                        terminal = Some(WalScrubTerminal::RecoverableTrailing {
                            reason: "WAL atomic batch commit without begin".to_string(),
                        });
                        break;
                    };
                    if !batch.matches_commit(*first_seq, *op_count) {
                        terminal = Some(WalScrubTerminal::RecoverableTrailing {
                            reason: "WAL atomic batch commit does not match begin".to_string(),
                        });
                        break;
                    }
                    open_batch = None;
                    records_checked += 1;
                    durable_len = frame_end;
                    pos = frame_end;
                }
                _ => {
                    if let Some(batch) = open_batch.as_mut() {
                        if !batch.push_normal_op(engine_seq) {
                            terminal = Some(WalScrubTerminal::RecoverableTrailing {
                                reason: "WAL atomic batch sequence is not contiguous".to_string(),
                            });
                            break;
                        }
                    } else {
                        durable_len = frame_end;
                    }
                    records_checked += 1;
                    pos = frame_end;
                }
            },
            Err(_)
                if matches!(
                    recognized_tag,
                    Some(OpTag::BeginAtomicBatch | OpTag::CommitAtomicBatch)
                ) =>
            {
                terminal = Some(WalScrubTerminal::RecoverableTrailing {
                    reason: "malformed WAL atomic marker".to_string(),
                });
                break;
            }
            Err(error) if recognized_tag.is_some() => {
                terminal = Some(WalScrubTerminal::Corrupt {
                    reason: format!("failed to decode WAL record: {error}"),
                });
                break;
            }
            Err(_) => {
                terminal = Some(WalScrubTerminal::RecoverableTrailing {
                    reason: "unknown WAL operation tag".to_string(),
                });
                break;
            }
        }
    }

    if terminal.is_none() && open_batch.is_some() {
        terminal = Some(WalScrubTerminal::RecoverableTrailing {
            reason: "incomplete WAL atomic batch at end of file".to_string(),
        });
    }

    Ok(WalScrubScan {
        records_checked,
        bytes_checked,
        durable_len,
        file_len,
        terminal,
    })
}

fn read_exact_counted(
    reader: &mut BufReader<File>,
    buf: &mut [u8],
) -> Result<usize, CountedReadError> {
    let mut read = 0usize;
    while read < buf.len() {
        match reader.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(error) => {
                return Err(CountedReadError {
                    bytes_read: read,
                    error,
                });
            }
        }
    }
    Ok(read)
}

impl WalReader {
    #[cfg(test)]
    pub(crate) fn new(db_dir: &Path) -> Self {
        WalReader {
            path: wal_generation_path(db_dir, 0),
        }
    }

    #[cfg(test)]
    pub(crate) fn read_all(&self) -> Result<Vec<(u64, WalOp)>, EngineError> {
        Self::read_path(&self.path).map(|result| result.records)
    }

    /// Read all valid records from a WAL generation file.
    #[cfg(test)]
    pub(crate) fn read_generation(
        db_dir: &Path,
        gen_id: u64,
    ) -> Result<Vec<(u64, WalOp)>, EngineError> {
        Self::read_generation_recoverable(db_dir, gen_id).map(|result| result.records)
    }

    /// Read all recoverable records plus the byte offset of the durable prefix.
    pub(crate) fn read_generation_recoverable(
        db_dir: &Path,
        gen_id: u64,
    ) -> Result<WalReadResult, EngineError> {
        let path = wal_generation_path(db_dir, gen_id);
        Self::read_path(&path)
    }

    fn read_path(path: &Path) -> Result<WalReadResult, EngineError> {
        if !path.exists() {
            return Ok(WalReadResult {
                records: Vec::new(),
                durable_len: 0,
            });
        }

        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(WalReadResult {
                records: Vec::new(),
                durable_len: 0,
            });
        }

        if file_len < WAL_HEADER_SIZE as u64 {
            return Err(EngineError::CorruptWal(
                "WAL file too small for header".into(),
            ));
        }

        let mut reader = BufReader::new(file);

        // Validate header
        let mut header = [0u8; WAL_HEADER_SIZE];
        reader
            .read_exact(&mut header)
            .map_err(EngineError::IoError)?;
        validate_wal_header(&header)?;

        let mut ops = Vec::new();
        let mut open_batch: Option<WalReadAtomicBatch> = None;
        let mut pos = WAL_HEADER_SIZE as u64;
        let mut durable_len = pos;

        loop {
            let frame_start = pos;
            // Read length
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(EngineError::IoError(e)),
            }
            let payload_len = u32::from_le_bytes(len_buf) as usize;

            if payload_len == 0 || payload_len > MAX_WAL_RECORD_SIZE {
                break;
            }
            let frame_end = frame_start + 8 + payload_len as u64;

            // Read CRC
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(_) => break,
            }
            let stored_crc = u32::from_le_bytes(crc_buf);

            // Read payload
            let mut payload = vec![0u8; payload_len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(_) => break,
            }

            // Validate CRC
            if crc32fast::hash(&payload) != stored_crc {
                break;
            }

            // V3 frame: first 8 bytes are engine_seq
            if payload.len() < 8 {
                break;
            }
            let engine_seq = u64::from_le_bytes(payload[..8].try_into().expect("8 bytes for seq"));
            let walop_bytes = &payload[8..];

            let recognized_tag = walop_bytes.first().and_then(|tag| OpTag::from_u8(*tag));

            match decode_wal_op(walop_bytes) {
                Ok(op) => match &op {
                    WalOp::BeginAtomicBatch {
                        first_seq,
                        op_count,
                    } => {
                        if open_batch.is_some() {
                            break;
                        }
                        let Some(batch) = WalReadAtomicBatch::new(*first_seq, *op_count) else {
                            break;
                        };
                        open_batch = Some(batch);
                        ops.push((engine_seq, op));
                        pos = frame_end;
                    }
                    WalOp::CommitAtomicBatch {
                        first_seq,
                        op_count,
                    } => {
                        let Some(batch) = open_batch else {
                            break;
                        };
                        if !batch.matches_commit(*first_seq, *op_count) {
                            break;
                        }
                        open_batch = None;
                        ops.push((engine_seq, op));
                        durable_len = frame_end;
                        pos = frame_end;
                    }
                    _ => {
                        if let Some(batch) = open_batch.as_mut() {
                            if !batch.push_normal_op(engine_seq) {
                                break;
                            }
                        } else {
                            durable_len = frame_end;
                        }
                        ops.push((engine_seq, op));
                        pos = frame_end;
                    }
                },
                Err(_)
                    if matches!(
                        recognized_tag,
                        Some(OpTag::BeginAtomicBatch | OpTag::CommitAtomicBatch)
                    ) =>
                {
                    break;
                }
                Err(_) if open_batch.is_some() && recognized_tag.is_some() => {
                    break;
                }
                Err(err) if recognized_tag.is_some() => {
                    return Err(EngineError::CorruptWal(format!(
                        "failed to decode WAL record: {}",
                        err
                    )));
                }
                Err(_) => break,
            }
        }

        Ok(WalReadResult {
            records: ops,
            durable_len,
        })
    }
}

/// WAL generation file path: `wal_<generation_id>.wal`
pub fn wal_generation_path(db_dir: &Path, gen_id: u64) -> PathBuf {
    db_dir.join(format!("wal_{}.wal", gen_id))
}

/// Delete a specific WAL generation file.
pub fn remove_wal_generation(db_dir: &Path, gen_id: u64) -> Result<(), EngineError> {
    let path = wal_generation_path(db_dir, gen_id);
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

pub(crate) fn truncate_wal_generation_to(
    db_dir: &Path,
    gen_id: u64,
    durable_len: u64,
) -> Result<(), EngineError> {
    let path = wal_generation_path(db_dir, gen_id);
    if !path.exists() {
        return Ok(());
    }
    let file = OpenOptions::new().write(true).open(&path)?;
    if file.metadata()?.len() > durable_len {
        file.set_len(durable_len)?;
        file.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn make_test_node(id: u64, key: &str) -> WalOp {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String(key.to_string()));
        WalOp::UpsertNode(NodeRecord {
            id,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: key.to_string(),
            props,
            created_at: 1000 * id as i64,
            updated_at: 1000 * id as i64 + 1,
            weight: 0.5,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        })
    }

    #[test]
    fn p45a2_wal_iterative_preparation_preserves_fields_crc_and_atomic_frames() {
        let mut node = make_test_node(7, "wire-parity");
        if let WalOp::UpsertNode(node) = &mut node {
            node.props.insert(
                "mixed".into(),
                PropValue::Array(vec![
                    PropValue::Bytes(vec![0, 128, 255]),
                    PropValue::Map(BTreeMap::from([("n".into(), PropValue::UInt(u64::MAX))])),
                    PropValue::Float(-0.0),
                ]),
            );
            node.dense_vector = Some(vec![0.0, 0.5]);
            node.sparse_vector = Some(vec![(2, 0.75)]);
        }
        let ops = [
            (1, node),
            (2, make_test_edge(8, 7, 9)),
            (
                3,
                WalOp::DeleteNode {
                    id: 9,
                    deleted_at: 123,
                },
            ),
            (
                4,
                WalOp::EnsureNodeLabel {
                    label: "wire".into(),
                    label_id: 3,
                },
            ),
        ];
        for input in [&ops[..1], &ops[..]] {
            let mut original = Vec::new();
            let mut governed = Vec::new();
            prepare_v3_batch_into(input, &mut Vec::new(), &mut original).unwrap();
            prepare_v3_batch_bounded(input, &mut Vec::new(), &mut governed).unwrap();
            assert_eq!(governed, original);
            let directory = TempDir::new().unwrap();
            let mut writer = WalWriter::open_generation(directory.path(), 0).unwrap();
            writer.append_prepared(&governed).unwrap();
            writer.sync().unwrap();
            // read_all retains atomic markers as part of the existing codec.
            assert_eq!(
                WalReader::new(directory.path()).read_all().unwrap().len(),
                input.len() + if input.len() > 1 { 2 } else { 0 }
            );
        }
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn post_p45b_wal_cumulative_limit_matches_all_fields_and_atomic_envelopes() {
        let mut n = make_test_node(1, "byte-parity");
        if let WalOp::UpsertNode(n) = &mut n {
            n.props.insert(
                "nested".into(),
                PropValue::Array(vec![
                    PropValue::Bytes(vec![0, 128, 255]),
                    PropValue::Map(BTreeMap::from([("float".into(), PropValue::Float(-0.0))])),
                ]),
            );
            n.dense_vector = Some(vec![1.0, 2.0]);
            n.sparse_vector = Some(vec![(7, 0.5)]);
        }
        let all = [
            n,
            make_test_edge(2, 1, 3),
            WalOp::DeleteNode {
                id: 3,
                deleted_at: 8,
            },
            WalOp::DeleteEdge {
                id: 4,
                deleted_at: 9,
            },
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
            WalOp::EnsureEdgeLabel {
                label: "E".into(),
                label_id: 2,
            },
        ];
        for ops in [&all[..0], &all[..1], &all[..2], &all[..]] {
            let old: Vec<_> = ops
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, op)| (i as u64 + 1, op))
                .collect();
            let mut expected = Vec::new();
            prepare_v3_batch_into(&old, &mut Vec::new(), &mut expected).unwrap();
            let mut encoded = vec![1, 2, 3];
            let mut actual = vec![4, 5, 6];
            prepare_v3_operations_bounded(ops, 1, &mut encoded, &mut actual, expected.len())
                .unwrap();
            assert_eq!(actual, expected);
            if !expected.is_empty() {
                let limit = expected.len() - 1;
                let err = prepare_v3_operations_bounded(ops, 1, &mut encoded, &mut actual, limit)
                    .unwrap_err();
                assert!(
                    matches!(err,EngineError::ResourceLimit{resource:"max_batch_bytes",requested,limit:l,retryable:false}if requested>l&&l==limit as u64)
                );
                assert!(actual.len() <= limit);
                assert!(encoded.len() <= limit);
            }
        }
        assert!(matches!(
            request_frame_overhead(usize::MAX, 1024),
            Err(EngineError::ResourceLimit {
                resource: "max_batch_bytes",
                ..
            })
        ));
        let ops = [
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 0,
            },
            WalOp::DeleteNode {
                id: 2,
                deleted_at: 0,
            },
        ];
        let mut frames = Vec::new();
        let mut scratch = Vec::new();
        prepare_v3_operations_bounded(&ops, 9, &mut scratch, &mut frames, 124).unwrap();
        assert_eq!(frames.len(), 58 + 2 * (16 + 17));
        assert!(
            prepare_v3_operations_bounded(&ops, u64::MAX, &mut scratch, &mut frames, 1000).is_err()
        );
        assert!(frames.is_empty());
    }
    #[cfg(feature = "production-concurrency")]
    #[test]
    fn post_p45b_wal_large_operation_stops_before_rejected_output_is_built() {
        let mut op = make_test_node(1, "small");
        if let WalOp::UpsertNode(n) = &mut op {
            // The first admitted chunk fits; a large later nested chunk must not append.
            n.props.insert("a".into(), PropValue::Int(1));
            n.props.insert(
                "z".into(),
                PropValue::Array(vec![PropValue::Bytes(vec![42; 2 * 1024 * 1024])]),
            );
            n.dense_vector = Some(vec![1.0; 100]);
            n.sparse_vector = Some(vec![(2, 0.5); 100]);
        }
        let mut scratch = Vec::new();
        let mut frames = Vec::new();
        let err =
            prepare_v3_operations_bounded(&[op], 1, &mut scratch, &mut frames, 4096).unwrap_err();
        assert!(matches!(
            err,
            EngineError::ResourceLimit {
                resource: "max_batch_bytes",
                ..
            }
        ));
        assert!(scratch.len() < 4096);
        assert!(frames.is_empty());
        assert!(scratch.capacity() < 8192);
        let key = make_test_node(2, &"k".repeat(65000));
        scratch.clear();
        let err =
            prepare_v3_operations_bounded(&[key], 1, &mut scratch, &mut frames, 1024).unwrap_err();
        assert!(matches!(
            err,
            EngineError::ResourceLimit {
                resource: "max_batch_bytes",
                ..
            }
        ));
        assert!(scratch.is_empty());
        assert!(frames.is_empty());
    }

    fn make_test_edge(id: u64, from: u64, to: u64) -> WalOp {
        WalOp::UpsertEdge(EdgeRecord {
            id,
            from,
            to,
            label_id: 10,
            props: BTreeMap::new(),
            created_at: 2000 * id as i64,
            updated_at: 2000 * id as i64 + 1,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        })
    }

    fn append_raw_payload_frame(path: &Path, payload: &[u8]) {
        let crc = crc32fast::hash(payload);
        let len = payload.len() as u32;
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.write_all(payload).unwrap();
        file.flush().unwrap();
    }

    fn append_raw_walop_frame(path: &Path, seq: u64, walop_bytes: &[u8]) {
        let mut payload = Vec::new();
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.extend_from_slice(walop_bytes);
        append_raw_payload_frame(path, &payload);
    }

    fn assert_recoverable_terminal(scan: &WalScrubScan, expected_reason: &str) {
        match &scan.terminal {
            Some(WalScrubTerminal::RecoverableTrailing { reason }) => {
                assert!(
                    reason.contains(expected_reason),
                    "expected recoverable reason containing {expected_reason:?}, got {reason:?}"
                );
            }
            other => panic!("expected recoverable terminal, got {other:?}"),
        }
    }

    fn assert_corrupt_terminal(scan: &WalScrubScan, expected_reason: &str) {
        match &scan.terminal {
            Some(WalScrubTerminal::Corrupt { reason }) => {
                assert!(
                    reason.contains(expected_reason),
                    "expected corrupt reason containing {expected_reason:?}, got {reason:?}"
                );
            }
            other => panic!("expected corrupt terminal, got {other:?}"),
        }
    }

    #[test]
    fn scrub_wal_valid_empty_header() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        drop(writer);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.file_len, WAL_HEADER_SIZE as u64);
        assert_eq!(scan.durable_len, WAL_HEADER_SIZE as u64);
        assert_eq!(scan.bytes_checked, WAL_HEADER_SIZE as u64);
        assert_eq!(scan.records_checked, 0);
        assert!(scan.terminal.is_none());
    }

    #[test]
    fn scrub_wal_zero_byte_generation_is_empty() {
        let dir = TempDir::new().unwrap();
        File::create(dir.path().join(WAL_FILENAME)).unwrap();

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.file_len, 0);
        assert_eq!(scan.durable_len, 0);
        assert_eq!(scan.bytes_checked, 0);
        assert_eq!(scan.records_checked, 0);
        assert!(scan.terminal.is_none());
    }

    #[test]
    fn scrub_wal_valid_single_op() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let file_len = std::fs::metadata(dir.path().join(WAL_FILENAME))
            .unwrap()
            .len();

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.file_len, file_len);
        assert_eq!(scan.durable_len, file_len);
        assert_eq!(scan.bytes_checked, file_len);
        assert_eq!(scan.records_checked, 1);
        assert!(scan.terminal.is_none());
    }

    #[test]
    fn scrub_wal_valid_atomic_batch() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer
            .append_batch(&[(1, make_test_node(1, "one")), (2, make_test_node(2, "two"))])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let file_len = std::fs::metadata(dir.path().join(WAL_FILENAME))
            .unwrap()
            .len();

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.file_len, file_len);
        assert_eq!(scan.durable_len, file_len);
        assert_eq!(scan.bytes_checked, file_len);
        assert_eq!(scan.records_checked, 4);
        assert!(scan.terminal.is_none());
    }

    #[test]
    fn scrub_wal_partial_trailing_frame_reports_durable_prefix() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let wal_path = dir.path().join(WAL_FILENAME);
        let durable_len = std::fs::metadata(&wal_path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xAA, 0xBB, 0xCC]).unwrap();
        file.flush().unwrap();
        drop(file);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.durable_len, durable_len);
        assert_eq!(scan.records_checked, 1);
        assert_eq!(scan.file_len - scan.durable_len, 3);
        assert_recoverable_terminal(&scan, "partial WAL frame length");
    }

    #[test]
    fn scrub_wal_bad_header_is_corrupt() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(WAL_FILENAME), b"BADWAL!!").unwrap();

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.file_len, WAL_HEADER_SIZE as u64);
        assert_eq!(scan.bytes_checked, WAL_HEADER_SIZE as u64);
        assert_eq!(scan.durable_len, 0);
        assert_corrupt_terminal(&scan, "invalid WAL magic");
    }

    #[test]
    fn scrub_wal_crc_mismatch_is_recoverable_suffix() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.append(&make_test_node(2, "two"), 2).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let wal_path = dir.path().join(WAL_FILENAME);
        let mut data = std::fs::read(&wal_path).unwrap();
        let first_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let second_start = WAL_HEADER_SIZE + 8 + first_len;
        data[second_start + 4] ^= 0xFF;
        std::fs::write(&wal_path, data).unwrap();

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 1);
        assert_recoverable_terminal(&scan, "CRC mismatch");
    }

    #[test]
    fn scrub_wal_unknown_op_tag_after_durable_prefix_is_trailing() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let wal_path = dir.path().join(WAL_FILENAME);
        append_raw_walop_frame(&wal_path, 99, &[255u8, 0, 0, 0]);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 1);
        assert_recoverable_terminal(&scan, "unknown WAL operation tag");
    }

    #[test]
    fn scrub_wal_malformed_recognized_non_marker_op_is_corrupt() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let wal_path = dir.path().join(WAL_FILENAME);
        let mut walop_bytes = encode_wal_op(&make_test_node(2, "two")).unwrap();
        walop_bytes.push(0xFF);
        append_raw_walop_frame(&wal_path, 2, &walop_bytes);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 1);
        assert_corrupt_terminal(&scan, "failed to decode WAL record");
    }

    #[test]
    fn scrub_wal_malformed_recognized_non_marker_op_inside_batch_is_corrupt() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer
            .append(
                &WalOp::BeginAtomicBatch {
                    first_seq: 1,
                    op_count: 2,
                },
                1,
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let wal_path = dir.path().join(WAL_FILENAME);
        let mut walop_bytes = encode_wal_op(&make_test_node(1, "bad")).unwrap();
        walop_bytes.push(0xFF);
        append_raw_walop_frame(&wal_path, 1, &walop_bytes);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 1);
        assert_eq!(scan.durable_len, WAL_HEADER_SIZE as u64);
        assert_corrupt_terminal(&scan, "failed to decode WAL record");
    }

    #[test]
    fn scrub_wal_malformed_atomic_begin_is_trailing() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer
            .append(
                &WalOp::BeginAtomicBatch {
                    first_seq: 1,
                    op_count: 1,
                },
                1,
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 0);
        assert_eq!(scan.durable_len, WAL_HEADER_SIZE as u64);
        assert_recoverable_terminal(&scan, "malformed WAL atomic batch begin marker");
    }

    #[test]
    fn scrub_wal_incomplete_atomic_batch_is_not_durable() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer
            .append(
                &WalOp::BeginAtomicBatch {
                    first_seq: 1,
                    op_count: 2,
                },
                1,
            )
            .unwrap();
        writer.append(&make_test_node(1, "one"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let scan = scrub_generation_readonly(dir.path(), 0).unwrap();

        assert_eq!(scan.records_checked, 2);
        assert_eq!(scan.durable_len, WAL_HEADER_SIZE as u64);
        assert_recoverable_terminal(&scan, "incomplete WAL atomic batch");
    }

    #[test]
    fn scrub_wal_scanner_is_streaming_and_does_not_call_recovery_reader() {
        let source = include_str!("wal.rs");
        let scanner = source
            .split("pub(crate) fn scrub_generation_readonly")
            .nth(1)
            .unwrap()
            .split("fn read_exact_counted")
            .next()
            .unwrap();
        assert!(
            !scanner.contains("read_generation_recoverable"),
            "read-only WAL scrub scanner must not call recovery reader"
        );
        assert!(
            !scanner.contains("Vec<(u64, WalOp)>"),
            "read-only WAL scrub scanner must not retain decoded WAL records"
        );
    }

    #[test]
    fn test_wal_write_and_read_single() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        let op = make_test_node(1, "user:alice");
        writer.append(&op, 1).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1);

        let (seq, ref op) = records[0];
        assert_eq!(seq, 1);
        match op {
            WalOp::UpsertNode(node) => {
                assert_eq!(node.id, 1);
                assert_eq!(node.key, "user:alice");
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_wal_write_and_read_many() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        for i in 0..1000 {
            let op = make_test_node(i, &format!("node:{}", i));
            writer.append(&op, i + 1).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1000);

        // Verify first and last
        match &records[0] {
            (seq, WalOp::UpsertNode(node)) => {
                assert_eq!(*seq, 1);
                assert_eq!(node.id, 0);
            }
            _ => panic!("expected UpsertNode"),
        }
        match &records[999] {
            (seq, WalOp::UpsertNode(node)) => {
                assert_eq!(*seq, 1000);
                assert_eq!(node.id, 999);
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_wal_reader_rejects_older_wal_version() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(WAL_FILENAME);
        let mut file = File::create(&path).unwrap();
        file.write_all(&WAL_MAGIC).unwrap();
        file.write_all(&1u32.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reader = WalReader::new(dir.path());
        let err = reader.read_all().unwrap_err();
        match err {
            EngineError::CorruptWal(message) => {
                assert!(message.contains("unsupported WAL version"));
            }
            other => panic!("expected CorruptWal, got {}", other),
        }
    }

    #[test]
    fn test_wal_mixed_operations() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        writer.append(&make_test_node(1, "alice"), 1).unwrap();
        writer.append(&make_test_node(2, "bob"), 2).unwrap();
        writer.append(&make_test_edge(1, 1, 2), 3).unwrap();
        writer
            .append(
                &WalOp::DeleteNode {
                    id: 2,
                    deleted_at: 9999,
                },
                4,
            )
            .unwrap();
        writer
            .append(
                &WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: 9999,
                },
                5,
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 5);

        assert!(matches!(&ops[0], (_, WalOp::UpsertNode(_))));
        assert!(matches!(&ops[1], (_, WalOp::UpsertNode(_))));
        assert!(matches!(&ops[2], (_, WalOp::UpsertEdge(_))));
        assert!(matches!(&ops[3], (_, WalOp::DeleteNode { .. })));
        assert!(matches!(&ops[4], (_, WalOp::DeleteEdge { .. })));
    }

    #[test]
    fn test_wal_empty_file() {
        let dir = TempDir::new().unwrap();
        // No WAL file exists yet
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert!(ops.is_empty());
    }

    #[test]
    fn test_wal_empty_existing_file() {
        let dir = TempDir::new().unwrap();
        // Create an empty WAL file
        File::create(dir.path().join(WAL_FILENAME)).unwrap();
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert!(ops.is_empty());
    }

    #[test]
    fn test_wal_corrupt_tail_recovery() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        // Write 5 valid records
        for i in 0..5 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)), i + 1)
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        // Append garbage bytes to simulate a crash during write
        let wal_path = dir.path().join(WAL_FILENAME);
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD, 0xFC, 0xAA, 0xBB])
            .unwrap();
        file.flush().unwrap();
        drop(file);

        // Reader should recover the 5 valid records and skip the garbage
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 5);
    }

    #[test]
    fn test_wal_corrupt_crc_detection() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        // Write 3 valid records
        for i in 0..3 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)), i + 1)
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        // Corrupt a byte in the CRC of the second record
        let wal_path = dir.path().join(WAL_FILENAME);
        let mut data = std::fs::read(&wal_path).unwrap();

        // First record starts after the 8-byte header
        let first_len = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let second_record_start = WAL_HEADER_SIZE + 4 + 4 + first_len;

        // Corrupt the CRC of the second record (4 bytes after the length field)
        let crc_offset = second_record_start + 4;
        data[crc_offset] ^= 0xFF;

        std::fs::write(&wal_path, &data).unwrap();

        // Reader should recover only the first record
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn test_wal_append_returns_size() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();

        let delete_op = WalOp::DeleteNode {
            id: 1,
            deleted_at: 1000,
        };
        let size = writer.append(&delete_op, 1).unwrap();
        // Delete payload: 1 (op) + 8 (id) + 8 (deleted_at) = 17 bytes
        // Frame: 4 (len) + 4 (crc) + 8 (seq) + 17 (walop) = 33
        assert_eq!(size, 33);
        writer.flush().unwrap();
    }

    #[test]
    fn test_wal_append_batch_rejects_non_contiguous_sequences() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        let err = writer
            .append_batch(&[(1, make_test_node(1, "a")), (3, make_test_node(2, "b"))])
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
        writer.flush().unwrap();

        let reader = WalReader::new(dir.path());
        assert!(reader.read_all().unwrap().is_empty());
    }

    #[test]
    fn test_wal_reopen_writer_and_append() {
        let dir = TempDir::new().unwrap();

        // First session: write 3 records
        {
            let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
            for i in 0..3 {
                writer
                    .append(&make_test_node(i, &format!("s1:{}", i)), i + 1)
                    .unwrap();
            }
            writer.flush().unwrap();
        }

        // Second session: reopen and append 2 more
        {
            let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
            for i in 10..12 {
                writer
                    .append(&make_test_node(i, &format!("s2:{}", i)), i + 1)
                    .unwrap();
            }
            writer.flush().unwrap();
        }

        // Read all, should see 5 records total
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 5);
    }

    // --- WAL magic number validation ---

    #[test]
    fn test_wal_rejects_non_overgraph_file() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write random garbage that's not an OverGraph WAL
        std::fs::write(&wal_path, b"NOT_A_WAL_FILE_AT_ALL").unwrap();

        // Reader should return an error, not silently empty
        let reader = WalReader::new(dir.path());
        let result = reader.read_all();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("invalid WAL magic"));
    }

    #[test]
    fn test_wal_rejects_wrong_version() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write correct magic but wrong version
        let mut data = Vec::new();
        data.extend_from_slice(b"OVGR");
        data.extend_from_slice(&99u32.to_le_bytes());
        std::fs::write(&wal_path, &data).unwrap();

        let reader = WalReader::new(dir.path());
        let result = reader.read_all();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("unsupported WAL version"));
    }

    #[test]
    fn test_wal_writer_validates_existing_file() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write a non-OverGraph file
        std::fs::write(&wal_path, b"BADMAGIC").unwrap();

        // Writer should refuse to open
        let result = WalWriter::open_generation(dir.path(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_header_written_on_create() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Create a WAL via writer
        let writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        drop(writer);

        // Verify the file starts with the header
        let data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() >= WAL_HEADER_SIZE);
        assert_eq!(&data[..4], b"OVGR");
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(version, WAL_VERSION);
    }

    // --- Regression test for M5: decode error treated as crash boundary ---

    #[test]
    fn test_wal_decode_error_is_crash_boundary_not_hard_error() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write 3 valid records via writer (header auto-added)
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        for i in 0..3 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)), i + 1)
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        // Append a record with VALID CRC but UNPARSEABLE payload (unknown op tag)
        // V3 format: first 8 bytes are seq, rest is walop bytes
        let mut bogus_payload = Vec::new();
        bogus_payload.extend_from_slice(&99u64.to_le_bytes()); // seq
        bogus_payload.extend_from_slice(&[255u8, 0, 0, 0, 0, 0, 0, 0]); // unknown op tag
        let crc = crc32fast::hash(&bogus_payload);
        let len = bogus_payload.len() as u32;

        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.write_all(&bogus_payload).unwrap();
        file.flush().unwrap();
        drop(file);

        // Reader must recover the 3 valid records, NOT return an error
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_wal_malformed_recognized_record_is_hard_error() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "valid"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // V3 format: seq prefix + malformed walop bytes
        let walop_bytes = encode_wal_op(&make_test_node(2, "bad")).unwrap();
        let mut malformed_payload = Vec::new();
        malformed_payload.extend_from_slice(&99u64.to_le_bytes()); // seq
        malformed_payload.extend_from_slice(&walop_bytes);
        malformed_payload.push(0xFF); // trailing garbage makes it malformed
        let crc = crc32fast::hash(&malformed_payload);
        let len = malformed_payload.len() as u32;

        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.write_all(&malformed_payload).unwrap();
        file.flush().unwrap();
        drop(file);

        let reader = WalReader::new(dir.path());
        match reader.read_all() {
            Err(EngineError::CorruptWal(message)) => {
                assert!(message.contains("failed to decode WAL record"));
            }
            Ok(_) => panic!("expected malformed recognized WAL record to fail"),
            Err(other) => panic!("expected CorruptWal, got {}", other),
        }
    }

    fn assert_trailing_garbage_on_recognized_record_is_hard_error(op: WalOp) {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "valid"), 1).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let walop_bytes = encode_wal_op(&op).unwrap();
        let mut malformed_payload = Vec::new();
        malformed_payload.extend_from_slice(&99u64.to_le_bytes());
        malformed_payload.extend_from_slice(&walop_bytes);
        malformed_payload.push(0xFF);
        let crc = crc32fast::hash(&malformed_payload);
        let len = malformed_payload.len() as u32;

        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.write_all(&malformed_payload).unwrap();
        file.flush().unwrap();
        drop(file);

        let reader = WalReader::new(dir.path());
        match reader.read_all() {
            Err(EngineError::CorruptWal(message)) => {
                assert!(message.contains("failed to decode WAL record"));
            }
            Ok(_) => panic!("expected malformed recognized WAL record to fail"),
            Err(other) => panic!("expected CorruptWal, got {}", other),
        }
    }

    #[test]
    fn test_wal_malformed_upsert_edge_with_trailing_garbage_is_hard_error() {
        assert_trailing_garbage_on_recognized_record_is_hard_error(make_test_edge(2, 1, 2));
    }

    #[test]
    fn test_wal_malformed_delete_node_with_trailing_garbage_is_hard_error() {
        assert_trailing_garbage_on_recognized_record_is_hard_error(WalOp::DeleteNode {
            id: 2,
            deleted_at: 2_000,
        });
    }

    #[test]
    fn test_wal_malformed_delete_edge_with_trailing_garbage_is_hard_error() {
        assert_trailing_garbage_on_recognized_record_is_hard_error(WalOp::DeleteEdge {
            id: 3,
            deleted_at: 3_000,
        });
    }

    // --- Regression test for M1: off-by-one in length sanity check ---

    #[test]
    fn test_wal_truncated_by_one_byte_in_payload() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write 2 valid records via writer (header auto-added)
        let mut writer = WalWriter::open_generation(dir.path(), 0).unwrap();
        writer.append(&make_test_node(1, "first"), 1).unwrap();
        writer.append(&make_test_node(2, "second"), 2).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Append a partial record: valid len header claiming 20 bytes, but only write 19
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        let fake_len: u32 = 20;
        let fake_crc: u32 = 0;
        file.write_all(&fake_len.to_le_bytes()).unwrap();
        file.write_all(&fake_crc.to_le_bytes()).unwrap();
        file.write_all(&[0u8; 19]).unwrap(); // 1 byte short
        file.flush().unwrap();
        drop(file);

        // Should recover exactly the 2 valid records
        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 2);
    }
}

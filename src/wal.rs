use crate::encoding::{decode_wal_op, encode_wal_op_into};
use crate::error::EngineError;
use crate::types::WalOp;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const WAL_FILENAME: &str = "data.wal";
const WAL_MAGIC: [u8; 4] = *b"OVGR";
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: usize = 8; // WAL_MAGIC (4) + WAL_VERSION (4)
/// Maximum size for a single WAL record payload (64 MB).
const MAX_WAL_RECORD_SIZE: usize = 64 * 1024 * 1024;

/// WAL record frame: [len:u32][crc32:u32][payload:bytes]
/// - len: byte length of payload (not including len or crc fields)
/// - crc32: CRC-32 of payload bytes
/// - payload: encoded WalOp
fn write_wal_header(writer: &mut impl Write) -> Result<(), EngineError> {
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
    path: PathBuf,
    writer: BufWriter<File>,
    encode_buf: Vec<u8>,
}

impl WalWriter {
    /// Open or create a WAL file for appending.
    ///
    /// If the file is new or empty, writes the WAL header (magic + version).
    /// If the file already has data, validates the existing header.
    pub fn open(db_dir: &Path) -> Result<Self, EngineError> {
        let path = db_dir.join(WAL_FILENAME);

        // Open a single file handle for both validation and append writes.
        // read+create+append avoids a TOCTOU race between check and open.
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
            // Validate existing header using the same file handle
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
            path,
            writer,
            encode_buf: Vec::new(),
        })
    }

    /// Append a single WalOp to the WAL. Returns the byte size written.
    pub fn append(&mut self, op: &WalOp) -> Result<usize, EngineError> {
        encode_wal_op_into(op, &mut self.encode_buf)?;
        let len = self.encode_buf.len() as u32;
        let crc = crc32fast::hash(&self.encode_buf);

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&self.encode_buf)?;

        // 4 (len) + 4 (crc) + payload
        Ok(8 + self.encode_buf.len())
    }

    /// Append multiple WalOps as a single atomic buffer write.
    /// All ops are pre-encoded before any I/O, so encoding failures
    /// don't leave partial data in the write buffer.
    /// Returns total bytes written (framing + payload).
    pub fn append_batch(&mut self, ops: &[WalOp]) -> Result<usize, EngineError> {
        // Pre-encode all ops into a single contiguous buffer
        let mut batch_buf = Vec::new();
        for op in ops {
            encode_wal_op_into(op, &mut self.encode_buf)?;
            let len = self.encode_buf.len() as u32;
            let crc = crc32fast::hash(&self.encode_buf);

            batch_buf.extend_from_slice(&len.to_le_bytes());
            batch_buf.extend_from_slice(&crc.to_le_bytes());
            batch_buf.extend_from_slice(&self.encode_buf);
        }

        let total = batch_buf.len();
        // Single write_all for the entire batch
        self.writer.write_all(&batch_buf)?;
        Ok(total)
    }

    /// Flush the WAL to disk.
    pub fn flush(&mut self) -> Result<(), EngineError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Sync the WAL to disk (flush + fsync).
    pub fn sync(&mut self) -> Result<(), EngineError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Truncate the WAL file and re-write the header.
    /// Re-opens the append writer after truncation.
    pub fn truncate_and_reset(&mut self) -> Result<(), EngineError> {
        self.writer.flush()?;
        {
            // Use a dedicated truncate handle so Windows does not rely on
            // set_len() against an append-mode descriptor.
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.path)?;
            let mut writer = BufWriter::new(file);
            write_wal_header(&mut writer)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Re-open append writer for subsequent WAL appends.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Return the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Write-ahead log reader. Reads framed records with CRC validation.
pub struct WalReader {
    path: PathBuf,
}

impl WalReader {
    pub fn new(db_dir: &Path) -> Self {
        WalReader {
            path: db_dir.join(WAL_FILENAME),
        }
    }

    /// Read all valid records from the WAL. Stops at EOF or first corrupt/truncated
    /// record (which is treated as a crash boundary; the partial record is ignored).
    ///
    /// Validates the WAL header (magic + version) before reading records.
    /// Returns an error if the file is not a valid OverGraph WAL.
    pub fn read_all(&self) -> Result<Vec<WalOp>, EngineError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(Vec::new());
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

        loop {
            // Read length
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(EngineError::IoError(e)),
            }
            let payload_len = u32::from_le_bytes(len_buf) as usize;

            // Sanity check: reject zero-length or impossibly large records
            if payload_len == 0 || payload_len > MAX_WAL_RECORD_SIZE {
                break;
            }

            // Read CRC
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(_) => break, // truncated
            }
            let stored_crc = u32::from_le_bytes(crc_buf);

            // Read payload
            let mut payload = vec![0u8; payload_len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(_) => break, // truncated
            }

            // Validate CRC
            if crc32fast::hash(&payload) != stored_crc {
                // Corrupt record. Stop here (crash boundary)
                break;
            }

            // Decode the operation. Treat decode failure as crash boundary
            match decode_wal_op(&payload) {
                Ok(op) => ops.push(op),
                Err(_) => break,
            }
        }

        Ok(ops)
    }

    /// Returns true if the WAL file exists.
    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Return the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Delete the WAL file (used after successful flush/checkpoint).
pub fn remove_wal(db_dir: &Path) -> Result<(), EngineError> {
    let path = db_dir.join(WAL_FILENAME);
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

/// Truncate the WAL file and re-write the header (alternative to remove for reuse).
pub fn truncate_wal(db_dir: &Path) -> Result<(), EngineError> {
    let path = db_dir.join(WAL_FILENAME);
    if path.exists() {
        let file = OpenOptions::new().write(true).truncate(true).open(&path)?;
        let mut writer = BufWriter::new(&file);
        write_wal_header(&mut writer)?;
        writer.flush()?;
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
            type_id: 1,
            key: key.to_string(),
            props,
            created_at: 1000 * id as i64,
            updated_at: 1000 * id as i64 + 1,
            weight: 0.5,
        })
    }

    fn make_test_edge(id: u64, from: u64, to: u64) -> WalOp {
        WalOp::UpsertEdge(EdgeRecord {
            id,
            from,
            to,
            type_id: 10,
            props: BTreeMap::new(),
            created_at: 2000 * id as i64,
            updated_at: 2000 * id as i64 + 1,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
        })
    }

    #[test]
    fn test_wal_write_and_read_single() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        let op = make_test_node(1, "user:alice");
        writer.append(&op).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);

        match &ops[0] {
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
        let mut writer = WalWriter::open(dir.path()).unwrap();

        for i in 0..1000 {
            let op = make_test_node(i, &format!("node:{}", i));
            writer.append(&op).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1000);

        // Verify first and last
        match &ops[0] {
            WalOp::UpsertNode(node) => assert_eq!(node.id, 0),
            _ => panic!("expected UpsertNode"),
        }
        match &ops[999] {
            WalOp::UpsertNode(node) => assert_eq!(node.id, 999),
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_wal_mixed_operations() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        writer.append(&make_test_node(1, "alice")).unwrap();
        writer.append(&make_test_node(2, "bob")).unwrap();
        writer.append(&make_test_edge(1, 1, 2)).unwrap();
        writer
            .append(&WalOp::DeleteNode {
                id: 2,
                deleted_at: 9999,
            })
            .unwrap();
        writer
            .append(&WalOp::DeleteEdge {
                id: 1,
                deleted_at: 9999,
            })
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 5);

        assert!(matches!(&ops[0], WalOp::UpsertNode(_)));
        assert!(matches!(&ops[1], WalOp::UpsertNode(_)));
        assert!(matches!(&ops[2], WalOp::UpsertEdge(_)));
        assert!(matches!(&ops[3], WalOp::DeleteNode { .. }));
        assert!(matches!(&ops[4], WalOp::DeleteEdge { .. }));
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
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Write 5 valid records
        for i in 0..5 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)))
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
        let mut writer = WalWriter::open(dir.path()).unwrap();

        // Write 3 valid records
        for i in 0..3 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)))
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
    fn test_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer.append(&make_test_node(1, "test")).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Verify data exists
        let reader = WalReader::new(dir.path());
        assert_eq!(reader.read_all().unwrap().len(), 1);

        // Truncate (re-writes header)
        truncate_wal(dir.path()).unwrap();

        // Verify empty records but valid WAL
        let reader = WalReader::new(dir.path());
        assert!(reader.read_all().unwrap().is_empty());

        // Verify we can reopen a writer on the truncated file
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer.append(&make_test_node(2, "after_truncate")).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let reader = WalReader::new(dir.path());
        let ops = reader.read_all().unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            WalOp::UpsertNode(node) => assert_eq!(node.key, "after_truncate"),
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_wal_remove() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer.append(&make_test_node(1, "test")).unwrap();
        writer.flush().unwrap();
        drop(writer);

        assert!(dir.path().join(WAL_FILENAME).exists());
        remove_wal(dir.path()).unwrap();
        assert!(!dir.path().join(WAL_FILENAME).exists());
    }

    #[test]
    fn test_wal_append_returns_size() {
        let dir = TempDir::new().unwrap();
        let mut writer = WalWriter::open(dir.path()).unwrap();

        let delete_op = WalOp::DeleteNode {
            id: 1,
            deleted_at: 1000,
        };
        let size = writer.append(&delete_op).unwrap();
        // Delete payload: 1 (op) + 8 (id) + 8 (deleted_at) = 17 bytes
        // Frame: 4 (len) + 4 (crc) + 17 (payload) = 25
        assert_eq!(size, 25);
        writer.flush().unwrap();
    }

    #[test]
    fn test_wal_reopen_writer_and_append() {
        let dir = TempDir::new().unwrap();

        // First session: write 3 records
        {
            let mut writer = WalWriter::open(dir.path()).unwrap();
            for i in 0..3 {
                writer
                    .append(&make_test_node(i, &format!("s1:{}", i)))
                    .unwrap();
            }
            writer.flush().unwrap();
        }

        // Second session: reopen and append 2 more
        {
            let mut writer = WalWriter::open(dir.path()).unwrap();
            for i in 10..12 {
                writer
                    .append(&make_test_node(i, &format!("s2:{}", i)))
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
        let result = WalWriter::open(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_header_written_on_create() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Create a WAL via writer
        let writer = WalWriter::open(dir.path()).unwrap();
        drop(writer);

        // Verify the file starts with the header
        let data = std::fs::read(&wal_path).unwrap();
        assert!(data.len() >= WAL_HEADER_SIZE);
        assert_eq!(&data[..4], b"OVGR");
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        assert_eq!(version, 1);
    }

    // --- Regression test for M5: decode error treated as crash boundary ---

    #[test]
    fn test_wal_decode_error_is_crash_boundary_not_hard_error() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write 3 valid records via writer (header auto-added)
        let mut writer = WalWriter::open(dir.path()).unwrap();
        for i in 0..3 {
            writer
                .append(&make_test_node(i, &format!("n:{}", i)))
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        // Append a record with VALID CRC but UNPARSEABLE payload (unknown op tag)
        let bogus_payload = vec![255u8, 0, 0, 0, 0, 0, 0, 0];
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

    // --- Regression test for M1: off-by-one in length sanity check ---

    #[test]
    fn test_wal_truncated_by_one_byte_in_payload() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join(WAL_FILENAME);

        // Write 2 valid records via writer (header auto-added)
        let mut writer = WalWriter::open(dir.path()).unwrap();
        writer.append(&make_test_node(1, "first")).unwrap();
        writer.append(&make_test_node(2, "second")).unwrap();
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

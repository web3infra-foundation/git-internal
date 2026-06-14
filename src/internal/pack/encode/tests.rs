use std::{io::Cursor, path::PathBuf, sync::Arc, time::Instant};

use tempfile::tempdir;
use tokio::sync::Mutex;

use super::*;
use crate::{
    hash::{HashKind, ObjectHash, set_hash_kind_for_test},
    internal::{
        object::{blob::Blob, types::ObjectType},
        pack::{
            Pack,
            test_pack_download::{PackFileGuard, download_pack_file},
            tests::init_logger,
            utils::read_offset_encoding,
        },
    },
    time_it,
};

use super::header::encode_offset;

/// Check if the given data is a valid pack file format by attempting to decode it.
fn check_format(data: &Vec<u8>) {
    // Use a smaller cap on 32-bit targets to avoid usize overflow.
    let max_pack_size_u64 = if cfg!(target_pointer_width = "64") {
        6u64 * 1024 * 1024 * 1024
    } else {
        2u64 * 1024 * 1024 * 1024
    };
    let max_pack_size = usize::try_from(max_pack_size_u64).unwrap_or_else(|_| {
        panic!(
            "internal assertion failed: pack size cap {} does not fit in usize on this \
             target; this should be unreachable given the target_pointer_width configuration",
            max_pack_size_u64
        )
    });
    let mut p = Pack::new(
        None,
        Some(max_pack_size), // 6GB on 64-bit, 2GB on 32-bit
        Some(PathBuf::from("/tmp/.cache_temp")),
        true,
    );
    let mut reader = Cursor::new(data);
    tracing::debug!("start check format");
    p.decode(&mut reader, |_| {}, None::<fn(ObjectHash)>)
        .expect("pack file format error");
}

async fn get_entries_for_test() -> (Arc<Mutex<Vec<Entry>>>, PackFileGuard) {
    let (source, dl_guard) = download_pack_file("encode-test-sha1.pack");

    let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

    let f = std::fs::File::open(&source).unwrap();
    tracing::info!("pack file size: {}", f.metadata().unwrap().len());
    let mut reader = std::io::BufReader::new(f);
    let entries = Arc::new(Mutex::new(Vec::new()));
    let entries_clone = entries.clone();
    p.decode(
        &mut reader,
        move |entry| {
            let mut entries = entries_clone.blocking_lock();
            entries.push(entry.inner);
        },
        None::<fn(ObjectHash)>,
    )
    .unwrap();
    assert_eq!(p.number, entries.lock().await.len());
    tracing::info!("total entries: {}", p.number);
    drop(p);

    (entries, dl_guard)
}
async fn get_entries_for_test_sha256() -> (Arc<Mutex<Vec<Entry>>>, PackFileGuard) {
    let (source, dl_guard) = download_pack_file("encode-test-sha256.pack");

    let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

    let f = std::fs::File::open(&source).unwrap();
    tracing::info!("pack file size: {}", f.metadata().unwrap().len());
    let mut reader = std::io::BufReader::new(f);
    let entries = Arc::new(Mutex::new(Vec::new()));
    let entries_clone = entries.clone();
    p.decode(
        &mut reader,
        move |entry| {
            let mut entries = entries_clone.blocking_lock();
            entries.push(entry.inner);
        },
        None::<fn(ObjectHash)>,
    )
    .unwrap();
    assert_eq!(p.number, entries.lock().await.len());
    tracing::info!("total entries: {}", p.number);
    drop(p);

    (entries, dl_guard)
}

#[tokio::test]
async fn test_pack_encoder() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    async fn encode_once(window_size: usize) -> Vec<u8> {
        let (tx, mut rx) = mpsc::channel(100);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

        // make some different objects, or decode will fail
        let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
        let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
        encoder.encode_async(entry_rx).await.unwrap();

        for str in str_vec {
            let blob = Blob::from_content(str);
            let entry: Entry = blob.into();
            entry_tx
                .send(MetaAttached {
                    inner: entry,
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        // assert!(encoder.get_hash().is_some());
        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }
        result
    }

    // without delta
    let pack_without_delta = encode_once(0).await;
    let pack_without_delta_size = pack_without_delta.len();
    check_format(&pack_without_delta);

    // with delta
    let pack_with_delta = encode_once(4).await;
    assert!(pack_with_delta.len() <= pack_without_delta_size);
    check_format(&pack_with_delta);
}
#[tokio::test]
async fn test_pack_encoder_sha256() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);

    async fn encode_once(window_size: usize) -> Vec<u8> {
        let (tx, mut rx) = mpsc::channel(100);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

        let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
        let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
        encoder.encode_async(entry_rx).await.unwrap();

        for s in str_vec {
            let blob = Blob::from_content(s);
            let entry: Entry = blob.into();
            entry_tx
                .send(MetaAttached {
                    inner: entry,
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }
        result
    }

    // without delta
    let pack_without_delta = encode_once(0).await;
    let pack_without_delta_size = pack_without_delta.len();
    check_format(&pack_without_delta);

    // with delta
    let pack_with_delta = encode_once(4).await;
    assert!(pack_with_delta.len() <= pack_without_delta_size);
    check_format(&pack_with_delta);
}

#[tokio::test]
async fn test_pack_encoder_rejects_unencodable_ai_type_parallel() {
    let (tx, _rx) = mpsc::channel(8);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
    let mut encoder = PackEncoder::new(1, 0, tx);

    let mut entry: Entry = Blob::from_content("ai").into();
    entry.obj_type = ObjectType::Task;
    entry_tx
        .send(MetaAttached {
            inner: entry,
            meta: EntryMeta::new(),
        })
        .await
        .expect("send entry");
    drop(entry_tx);

    let err = encoder
        .encode(entry_rx)
        .await
        .expect_err("must reject AI pack type");
    assert!(matches!(err, GitError::PackEncodeError(_)));
}

#[tokio::test]
async fn test_pack_encoder_rejects_unencodable_ai_type_delta_window() {
    let (tx, _rx) = mpsc::channel(8);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
    let mut encoder = PackEncoder::new(1, 10, tx);

    let mut entry: Entry = Blob::from_content("ai").into();
    entry.obj_type = ObjectType::Task;
    entry_tx
        .send(MetaAttached {
            inner: entry,
            meta: EntryMeta::new(),
        })
        .await
        .expect("send entry");
    drop(entry_tx);

    let err = encoder
        .encode(entry_rx)
        .await
        .expect_err("must reject AI pack type");
    assert!(matches!(err, GitError::PackEncodeError(_)));
}

#[tokio::test]
async fn test_pack_encoder_parallel_large_file() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();

    let start = Instant::now();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    // encode entries with parallel
    let (tx, mut rx) = mpsc::channel(1_000_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

    let mut encoder = PackEncoder::new(entries_number, 0, tx);
    tokio::spawn(async move {
        time_it!("test parallel encode", {
            encoder.parallel_encode(entry_rx).await.unwrap();
        });
    });

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", result.len());
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    // check format
    check_format(&result);
}
#[tokio::test]
async fn test_pack_encoder_parallel_large_file_sha256() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);
    init_logger();

    let start = Instant::now();
    // use sha256 pack file for testing
    let (entries, _dl_guard) = get_entries_for_test_sha256().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let (tx, mut rx) = mpsc::channel(1_000_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

    let mut encoder = PackEncoder::new(entries_number, 0, tx);
    tokio::spawn(async move {
        time_it!("test parallel encode sha256", {
            encoder.parallel_encode(entry_rx).await.unwrap();
        });
    });

    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("sha256 test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", result.len());
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    check_format(&result);
}

#[tokio::test]
async fn test_pack_encoder_large_file() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();
    // encode entries
    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let mut encoder = PackEncoder::new(entries_number, 0, tx);
    tokio::spawn(async move {
        time_it!("test encode no parallel", {
            encoder.encode(entry_rx).await.unwrap();
        });
    });

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );
}
#[tokio::test]
async fn test_pack_encoder_large_file_sha256() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test_sha256().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();
    // encode entries
    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let mut encoder = PackEncoder::new(entries_number, 0, tx);
    tokio::spawn(async move {
        time_it!("test encode no parallel sha256", {
            encoder.encode(entry_rx).await.unwrap();
        });
    });

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );
}

#[tokio::test]
async fn test_pack_encoder_with_zstdelta() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();
    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let encoder = PackEncoder::new(entries_number, 10, tx);
    encoder
        .encode_async_with_zstdelta(entry_rx)
        .await
        .unwrap();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );

    // check format
    check_format(&result);
}
#[tokio::test]
async fn test_pack_encoder_with_zstdelta_sha256() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test_sha256().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();
    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let encoder = PackEncoder::new(entries_number, 10, tx);
    encoder
        .encode_async_with_zstdelta(entry_rx)
        .await
        .unwrap();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );

    // check format
    check_format(&result);
}

#[test]
fn test_encode_offset() {
    // let value = 11013;
    let value = 16389;

    let data = encode_offset(value);
    println!("{data:?}");
    let mut reader = Cursor::new(data);
    let (result, _) = read_offset_encoding(&mut reader).unwrap();
    println!("result: {result}");
    assert_eq!(result, value as u64);
}

#[tokio::test]
async fn test_pack_encoder_large_file_with_delta() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let encoder = PackEncoder::new(entries_number, 10, tx);

    let start = Instant::now();
    encoder.encode_async(entry_rx).await.unwrap();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );

    // check format
    check_format(&result);
}
#[tokio::test]
async fn test_pack_encoder_large_file_with_delta_sha256() {
    let _guard = set_hash_kind_for_test(HashKind::Sha256);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test_sha256().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let (tx, mut rx) = mpsc::channel(100_000);
    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

    let encoder = PackEncoder::new(entries_number, 10, tx);

    let start = Instant::now();
    encoder.encode_async(entry_rx).await.unwrap();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    let mut result = Vec::new();
    while let Some(chunk) = rx.recv().await {
        result.extend(chunk);
    }

    let pack_size = result.len();
    let compression_rate = if total_original_size > 0 {
        1.0 - (pack_size as f64 / total_original_size as f64)
    } else {
        0.0
    };

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("new pack file size: {}", pack_size);
    tracing::info!("original total size: {}", total_original_size);
    tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
    tracing::info!(
        "space saved: {} bytes",
        total_original_size.saturating_sub(pack_size)
    );

    // check format
    check_format(&result);
}

#[tokio::test]
async fn test_pack_encoder_output_to_files() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();

    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
    let dir = tempdir().unwrap();
    let path = dir.path();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 0)
        .await
        .unwrap();

    let mut pack_file = None;
    let mut idx_file = None;
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        tracing::info!("file name: {:?}", file_name);
        let file_name = file_name.to_string_lossy();
        if file_name.ends_with(".pack") {
            pack_file = Some(entry.path());
        } else if file_name.ends_with(".idx") {
            idx_file = Some(entry.path());
        }
    }
    let pack_file = pack_file.expect("pack file not generated");
    let idx_file = idx_file.expect("idx file not generated");
    assert!(
        pack_file.metadata().unwrap().len() > 0,
        "pack file is empty"
    );
    assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("original total size: {}", total_original_size);
}

#[tokio::test]
async fn test_pack_encoder_output_to_files_with_delta() {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    init_logger();
    let (entries, _dl_guard) = get_entries_for_test().await;
    let entries_number = entries.lock().await.len();

    let total_original_size: usize = entries
        .lock()
        .await
        .iter()
        .map(|entry| entry.data.len())
        .sum();

    let start = Instant::now();

    let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
    let dir = tempdir().unwrap();
    let path = dir.path();

    // spawn a task to send entries
    tokio::spawn(async move {
        let entries = entries.lock().await;
        for entry in entries.iter() {
            entry_tx
                .send(MetaAttached {
                    inner: entry.clone(),
                    meta: EntryMeta::new(),
                })
                .await
                .unwrap();
        }
        drop(entry_tx);
        tracing::info!("all entries sent");
    });

    encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 10)
        .await
        .unwrap();

    let mut pack_file = None;
    let mut idx_file = None;
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        tracing::info!("file name: {:?}", file_name);
        let file_name = file_name.to_string_lossy();
        if file_name.ends_with(".pack") {
            pack_file = Some(entry.path());
        } else if file_name.ends_with(".idx") {
            idx_file = Some(entry.path());
        }
    }
    let pack_file = pack_file.expect("pack file not generated");
    let idx_file = idx_file.expect("idx file not generated");
    assert!(
        pack_file.metadata().unwrap().len() > 0,
        "pack file is empty"
    );
    assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

    let duration = start.elapsed();
    tracing::info!("test executed in: {:.2?}", duration);
    tracing::info!("original total size: {}", total_original_size);
}

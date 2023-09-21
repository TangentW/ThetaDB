use std::{collections::HashMap, fs, io::Write};

use rand::Rng;
use thetadb::{ErrorCode, Result, ThetaDB, MAX_KEY_LEN, MAX_VALUE_LEN};

// Here are the highest level APIs tests.
// Some `mod`s also have their own tests inside.

#[test]
fn test_crud() -> Result<()> {
    test_db("test_crud.theta", |db| {
        let page_size = db.debugger()?.page_size()?;
        let key_value_pairs = obtain_key_value_pairs(500, MAX_KEY_LEN, page_size as usize);

        // Test `put` and `get`.
        for (key, _) in &key_value_pairs {
            assert!(!db.contains(key)?);
            assert_eq!(db.get(key)?, None);
        }
        for (key, value) in &key_value_pairs {
            db.put(key, value)?;
        }
        for (key, value) in &key_value_pairs {
            assert!(db.contains(key)?);
            assert_eq!(db.get(key)?.as_ref(), Some(value));
        }

        // Test `delete`.
        let deleted_pairs = &key_value_pairs[..200];
        let key_value_pairs = &key_value_pairs[200..];

        for (key, _) in deleted_pairs {
            db.delete(key)?;
        }
        for (key, value) in key_value_pairs {
            assert!(db.contains(key)?);
            assert_eq!(db.get(key)?.as_ref(), Some(value));
        }
        for (key, _) in deleted_pairs {
            assert!(!db.contains(key)?);
            assert_eq!(db.get(key)?.as_ref(), None);
        }

        // Test `put` after `delete`.
        let new_key_value_pairs = obtain_key_value_pairs(200, MAX_KEY_LEN, page_size as usize);
        for (key, value) in &new_key_value_pairs {
            db.put(key, value)?;
        }
        for (key, value) in &new_key_value_pairs {
            assert!(db.contains(key)?);
            assert_eq!(db.get(key)?.as_ref(), Some(value));
        }

        // Test `delete all`.
        for (key, _) in key_value_pairs {
            db.delete(key)?;
        }
        for (key, _) in &new_key_value_pairs {
            db.delete(key)?;
        }

        Ok(())
    })
}

#[test]
fn test_cursor() -> Result<()> {
    test_db("test_cursor.theta", |db| {
        let page_size = db.debugger()?.page_size()?;
        let mut key_value_pairs = obtain_key_value_pairs(500, MAX_KEY_LEN, page_size as usize);

        for (key, value) in &key_value_pairs {
            db.put(key, value)?;
        }

        let mut values = Vec::new();

        // Test forward traversal.
        let mut cursor = db.first_cursor()?;
        while let Some(value) = cursor.value()? {
            values.push(value);
            cursor.next()?;
        }

        key_value_pairs.sort_by(|l, r| l.0.cmp(&r.0));
        assert_eq!(
            values,
            key_value_pairs
                .iter()
                .map(|x| x.1.clone())
                .collect::<Vec<_>>()
        );
        values.clear();

        // Test backward traversal.
        let mut cursor = db.last_cursor()?;
        while let Some(value) = cursor.value()? {
            values.push(value);
            cursor.prev()?;
        }

        key_value_pairs.sort_by(|l, r| r.0.cmp(&l.0));
        assert_eq!(
            values,
            key_value_pairs
                .iter()
                .map(|x| x.1.clone())
                .collect::<Vec<_>>()
        );

        Ok(())
    })
}

#[test]
fn test_open_invalid_file() {
    let path = format!("target/invalid.theta");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    file.write(b"ABCD1234").unwrap();
    drop(file);

    assert_eq!(
        ThetaDB::open(&path).err().unwrap().code(),
        ErrorCode::FileUnexpected
    );
    _ = fs::remove_file(&path);
}

#[test]
fn test_put_large_key_value() -> Result<()> {
    test_db("test_error.theta", |db| {
        let key = vec![1; MAX_KEY_LEN];
        db.put(key, b"")?;

        let key = vec![1; MAX_KEY_LEN + 1];
        assert_eq!(
            db.put(key, b"").err().unwrap().code(),
            ErrorCode::InputInvalid
        );

        let value = vec![1; MAX_VALUE_LEN];
        db.put(b"key", value)?;

        let value = vec![1; MAX_VALUE_LEN + 1];
        assert_eq!(
            db.put(b"key", value).err().unwrap().code(),
            ErrorCode::InputInvalid
        );

        Ok(())
    })
}

fn test_db(name: &str, test: impl FnOnce(ThetaDB) -> Result<()>) -> Result<()> {
    let path = format!("target/{name}");
    let res = ThetaDB::open(&path).and_then(test);
    _ = fs::remove_file(&path);
    res
}

fn obtain_key_value_pairs(
    count: usize,
    key_max_len: usize,
    value_max_len: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|_| (rand_bytes(key_max_len), rand_bytes(value_max_len)))
        .collect::<HashMap<_, _>>()
        .into_iter()
        .collect()
}

fn rand_bytes(max_len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(0..=max_len);
    (0..len).map(|_| rand::random()).collect()
}

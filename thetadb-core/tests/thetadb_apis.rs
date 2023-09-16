use std::{collections::HashMap, fs};

use rand::Rng;
use thetadb::{Result, ThetaDB, MAX_KEY_LEN};

// Here are the highest level APIs tests.
// Some `mod`s also have their own tests inside.

#[test]
fn test_crud() -> Result<()> {
    test_db("test_crud.theta", |db| {
        let page_size = db.debugger()?.page_size()?;
        let key_value_pairs = obtain_key_value_pairs(500, MAX_KEY_LEN, page_size as usize);

        // Test `put` and `get`.
        for (key, value) in &key_value_pairs {
            db.put(key, value)?;
        }
        for (key, value) in &key_value_pairs {
            assert_eq!(db.get(key)?.as_ref(), Some(value));
        }

        // Test `delete`.
        let deleted_pairs = &key_value_pairs[..200];
        let key_value_pairs = &key_value_pairs[200..];

        for (key, _) in deleted_pairs {
            db.delete(key)?;
        }
        for (key, value) in key_value_pairs {
            assert_eq!(db.get(key)?.as_ref(), Some(value));
        }
        for (key, _) in deleted_pairs {
            assert_eq!(db.get(key)?.as_ref(), None);
        }

        // Test `put` after `delete`.
        let new_key_value_pairs = obtain_key_value_pairs(200, MAX_KEY_LEN, page_size as usize);
        for (key, value) in &new_key_value_pairs {
            db.put(key, value)?;
        }
        for (key, value) in &new_key_value_pairs {
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
        let key_value_pairs = obtain_key_value_pairs(500, MAX_KEY_LEN, page_size as usize);

        for (key, value) in &key_value_pairs {
            db.put(key, value)?;
        }

        let mut values = Vec::new();
        let mut cursor = db.first_cursor()?;
        while let Some(value) = cursor.value()? {
            values.push(value);
            cursor.next()?;
        }

        let mut key_value_pairs = key_value_pairs.into_iter().collect::<Vec<_>>();
        key_value_pairs.sort_by(|l, r| l.0.cmp(&r.0));
        let sorted_values = key_value_pairs.into_iter().map(|x| x.1).collect::<Vec<_>>();

        assert_eq!(values, sorted_values);
        Ok(())
    })
}

fn test_db(name: &str, test: impl FnOnce(ThetaDB) -> Result<()>) -> Result<()> {
    let path = format!("target/{name}");
    ThetaDB::open(&path).and_then(test)?;
    _ = fs::remove_file(&path);
    Ok(())
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

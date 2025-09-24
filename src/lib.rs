pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod test_utils {
    pub mod load_test_url;
}

pub mod db_utils {
    pub mod kv_db_local;
    pub mod kv_cache;
}

pub mod web2_utils {
    pub mod graphql_helper;
    pub mod curl_helper;
}

pub use crate::db_utils::kv_cache::cache_result;
pub use crate::test_utils::load_test_url;
pub use crate::db_utils::kv_db_local;
pub use crate::db_utils::kv_cache;

// Note: The kv_cache! macro is automatically available at crate root via #[macro_export]
// No need to re-export it here

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

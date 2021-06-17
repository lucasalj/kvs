use super::Result;

/// Models a key-value database engine with a very simplified interface
pub trait KvsEngine {
    /// Set the `value` of a string `key` to a string.
    /// Return an error if the `value` is not written successfully.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kvs::KvStore;
    /// use kvs::Result;
    ///
    /// let user_data = KvStore::open("./");
    /// assert!(user_data.is_ok());
    /// let mut user_data = user_data.unwrap();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// user_data.set("age".to_owned(), "21".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()).unwrap(), Some("John".to_owned()));
    ///
    /// user_data.set("age".to_owned(), "22".to_owned());
    /// assert_eq!(user_data.get("age".to_owned()).unwrap(), Some("22".to_owned()));
    /// ```
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a string `key`.
    /// If the `key` does not exist, return `None`.
    /// Return an error if the value is not read successfully.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::open("./").unwrap();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// user_data.set("age".to_owned(), "21".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()).unwrap(), Some("John".to_owned()));
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given `key`.
    /// Return an error if the `key` does not exist or is not removed successfully.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::open("./").unwrap();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()).unwrap(), Some("John".to_owned()));
    ///
    /// user_data.remove("name".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()).unwrap(), None);
    /// ```
    fn remove(&mut self, key: String) -> Result<()>;
}

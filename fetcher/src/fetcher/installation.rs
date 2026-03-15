use std::fmt;
use std::path::PathBuf;

use crate::BuildInfo;

/// Details of an installed browser.
#[derive(Clone, Debug)]
pub struct BrowserFetcherInstallation {
    pub folder_path: PathBuf,
    pub executable_path: PathBuf,
    pub build_info: BuildInfo,
}

impl fmt::Display for BrowserFetcherInstallation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, Path: {}",
            self.build_info,
            self.executable_path.display()
        )
    }
}

use std::{fmt, str::FromStr};

use super::{Result, VersionError};
use crate::{BrowserHost, BrowserKind, BuildInfo};

/// Represents a specific Chrome version (e.g. "133.0.6943.126").
///
/// Only supported for Chrome and ChromeHeadlessShell.
#[derive(Clone, Copy, Debug)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub build: u32,
    pub patch: u32,
}

impl Version {
    pub const fn new(major: u32, minor: u32, build: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            build,
            patch,
        }
    }

    pub(crate) async fn resolve(
        &self,
        kind: BrowserKind,
        _host: &BrowserHost,
    ) -> Result<BuildInfo> {
        match kind {
            BrowserKind::Chromium => Err(VersionError::ResolveFailed(anyhow::anyhow!(
                "Version strings are not supported for Chromium, use Revision instead"
            ))),
            BrowserKind::Chrome | BrowserKind::ChromeHeadlessShell => {
                Ok(BuildInfo::version(self.to_string()))
            }
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}",
            self.major, self.minor, self.build, self.patch
        )
    }
}

impl FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return Err(VersionError::InvalidVersion(s.to_string()));
        }
        let parse = |part: &str| {
            part.parse::<u32>()
                .map_err(|_| VersionError::InvalidVersion(s.to_string()))
        };
        Ok(Version {
            major: parse(parts[0])?,
            minor: parse(parts[1])?,
            build: parse(parts[2])?,
            patch: parse(parts[3])?,
        })
    }
}

impl TryFrom<String> for Version {
    type Error = VersionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

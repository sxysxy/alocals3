#[cfg(feature = "server-binary")]
pub fn server_build_marker() {}

#[cfg(all(not(feature = "server-binary"), feature = "extension-module"))]
include!("python_lib.rs");

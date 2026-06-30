#[cfg(feature = "server-binary")]
pub fn server_build_marker() {}

#[cfg(not(feature = "server-binary"))]
include!("python_lib.rs");

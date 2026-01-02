fn main() {
    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let default_path = manifest_dir.join("../../..").join("percona-parser/build");
    let fallback_path = manifest_dir.join("../../percona-parser/build");
    let lib_path = std::env::var("IBD_READER_LIB_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            if default_path.exists() {
                default_path
            } else {
                fallback_path
            }
        });

    let lib_found = lib_path.join("libibd_reader.so").exists()
        || lib_path.join("libibd_reader.dylib").exists()
        || lib_path.join("ibd_reader.dll").exists();

    if lib_found && std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("unix") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
    }

    println!("cargo:rerun-if-env-changed=IBD_READER_LIB_PATH");
}

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ibd_reader_available)");

    // Link to libibd_reader.so from percona-parser
    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let lib_path = std::env::var("IBD_READER_LIB_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| manifest_dir.join("../../percona-parser/build"));

    let lib_found = lib_path.join("libibd_reader.so").exists()
        || lib_path.join("libibd_reader.dylib").exists()
        || lib_path.join("ibd_reader.dll").exists();

    if lib_found {
        println!("cargo:rustc-link-search=native={}", lib_path.display());
        println!("cargo:rustc-link-lib=dylib=ibd_reader");
        println!("cargo:rustc-cfg=ibd_reader_available");
    } else {
        println!(
            "cargo:warning=IBD reader library path not found: {}",
            lib_path.display()
        );
    }

    // Re-run if environment variable changes
    println!("cargo:rerun-if-env-changed=IBD_READER_LIB_PATH");
}

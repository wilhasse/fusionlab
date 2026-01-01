fn main() {
    // Link to libibd_reader.so from percona-parser
    let lib_path = std::env::var("IBD_READER_LIB_PATH")
        .unwrap_or_else(|_| "/home/cslog/mysql/percona-parser/build".to_string());

    println!("cargo:rustc-link-search=native={}", lib_path);
    println!("cargo:rustc-link-lib=dylib=ibd_reader");

    // Re-run if environment variable changes
    println!("cargo:rerun-if-env-changed=IBD_READER_LIB_PATH");
}

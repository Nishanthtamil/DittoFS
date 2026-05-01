#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;
    use std::process::Command;

    #[test]
    #[ignore]
    fn benchmark_performance() {
        let mount_point = "/tmp/ditto_bench";
        let db_path = "/tmp/db_bench";
        let _ = fs::remove_dir_all(mount_point);
        let _ = fs::remove_dir_all(db_path);
        fs::create_dir_all(mount_point).unwrap();

        // Start dittofs in background
        let mut child = Command::new("./target/release/dittofs")
            .arg("--mount")
            .arg(mount_point)
            .arg("--db")
            .arg(db_path)
            .spawn()
            .expect("failed to start dittofs");

        // Wait for mount
        std::thread::sleep(std::time::Duration::from_secs(5));

        let num_files = 1000;
        let file_size = 1024 * 10; // 10KB each

        println!("Benchmarking creation of {} files...", num_files);
        let start = Instant::now();
        for i in 0..num_files {
            let path = format!("{}/file_{}.txt", mount_point, i);
            fs::write(path, vec![0u8; file_size]).unwrap();
        }
        let duration = start.elapsed();
        println!("Write took: {:?}", duration);

        println!("Benchmarking reading of {} files...", num_files);
        let start = Instant::now();
        for i in 0..num_files {
            let path = format!("{}/file_{}.txt", mount_point, i);
            let data = fs::read(path).unwrap();
            assert_eq!(data.len(), file_size);
        }
        let duration = start.elapsed();
        println!("Read took: {:?}", duration);

        // Cleanup
        let _ = Command::new("fusermount").arg("-uz").arg(mount_point).status();
        let _ = child.kill();
    }
}

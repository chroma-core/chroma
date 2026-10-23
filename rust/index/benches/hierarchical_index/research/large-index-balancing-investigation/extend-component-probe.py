from pathlib import Path
root=Path('/home/ubuntu/chroma-large-index-balancing')
p=root/'rust/index/benches/hierarchical_index/writer/persistence.rs';s=p.read_text()
s=s.replace('        let started = std::time::Instant::now();','''        println!("[component-probe] io_before {}", std::fs::read_to_string("/proc/self/io").unwrap().replace('\\n', " "));
        let started = std::time::Instant::now();''',1)
s=s.replace('        println!("[component-probe] finished elapsed_s={:.6}", started.elapsed().as_secs_f64());','''        println!("[component-probe] finished elapsed_s={:.6}", started.elapsed().as_secs_f64());
        println!("[component-probe] io_after {}", std::fs::read_to_string("/proc/self/io").unwrap().replace('\\n', " "));''',1)
s=s.replace('            reader.clear_loaded_blocks();\n            for result in results {','''            if std::env::var_os("CHROMA_COMPONENT_KEEP_SCALAR_PINS").is_none() {
                reader.clear_loaded_blocks();
            }
            for result in results {''',1)
p.write_text(s)

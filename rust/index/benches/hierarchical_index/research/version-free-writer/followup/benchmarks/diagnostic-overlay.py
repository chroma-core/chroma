from pathlib import Path
import difflib
r=Path('/mnt/msmarco/version-free-followup-20261020');p=r/'diagnostic/rust/index/benches/hierarchical_spann_profile_quantized.rs';before=p.read_text();after=before
anchor='''            build_wall.enter(Phase::Balance);
            progress.suspend(|| {'''
replacement='''            println!("BENCHMARK_BALANCE_ENTER unix={:.6} elapsed={:.6} checkpoint={} batch_vectors={}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(), total_start.elapsed().as_secs_f64(), checkpoint_idx + 1, batch.len());
            flush_stdout();
            build_wall.enter(Phase::Balance);
            progress.suspend(|| {'''
assert after.count(anchor)==1;after=after.replace(anchor,replacement)
anchor='''                writer.balance_index_parallel(args.balance_threads.unwrap_or(args.threads))
            })?;
            build_wall.enter(Phase::Outer);'''
replacement=anchor+'''
            println!("BENCHMARK_BALANCE_EXIT unix={:.6} elapsed={:.6} checkpoint={}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(), total_start.elapsed().as_secs_f64(), checkpoint_idx + 1);
            flush_stdout();'''
assert after.count(anchor)==1;after=after.replace(anchor,replacement)
anchor='''        if let Some(snapshots) = &args.checkpoint_snapshots_dir {'''
assert after.count(anchor)==1;after=after.replace(anchor,'''        if let Some(snapshots) = args.checkpoint_snapshots_dir.as_ref().filter(|_| total_vectors == 8_000_000) {''')
p.write_text(after);(r/'artifacts/diagnostic-only.patch').write_text(''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/rust/index/benches/hierarchical_spann_profile_quantized.rs',tofile='b/rust/index/benches/hierarchical_spann_profile_quantized.rs')))

use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;
use std::time::Instant;
pub(crate) trait Runnable {
    fn run(&self);
    fn box_clone(&self) -> Box<dyn Runnable + Send + Sync>;
}

impl Clone for Box<dyn Runnable + Send + Sync> {
    fn clone(&self) -> Box<dyn Runnable + Send + Sync> {
        self.box_clone()
    }
}

#[derive(Clone)]
pub(crate) struct Runtime {
    runable: Box<dyn Runnable + Send + Sync>,
    running: Arc<RwLock<bool>>,
    interval: Option<Duration>,
}

impl Runtime {
    pub(crate) fn new(
        runable: Box<dyn Runnable + Send + Sync>,
        interval: Option<Duration>,
    ) -> Runtime {
        Runtime {
            runable: runable,
            running: Arc::new(RwLock::new(true)),
            interval: interval,
        }
    }

    pub(crate) fn execute(&self) -> RuntimeHandle {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runnable = self.runable.box_clone();
        let running = self.running.clone();
        let interval = self.interval;
        let join_handle = thread::spawn(move || {
            rt.block_on(async move {
                let should_sleep = match interval {
                    Some(_interval) => true,
                    None => false,
                };
                let mut next_time;
                if should_sleep {
                    next_time = Instant::now() + interval.unwrap();
                } else {
                    next_time = Instant::now();
                }
                loop {
                    if !*running.read().unwrap() {
                        break;
                    }
                    runnable.run();
                    if should_sleep {
                        thread::sleep(next_time - Instant::now());
                        next_time += interval.unwrap();
                    }
                }
            });
        });
        RuntimeHandle {
            join_handle: join_handle,
            running: self.running.clone(),
        }
    }

    pub(crate) fn shutdown(&self) {
        let mut running = self.running.write().unwrap();
        *running = false;
    }
}

pub(crate) struct RuntimeHandle {
    pub(crate) join_handle: thread::JoinHandle<()>,
    pub(crate) running: Arc<RwLock<bool>>,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::sync::RwLock;

    #[derive(Clone)]
    struct TestRunnable {
        pub(crate) counter: Arc<RwLock<i32>>,
    }

    impl Runnable for TestRunnable {
        fn run(&self) {
            let mut counter = self.counter.write().unwrap();
            *counter += 1;
        }

        fn box_clone(&self) -> Box<dyn Runnable + Send + Sync> {
            Box::new((*self).clone())
        }
    }

    #[test]
    fn test_runtime() {
        let counter = Arc::new(RwLock::new(0));
        let runnable = TestRunnable {
            counter: counter.clone(),
        };
        let runtime = Runtime {
            running: Arc::new(RwLock::new(true)),
            runable: Box::new(runnable),
            interval: None,
        };
        let runtime_clone = runtime.clone();
        thread::spawn(move || {
            let handle = runtime.execute();
            handle.join_handle.join().unwrap();
        });
        thread::sleep(Duration::from_millis(100));
        runtime_clone.shutdown();
    }
}

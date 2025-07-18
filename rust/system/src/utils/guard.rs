pub struct CleanupGuard<F>
where
    F: FnMut(),
{
    cleanup_fn: F,
}

impl<F> CleanupGuard<F>
where
    F: FnMut(),
{
    pub fn new(cleanup_fn: F) -> Self {
        Self { cleanup_fn }
    }
}

impl<F> Drop for CleanupGuard<F>
where
    F: FnMut(),
{
    fn drop(&mut self) {
        (self.cleanup_fn)();
    }
}

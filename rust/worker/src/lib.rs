// Export index as a module
pub mod index;
pub mod memberlist_provider;

pub trait Component {
    fn start(&self);
    fn stop(&self);
}

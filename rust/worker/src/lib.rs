// Export index as a module
pub mod index;
pub mod memberlist_provider;
mod rendezvous_hash;

pub trait Component {
    fn start(&self);
    fn stop(&self);
}

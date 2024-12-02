use std::sync::Mutex;

use crate::{Error, StreamID};

/////////////////////////////////////////// StreamManager //////////////////////////////////////////

#[derive(Debug, Default)]
pub struct StreamManager {
    streams: Mutex<Vec<StreamID>>,
}

impl StreamManager {
    /// Return a list of streams this log currently allows for writing.
    pub fn streams(&self) -> Vec<StreamID> {
        // SAFETY(rescrv):  Mutex poisoning.
        self.streams.lock().unwrap().clone()
    }

    /// Open a new stream in the log.
    pub fn open_stream(&self, stream_id: StreamID) -> Result<(), Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut streams = self.streams.lock().unwrap();
        if streams.contains(&stream_id) {
            Err(Error::AlreadyOpen)
        } else {
            streams.push(stream_id);
            Ok(())
        }
    }

    /// Close a stream in the log.
    pub fn close_stream(&self, stream_id: StreamID) -> Result<(), Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut streams = self.streams.lock().unwrap();
        if !streams.contains(&stream_id) {
            Err(Error::ClosedStream)
        } else {
            streams.retain(|s| *s != stream_id);
            Ok(())
        }
    }

    /// True if the stream is open for this log.
    pub fn stream_is_open(&self, stream_id: StreamID) -> bool {
        let streams = self.streams.lock().unwrap();
        streams.contains(&stream_id)
    }
}

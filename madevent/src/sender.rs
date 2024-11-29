use serde::Serialize;

pub struct Sender;

impl Sender {
    pub fn event(self, _name: impl Into<String>, _data: impl Serialize) -> Self {
        todo!()
    }

    pub fn event_with_metadata(
        self,
        _name: impl Into<String>,
        _data: impl Serialize,
        _metadata: impl Serialize,
    ) -> Self {
        todo!()
    }

    pub async fn send(&self) -> Vec<Event> {
        todo!()
    }
}

//struct SenderEvent;
#[derive(Debug, PartialEq)]
pub struct Event;

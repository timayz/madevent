use serde::Serialize;

pub struct Sender;

impl Sender {
    pub fn original_version(self, _original_version: u16) -> Self {
        todo!()
    }

    pub fn event<D>(self, _data: &D) -> Self
    where
        D: ?Sized + Serialize,
    {
        todo!()
    }

    pub fn event_with_metadata<D, M>(self, _data: impl Serialize, _metadata: impl Serialize) -> Self
    where
        D: ?Sized + Serialize,
        M: ?Sized + Serialize,
    {
        todo!()
    }

    pub async fn send(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub enum SenderError {
    InvalidOriginalVersion,
}

pub type Result<E> = std::result::Result<E, SenderError>;

//struct SenderEvent;
#[derive(Debug, PartialEq)]
pub struct Event;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Created {
        pub name: String,
    }

    #[derive(Serialize)]
    struct Deleted {
        pub deleted: bool,
    }

    #[derive(Serialize)]
    struct Edited {
        pub name: String,
        pub description: String,
        pub category: String,
        pub visible: bool,
        pub stock: i32,
        pub price: f32,
    }

    #[derive(Serialize)]
    struct VisibilityChanged {
        pub visible: bool,
    }

    #[derive(Serialize)]
    struct ThumbnailChanged {
        pub thumbnail: String,
    }

    #[tokio::test]
    async fn send() {
        let res = Sender
            .original_version(0)
            .event(&Created {
                name: "Product 1".to_owned(),
            })
            .send()
            .await;

        assert!(res.is_ok());

        let res = Sender
            .original_version(1)
            .event(&VisibilityChanged { visible: false })
            .event(&ThumbnailChanged {
                thumbnail: "product_1.png".to_owned(),
            })
            .send()
            .await;

        assert!(res.is_ok());

        let res = Sender
            .original_version(3)
            .event(&Edited {
                name: "Kit Ring Alarm XL".to_owned(),
                description:
                    "Connected wireless home alarm, security system with assisted monitoring"
                        .to_owned(),
                category: "ring".to_owned(),
                visible: true,
                stock: 100,
                price: 309.99,
            })
            .send()
            .await;

        assert!(res.is_ok());

        let res = Sender
            .original_version(4)
            .event(&Deleted { deleted: true })
            .send()
            .await;

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn concurrency() {}

    #[tokio::test]
    async fn invalid_original_version() {
        let res = Sender
            .original_version(0)
            .event(&Created {
                name: "Product 1".to_owned(),
            })
            .send()
            .await;

        assert!(res.is_ok());

        let err = Sender
            .original_version(0)
            .event(&VisibilityChanged { visible: false })
            .send()
            .await
            .unwrap_err();

        assert_eq!(err, SenderError::InvalidOriginalVersion);

        let err = Sender
            .original_version(2)
            .event(&VisibilityChanged { visible: false })
            .send()
            .await
            .unwrap_err();

        assert_eq!(err, SenderError::InvalidOriginalVersion);

        let res = Sender
            .original_version(1)
            .event(&Deleted { deleted: true })
            .send()
            .await;

        assert!(res.is_ok());
    }
}

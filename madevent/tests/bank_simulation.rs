use madevent::Event;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

#[tokio::test]
async fn read() {
    let mut acc = Account::default();
    acc.apply(Event {
        id: Default::default(),
        name: Default::default(),
        aggregate: Default::default(),
        version: 0,
        data: Default::default(),
        metadata: None,
        timestamp: 0,
    });
    assert_eq!(acc.balance, 0.0);
}

#[derive(Debug)]
pub enum MoneyTransactionState {
    New,
    Pending,
    Succeeded,
    Cancelled,
}

#[derive(Debug)]
pub enum MoneyTransactionType {
    Incoming,
    Outgoing,
}

#[derive(Debug)]
pub struct MoneyTransaction {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
    pub state: MoneyTransactionState,
    pub transaction_type: MoneyTransactionType,
    pub created_at: u32,
    pub updated_at: u32,
}

impl Default for MoneyTransaction {
    fn default() -> Self {
        Self {
            transaction_id: Default::default(),
            from_id: Default::default(),
            to_id: Default::default(),
            value: 0.0,
            state: MoneyTransactionState::New,
            transaction_type: MoneyTransactionType::Incoming,
            created_at: 0,
            updated_at: 0,
        }
    }
}

#[derive(Debug, Default)]
struct Account {
    pub id: Ulid,
    pub fullname: String,
    pub balance: f32,
    pub transaction_to_reserved_balance: HashMap<Ulid, f32>,
    pub transactions: HashMap<Ulid, MoneyTransaction>,
    pub created_at: u32,
    pub updated_at: u32,
}

impl Account {
    fn apply(&mut self, event: Event) {
        if let Some(data) = event.to_data::<AccountCreated>().unwrap() {
            let id = event.aggregate.split('/').last().unwrap();
            self.id = Ulid::from_string(id).unwrap();
            self.fullname = data.fullname;
            self.created_at = event.timestamp;
            self.updated_at = event.timestamp;

            return;
        }

        if let Some(data) = event.to_data::<FullNameChanged>().unwrap() {
            self.fullname = data.fullname;
            self.updated_at = event.timestamp;

            return;
        }

        if let Some(data) = event.to_data::<MoneyTransferred>().unwrap() {
            self.updated_at = event.timestamp;

            let transaction_type = if self.id == data.from_id {
                MoneyTransactionType::Outgoing
            } else {
                MoneyTransactionType::Incoming
            };
            let value = if self.id == data.from_id {
                data.value * -1.0
            } else {
                data.value
            };

            self.transactions.insert(
                data.transaction_id,
                MoneyTransaction {
                    transaction_id: data.transaction_id,
                    from_id: data.from_id,
                    to_id: data.to_id,
                    value,
                    state: MoneyTransactionState::New,
                    transaction_type,
                    created_at: event.timestamp,
                    updated_at: event.timestamp,
                },
            );

            return;
        }

        if let Some(data) = event.to_data::<AccountDebited>().unwrap() {
            self.updated_at = event.timestamp;
            self.balance -= data.value;
            self.transaction_to_reserved_balance
                .insert(data.transaction_id, data.value * -1.0);

            if let Some(transaction) = self.transactions.get_mut(&data.transaction_id) {
                transaction.state = MoneyTransactionState::Pending;
                transaction.updated_at = event.timestamp;
            }
        }

        if let Some(data) = event.to_data::<AccountCredited>().unwrap() {
            self.updated_at = event.timestamp;
            self.transaction_to_reserved_balance
                .insert(data.transaction_id, data.value);

            if let Some(transaction) = self.transactions.get_mut(&data.transaction_id) {
                transaction.state = MoneyTransactionState::Pending;
                transaction.updated_at = event.timestamp;
            }
        }

        if let Some(data) = event.to_data::<MoneyTransferSucceeded>().unwrap() {
            self.updated_at = event.timestamp;

            if let Some(transaction) = self.transactions.get_mut(&data.transaction_id) {
                transaction.state = MoneyTransactionState::Succeeded;
                transaction.updated_at = event.timestamp;
            }

            if let Some(ttrb) = self
                .transaction_to_reserved_balance
                .remove(&data.transaction_id)
            {
                if data.to_id == self.id {
                    self.balance += ttrb
                }
            }
        }

        if let Some(data) = event.to_data::<MoneyTransferCancelled>().unwrap() {
            self.updated_at = event.timestamp;
            if data.to_id == self.id {
                self.transaction_to_reserved_balance
                    .remove(&data.transaction_id);
            } else if let Some(ttrb) = self
                .transaction_to_reserved_balance
                .get(&data.transaction_id)
            {
                self.balance += ttrb * -1.0;
                self.transaction_to_reserved_balance
                    .remove(&data.transaction_id);
            }

            if let Some(transaction) = self.transactions.get_mut(&data.transaction_id) {
                transaction.state = MoneyTransactionState::Cancelled;
                transaction.updated_at = event.timestamp;
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Reason {
    BalanceTooLow,
    InternalServerError,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AccountCreated {
    pub fullname: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AccountCredited {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AccountDebited {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FullNameChanged {
    pub fullname: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MoneyTransferCancelled {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
    pub reason: Reason,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MoneyTransferSucceeded {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct MoneyTransferred {
    pub transaction_id: Ulid,
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub value: f32,
}

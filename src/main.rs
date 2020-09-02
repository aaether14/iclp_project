use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::prelude::*;

use serde_json::json;

use anyhow::Context;

use std::net::SocketAddr;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

#[derive(Debug)]
enum Command {
    CreateAccount(String, String),
    Login(String, String),
    Unparsable(String),
    SendMessage(Vec<String>, String),
    ReadMessage(usize),
    ReadMailbox,
    Logout,
    Exit
}

impl Command {
    fn parse(data: &str) -> Command {
        match data.trim().split(|x| x == ' ').collect::<Vec<_>>().as_slice() {
            &["CREATE_ACCOUNT", username, password] => 
                Command::CreateAccount(username.to_string(), password.to_string()),
            &["LOGIN", username, password] => 
                Command::Login(username.to_string(), password.to_string()), 
            &["LOGOUT"] => Command::Logout,
            &["SEND", ref recipients @ .. , message] => 
                Command::SendMessage(recipients.iter().map(|x| x.to_string()).collect(), message.to_string()),
            &["READ_MSG", id] => id.parse().map_or(Command::Unparsable(data.to_string()), 
                |x| Command::ReadMessage(x)),
            &["READ_MAILBOX"] => Command::ReadMailbox,
            _ => Command::Unparsable(data.to_string())
        }
    }
}

#[derive(Debug)]
enum Message {
    Success,
    ForceLogout,
    Message(String, String), // from, content
    Mailbox(Vec<usize>),
    NewMessageInMailbox(usize),
    Error(String),
    ClientClosed(SocketAddr),
    NewClient(SocketAddr, mpsc::Sender<Message>),
    Command(SocketAddr, Command)
}

impl Message {
    fn to_json_string(&self) -> String {
        let value = match self {
            Message::Success => json!({
                "type": "success"
            }),
            Message::ForceLogout => json!({
                "type": "force_logout"
            }),
            Message::NewMessageInMailbox(id) => json!({
                "type": "new_message_in_mail_box",
                "id": id
            }),
            Message::Message(from, content) => json!({
                "type": "message",
                "from": from,
                "content": content
            }),
            Message::Mailbox(ids) => json!({
                "type": "mailbox",
                "ids": ids
            }),
            Message::Error(content) => json!({
                "type": "error",
                "content": content
            }),
            rest => json!({
                "type": "unserializable", 
                "content": format!("{:?}", rest)
            })
        };
        value.to_string()
    }
}

struct CommandParser {
    buffer: Vec<u8>
}

impl CommandParser {
    async fn read_and_parse<'a>(&mut self, socket_reader: &mut ReadHalf<'a>) -> anyhow::Result<Vec<Command>> {
        let read = socket_reader.read_buf(&mut self.buffer).await?;
        let mut result = Vec::new();
        if read == 0 {
            // nothing was read, just issue the exit command
            result.push(Command::Exit);
        }
        else {
            // we look for possible commands searching for '\n' bytes.
            if let Some(last_separator_index) = self.buffer.iter().rposition(|x| *x == '\n' as u8) {
                result.extend(self.buffer.split(|x| *x == '\n' as u8).
                    filter(|x| !x.is_empty()). // sans the last element which is empty
                    map(|x| std::str::from_utf8(x).unwrap_or("Invalid utf8.")). // will result in Unparsable
                    map(|x| Command::parse(x)));
                // remove the parsed section from the buffer
                self.buffer.drain(0..last_separator_index + 1);
            }
        }
        Ok(result)
    }
}

async fn send_message(sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>, 
    message: Message) -> anyhow::Result<Message> {
    let (result_sender, result_receiver) = oneshot::channel();
    sender.send((message, result_sender)).await?;
    let result = result_receiver.await?;
    Ok(result)
}

async fn write_to_socket<'a>(socket_writer: &mut WriteHalf<'a>, data: &str) -> anyhow::Result<()> {
    let bytes = data.as_bytes();
    socket_writer.write_all(&bytes.len().to_be_bytes()).await?;
    socket_writer.write_all(bytes).await?;
    Ok(())
}

async fn handle_commands_and_notifications(socket: &mut TcpStream, address: SocketAddr, 
    data_server_sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>,
    mut notify_sender: mpsc::Sender<Message>, mut notify_receiver: mpsc::Receiver<Message>) -> anyhow::Result<()> {
    let mut command_parser = CommandParser {
        buffer: Vec::new()
    };
    let (mut socket_reader, mut socket_writer) = socket.split();
    // we run two loops concurrently, one processing input from the client
    // the other one processing notifications (which can also come from processing commands) and 
    // sending results back to the client
    tokio::select! {
        r1 = async {
            loop {
                for command in command_parser.read_and_parse(&mut socket_reader).await? {
                    if let Command::Exit = command {
                        return Ok::<(), anyhow::Error>(())
                    }
                    let result = send_message(data_server_sender, Message::Command(address, command)).await?;
                    // notifications are just messages that we intend to send back to the client
                    notify_sender.send(result).await?;
                }
            }
        } => r1?,
        r2 = async {
            loop {
                let notification = notify_receiver.recv().await.context("Cound not receive notification.")?;
                write_to_socket(&mut socket_writer, &notification.to_json_string()).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        } => r2?
    }
    Ok(())
}

async fn send_client_closed(data_server_sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>,
    address: SocketAddr) -> anyhow::Result<()> {
    if let client_closed_result @ Message::Error(_) = 
        send_message(data_server_sender, Message::ClientClosed(address)).await? {
        eprintln!("{:?}", client_closed_result);
    }
    Ok(())
}

async fn handle_connection(mut socket: TcpStream, address: SocketAddr, 
    mut data_server_sender: mpsc::Sender<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    println!("Client {} connected.", address);
    let (notify_sender, notify_receiver) = mpsc::channel(10);
    // send any error we encouter back to the client and exit gracefully 
    if let new_client_result @ Message::Error(_) = 
        send_message(&mut data_server_sender, Message::NewClient(address, notify_sender.clone())).await? {
            let (_, mut socket_writer) = socket.split();
            write_to_socket(&mut socket_writer, &new_client_result.to_json_string()).await?;
            println!("Client {} disconnected.", address);
            return Ok(())
    }
    // we can't allow the error to propagate yet because we need to send the ClientClosed message to the data server
    if let error @ Err(_) = handle_commands_and_notifications(&mut socket, address, 
        &mut data_server_sender, notify_sender, notify_receiver).await {
        send_client_closed(&mut data_server_sender, address).await?;
        return error;
    }
    send_client_closed(&mut data_server_sender, address).await?;
    println!("Client {} disconnected.", address);
    Ok(())
}

struct Account {
    password: String,
    messages: Vec<(String, String)>, // (sender, message)
    logged_address: Option<SocketAddr> // if present, a client is logged into this account
}

struct AccountsManager {
    accounts: HashMap<String, Account>,
    notifiers: HashMap<SocketAddr, mpsc::Sender<Message>>
}

impl AccountsManager {

    fn create_account(&mut self, username: String, password: String) -> Message {
        match self.accounts.entry(username.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(Account {
                    password,
                    messages: Vec::new(),
                    logged_address: None
                });
                println!("The user '{}' was created.", username);
                Message::Success
            }
            Entry::Occupied(account) => {
                Message::Error(format!("An account with the username '{}' already exists.", account.key()))
            }
        }
    }

    fn logged_account(&mut self, address: SocketAddr) -> Option<(&String, &mut Account)> {
        self.accounts.iter_mut().find(|x| x.1.logged_address.as_ref().
            map_or(false, |y| y == &address))
    }

    fn logout(&mut self, address: SocketAddr) -> Message {
        if let Some(account) = self.logged_account(address) {
            println!("Client {} logged out of '{}'.", address, account.0);
            account.1.logged_address = None;
            Message::Success
        }
        else {
            Message::Error(format!("Client {} is not logged in.", address))
        }
    }

    async fn login(&mut self, username: String, password: String, address: SocketAddr) -> anyhow::Result<Message> {
        if let Some(account) = self.logged_account(address) {
            return Ok(Message::Error(format!("The client {} is already logged into '{}'.", address, account.0)))
        }
        match self.accounts.entry(username.clone()) {
            Entry::Vacant(_) => {
                Ok(Message::Error("Invalid username or password.".to_string()))
            }
            Entry::Occupied(mut account) => {
                if account.key() == &username && account.get().password == password {
                    // if we already have a client connected we log it out and notify it
                    // replacing the address means that the new client is considered to be logged in
                    if let Some(old_address) = account.get_mut().logged_address.replace(address) {
                        println!("{} was logged out of '{}', logging {} in.", old_address, username, address);
                        self.notifiers.get_mut(&old_address).
                            context(format!("No notifier for client {}.", old_address))?.
                            send(Message::ForceLogout).await?;
                    }
                    println!("Client {} logged into '{}'.", address, username);
                    Ok(Message::Success)
                }
                else {
                    Ok(Message::Error("Invalid username or password.".to_string()))
                }
            }
        }
    }

    async fn send(&mut self, recipients: Vec<String>, message: String, address: SocketAddr) -> anyhow::Result<Message> {
        if let Some(account) = self.logged_account(address) {
            let username = account.0.clone();
            // make sure all recipients are valid
            for r in &recipients {
                if !self.accounts.keys().any(|x| x == r) {
                    return Ok(Message::Error(format!("Recipient '{}' is not a valid account.", r)))
                }
            }
            for r in &recipients {
                let recipient_account = self.accounts.get_mut(r).context("Unreachable.")?;
                recipient_account.messages.push((username.clone(), message.clone()));
                if let Some(logged_address) = recipient_account.logged_address {
                    self.notifiers.get_mut(&logged_address).
                        context(format!("No notifier for client {}.", logged_address))?.
                        send(Message::NewMessageInMailbox(recipient_account.messages.len() - 1)).await?;
                }
            }
            Ok(Message::Success)
        }
        else {
            Ok(Message::Error(format!("The client {} is not logged in.", address)))
        }
    }

    fn read_message(&mut self, id: usize, address: SocketAddr) -> Message {
        if let Some(account) = self.logged_account(address) {
            if let Some(message) = account.1.messages.get(id) {
                Message::Message(message.0.clone(), message.1.clone())
            }
            else {
                Message::Error(format!("The user '{}' has no message with id {}.", account.0, id))
            }
        }
        else {
            Message::Error(format!("The client {} is not logged in.", address))
        }
    }

    fn read_mailbox(&mut self, address: SocketAddr) -> Message {
        if let Some(account) = self.logged_account(address) {
            // if n messages then 0 -> n-1 indices will be collected
            Message::Mailbox((0..account.1.messages.len()).collect())
        }
        else {
            Message::Error(format!("The client {} is not logged in.", address))
        }
    }
}

async fn data_server(mut data_server_receiver: mpsc::Receiver<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    let mut accounts_manager = AccountsManager {
        accounts: HashMap::new(),
        notifiers: HashMap::new()
    };
    loop {
        let (message, result_sender) = 
            data_server_receiver.recv().await.context("Error receiving message. Closing.")?;
        let result = match message {
            Message::Command(address, command) => match command {
                Command::CreateAccount(username, password) => 
                    accounts_manager.create_account(username, password),
                Command::Login(username, password) =>
                    accounts_manager.login(username, password, address).await?,
                Command::Logout => accounts_manager.logout(address),
                Command::SendMessage(recipients, message) => 
                    accounts_manager.send(recipients, message, address).await?,
                Command::ReadMessage(id) => accounts_manager.read_message(id, address),
                Command::ReadMailbox => accounts_manager.read_mailbox(address),
                _ => Message::Error(format!("Unknown command {:?}", command))
            }
            // every client will send a notifier upon connecting. 
            Message::NewClient(address, notify_sender) => {
                println!("New client {}.", address);
                accounts_manager.notifiers.insert(address, notify_sender);
                Message::Success
            }
            // remove the notifier upon disconnection
            Message::ClientClosed(address) => {
                println!("Client closed {}.", address);
                accounts_manager.notifiers.remove(&address);
                accounts_manager.logout(address);
                Message::Success
            }
            _ => Message::Error(format!("Unknown message {:?}.", message))
        };
        if let Err(unsent_result) = result_sender.send(result) {
            return Err(anyhow::anyhow!("Could not send result {:?} to client.", unsent_result));
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:8080").await?;
    let (data_server_sender, data_server_receiver) = 
        mpsc::channel(100);
    tokio::spawn(async move {
        data_server(data_server_receiver).await.unwrap();
    });
    loop {
        let (socket, address) = listener.accept().await?;
        let data_server_sender = data_server_sender.clone();
        tokio::spawn(async move {
            // catch any error here so as not to panic the thread running the task
            if let Err(error) = handle_connection(socket, address, data_server_sender).await {
                eprintln!("{}", error);
            }
        });
    }
}
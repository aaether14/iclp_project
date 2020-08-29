use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::prelude::*;

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
    Logout,
    Exit
}

#[derive(Debug)]
enum Message {
    Success,
    ForceLogout,
    MessageView {
        from: String,
        content: String
    },
    NewMessageInMailbox(usize),
    Error(String),
    ClientClosed(SocketAddr),
    NewClient(SocketAddr, mpsc::Sender<Message>),
    Command(SocketAddr, Command)
}

impl Command {
    fn parse(data: &str) -> Command {
        match &data.split(|x| x == ' ').collect::<Vec<_>>() as &[&str] {
            &["CREATE_ACCOUNT", username, password] => 
                Command::CreateAccount(username.to_string(), password.to_string()),
            &["LOGIN", username, password] => 
                Command::Login(username.to_string(), password.to_string()), 
            &["LOGOUT"] => Command::Logout,
            &["SEND", ref recipients @ .. , message] => 
                Command::SendMessage(recipients.iter().map(|x| x.to_string()).collect(), message.to_string()),
            &["READ_MSG", id] => id.parse().map_or(Command::Unparsable(data.to_string()), 
                |x| Command::ReadMessage(x)),
            _ => Command::Unparsable(data.to_string())
        }
    }
}

struct CommandParser {
    buffer: Vec<u8>
}

impl CommandParser {
    async fn read_and_parse(&mut self, socket: &mut TcpStream) -> anyhow::Result<Vec<Command>> {
        let read = socket.read_buf(&mut self.buffer).await?;
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

async fn send_message(main_sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>, 
    message: Message) -> anyhow::Result<Message> {
    let (result_sender, result_receiver) = oneshot::channel();
    // only issue an error if the message architecture is malfunctioning 
    main_sender.send((message, result_sender)).await?;
    let result = result_receiver.await?;
    Ok(result)
}

async fn handle_commands(socket: &mut TcpStream, address: SocketAddr, 
    main_sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>,
    notify_receiver: &mut mpsc::Receiver<Message>) -> anyhow::Result<()> {
    let mut command_parser = CommandParser {
        buffer: Vec::new()
    };
    loop {
        // race the two features in a loop so as to run them concurrently
        tokio::select! {
            commands = command_parser.read_and_parse(socket) => {
                for command in commands? {
                    // return if no longer connected to the client
                    if let Command::Exit = command {
                        return Ok(());
                    }
                    else {
                        // send the result back to the client
                        let result = send_message(main_sender, Message::Command(address, command)).await?;
                        socket.write_all(format!("{:?}", result).as_bytes()).await?;
                    }
                }
            },
            notification = notify_receiver.recv() => {
                socket.write_all(format!("{:?}", 
                    notification.context("Could not receive notification.")?).as_bytes()).await?;
            }
        }
    }
}

async fn send_client_closed(main_sender: &mut mpsc::Sender<(Message, oneshot::Sender<Message>)>,
    address: SocketAddr) -> anyhow::Result<()> {
    if let client_closed_result @ Message::Error(_) = 
        send_message(main_sender, Message::ClientClosed(address)).await? {
        eprintln!("{:?}", client_closed_result);
    }
    Ok(())
}

// the main_sender will be used to send messages to the data server
// we also send a oneshot::Receiver in order to receive results
async fn handle_connection(mut socket: TcpStream, address: SocketAddr, 
    mut main_sender: mpsc::Sender<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    println!("Client {} connected.", address);
    let (notify_sender, mut notify_receiver) = mpsc::channel(10);
    // send any error we encouter back to the client and exit gracefully 
    if let new_client_result @ Message::Error(_) = 
        send_message(&mut main_sender, Message::NewClient(address, notify_sender)).await? {
            socket.write_all(format!("{:?}", new_client_result).as_bytes()).await?;
            println!("Client {} disconnected.", address);
            return Ok(())
    }
    // we can't allow the error to propagate yet because we need to send the ClientClosed message to the data server
    if let error @ Err(_) = handle_commands(&mut socket, address, 
        &mut main_sender, &mut notify_receiver).await {
        send_client_closed(&mut main_sender, address).await?;
        return error;
    }
    send_client_closed(&mut main_sender, address).await?;
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
                Message::MessageView {
                    from: message.0.clone(),
                    content: message.1.clone()
                }
            }
            else {
                Message::Error(format!("The user '{}' has no message with id {}.", account.0, id))
            }
        }
        else {
            Message::Error(format!("The client {} is not logged in.", address))
        }
    }
}

async fn data_server(mut main_receiver: mpsc::Receiver<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    let mut accounts_manager = AccountsManager {
        accounts: HashMap::new(),
        notifiers: HashMap::new()
    };
    loop {
        let (message, result_sender) = 
            main_receiver.recv().await.context("Error receiving message. Closing.")?;
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
    let (main_sender, main_receiver) = 
        mpsc::channel(100);
    tokio::spawn(async move {
        data_server(main_receiver).await.unwrap();
    });
    loop {
        let (socket, address) = listener.accept().await?;
        let main_sender = main_sender.clone();
        tokio::spawn(async move {
            // catch any error here so as not to panic the thread running the task
            if let Err(error) = handle_connection(socket, address, main_sender.clone()).await {
                eprintln!("{}", error);
            }
        });
    }
}
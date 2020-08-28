use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::prelude::*;

use anyhow::Context;

use std::net::SocketAddr;

#[derive(Debug, PartialEq)]
enum Command {
    CreateAccount(String, String),
    Unparsable(String),
    Exit
}

#[derive(Debug)]
enum Message {
    Success,
    Error(String),
    Command(Command)
}

impl Command {
    fn parse(data: &str) -> Command {
        match &data.split(|x| x == ' ').collect::<Vec<_>>() as &[&str] {
            &["CREATE_ACCOUNT", username, password] => Command::CreateAccount(
                username.to_string(),
                password.to_string()
            ),
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

// the main_sender will be used to send messages to the data server
// we also send a oneshot::Receiver in order to receive results
async fn handle_connection(mut socket: TcpStream, address: SocketAddr, 
    mut main_sender: mpsc::Sender<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    println!("Client {:?} connected.", address);
    let mut command_parser = CommandParser {
        buffer: Vec::new()
    };
    loop {
        for command in command_parser.read_and_parse(&mut socket).await? {
            // return if no longer connected to the client
            if command == Command::Exit {
                println!("Client {:?} disconnected.", address);
                return Ok(());
            }
            else {
                // forward the command to the data server
                // send_message can only fail if the message sending architecture fails
                // ordinary errors are sent back to the user
                let result = send_message(&mut main_sender, Message::Command(command)).await?;
                socket.write_all(format!("{:?}", result).as_bytes()).await?;
            }
        }
    }
}

async fn data_server(mut main_receiver: mpsc::Receiver<(Message, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
    loop {
        let (message, result_sender) = 
        main_receiver.recv().await.context("Error receiving message. Closing.")?;
        let result = match message {
            Message::Command(command) => match command {
                _ => Message::Error(format!("Unknown command {:?}", command))
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
            if let Err(error) = handle_connection(socket, address, main_sender.clone()).await {
                eprintln!("{}", error);
            }
        });
    }
}
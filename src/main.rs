use futures::future::join_all;
use futures::sink::SinkExt;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

use clap::Clap;
use env_logger::Builder;
use log::LevelFilter;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::task::JoinHandle;
use tokio::task::spawn;
use tokio::time::timeout;
use tokio_stream::StreamExt as TokioStreamExt;

use tokio_util::codec::{Framed,LinesCodec};

async fn server_default(listener: TcpListener, length: usize) -> JoinHandle<()> {
    spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                spawn(async move {
                    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

                    loop {
                        while let Some(Ok(line)) = stream.next().await {
                            stream.send(line).await.unwrap();
                        }
                    }
                });
            }
        }
    })
}

async fn server_rolling_instant(listener: TcpListener, length: usize) -> JoinHandle<()> {
    spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                spawn(async move {
                    let timeout = Duration::from_secs(5);
                    let mut active_at = Instant::now();

                    let mut timer = tokio::time::interval(Duration::from_secs(5));

                    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

                    loop {
                        tokio::select! {
                            biased;
                            read = stream.next() => {
                                active_at = Instant::now();

                                if let Some(Ok(line)) = read {
                                    stream.send(line).await.unwrap();
                                }
                            },
                            _ = timer.tick() => {
                                if active_at.elapsed() > timeout {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        }
    })
}

async fn server_loop_sleep(listener: TcpListener, length: usize) -> JoinHandle<()> {
    spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                spawn(async move {
                    let timeout = Duration::from_secs(5);
                    let mut active_at = Instant::now();

                    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

                    loop {
                        let sleep = tokio::time::sleep(Duration::from_secs(5));

                        tokio::select! {
                            biased;
                            read = stream.next() => {
                                active_at = Instant::now();

                                if let Some(Ok(line)) = read {
                                    stream.send(line).await.unwrap();
                                }
                            },
                            _ = sleep => {
                                if active_at.elapsed() > timeout {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        }
    })
}

async fn server_loop_sleep_unbiased(listener: TcpListener, length: usize) -> JoinHandle<()> {
    spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                spawn(async move {
                    let timeout = Duration::from_secs(5);
                    let mut active_at = Instant::now();

                    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

                    loop {
                        let sleep = tokio::time::sleep(Duration::from_secs(5));

                        tokio::select! {
                            read = stream.next() => {
                                active_at = Instant::now();

                                if let Some(Ok(line)) = read {
                                    stream.send(line).await.unwrap();
                                }
                            },
                            _ = sleep => {
                                if active_at.elapsed() > timeout {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        }
    })
}

async fn server_next_timeout(listener: TcpListener, length: usize) -> JoinHandle<()> {
    spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                spawn(async move {
                    let timeout_duration = Duration::from_secs(5);
                    let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

                    loop {
                        while let Ok(Some(Ok(line))) = timeout(timeout_duration, stream.next()).await {
                            stream.send(line).await.unwrap();
                        }
                    }
                });
            }
        }
    })
}

// NOTE: Not supported yet, timeout() breaks send()
// async fn server_stream_timeout(listener: TcpListener) {
//     spawn(async move {
//         loop {
//             if let Ok((stream, _)) = listener.accept().await {
//                 spawn(async move {
//                     let stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

//                     let mut stream = stream.timeout(Duration::from_secs(5));
//                     tokio::pin!(stream);

//                     loop {
//                         while let Some(Ok(line)) = stream.next().await {
//                             stream.send(line).await.unwrap();
//                         }
//                     }
//                 });
//             }
//         }
//     });
// }

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
struct Program {
    #[clap(short, long, parse(from_occurrences))]
    verbose: usize,
    #[clap(short,long,default_value="127.0.0.1:3090")]
    bind : String,
    #[clap(short='n',long,default_value="1")]
    clients : usize,
    #[clap(short='e',long,default_value="996")]
    length : usize,
    #[clap(short='r',long,default_value="65535")]
    lines : usize,
    #[clap(long)]
    hold : bool,
    #[clap(short,long,default_value="default")]
    server_type : String
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let program = Program::parse();

    let mut builder = Builder::new();

    builder.filter_level(
        match program.verbose {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            _ => LevelFilter::Trace
        }
    );

    builder.parse_env("RUST_LOG");
    builder.init();

    let listener = TcpListener::bind(program.bind.as_str()).await.unwrap();

    let server = match program.server_type.as_str() {
        "rolling_instant" => server_rolling_instant(listener, program.length).await,
        "loop_sleep" => server_loop_sleep(listener, program.length).await,
        "loop_sleep_unbiased" => server_loop_sleep_unbiased(listener, program.length).await,
        "next_timeout" => server_next_timeout(listener, program.length).await,
        _ => server_default(listener, program.length).await
    };

    log::debug!("Server started");

    log::debug!("Clients started");

    let lines = program.lines;
    let addr : SocketAddr = program.bind.parse().unwrap();

    let data = format!("{:-^1$}", "x", program.length - 2);

    let start = Instant::now();
    let length = program.length;

    let clients = (0..program.clients).map(|_| {
        let data = data.clone();

        spawn(async move {
            let start = Instant::now();
            let socket = TcpSocket::new_v4().unwrap();
            let stream = socket.connect(addr).await.unwrap();

            stream.set_nodelay(true).unwrap();

            let mut stream = Framed::new(stream, LinesCodec::new_with_max_length(length));

            for _ in 0..lines {
                stream.send(data.as_str()).await.unwrap();
                stream.next().await;
            }

            start.elapsed()
        })
    });

    let mut total_lines = 0;
    let mut total_elapsed = Duration::from_secs(0);
    let mut errored = 0;

    for r in join_all(clients).await {
        match r {
            Ok(d) => {
                total_lines += program.lines;
                total_elapsed += d;
            },
            Err(err) => {
                log::error!("Client error: {}", err);

                errored += 1;
            }
        }
    }

    let elapsed = start.elapsed();

    let mb = (total_lines * (program.length + 2)) as f64 / (1024 * 1024) as f64;

    log::info!("Average TPS: {:.0}", total_lines as f64 / total_elapsed.as_secs_f64());
    log::info!("Average MB/s: {:.1}", mb / total_elapsed.as_secs_f64());
    log::info!("Overall MB/s: {:.1}", mb / elapsed.as_secs_f64());


    if errored > 0 {
        log::error!("{} client(s) experienced errors", errored);
    }

    log::debug!("Clients finished");

    if program.hold {
        server.await.unwrap();
    }

    Ok(())
}

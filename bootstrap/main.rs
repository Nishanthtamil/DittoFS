use libp2p::{
    kad, identify, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, tls, yamux, Swarm, StreamProtocol
};
use std::time::Duration;
use futures::stream::StreamExt;

#[derive(NetworkBehaviour)]
struct BootstrapBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    identify: identify::Behaviour,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            tls::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let kad_config = kad::Config::new(StreamProtocol::new("/dittofs/1.0.0"));
            let store = kad::store::MemoryStore::new(key.public().to_peer_id());
            let kademlia = kad::Behaviour::with_config(key.public().to_peer_id(), store, kad_config);

            let identify = identify::Behaviour::new(identify::Config::new(
                "/dittofs/1.0.0".into(),
                key.public(),
            ));

            Ok(BootstrapBehaviour { kademlia, identify })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/4001".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/4001/quic-v1".parse()?)?;

    println!("Bootstrap node started!");
    println!("Local Peer ID: {:?}", swarm.local_peer_id());

    while let Some(event) = swarm.next().await {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening on: {:?}", address);
            }
            SwarmEvent::Behaviour(BootstrapBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. })) => {
                for addr in info.listen_addrs {
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

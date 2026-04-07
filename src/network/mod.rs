//! Gossip Layer Module
//! Handles peer-to-peer communication using libp2p.

use libp2p::{
    gossipsub, mdns, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, tls, yamux, Swarm
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tokio::sync::mpsc;
use futures::stream::StreamExt;

pub mod update;

#[derive(NetworkBehaviour)]
pub struct DittoNetworkBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub const DITTO_TOPIC: &str = "ditto-fs-sync";

pub fn create_swarm() -> Result<Swarm<DittoNetworkBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            tls::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };
            
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .expect("Valid gossipsub config");
                
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            ).expect("Valid gossipsub behaviour");
            
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(DittoNetworkBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    Ok(swarm)
}

pub async fn run_swarm(
    mut swarm: Swarm<DittoNetworkBehaviour>,
    mut local_updates: mpsc::Receiver<Vec<u8>>,
    remote_updates: mpsc::Sender<Vec<u8>>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let topic = gossipsub::IdentTopic::new(DITTO_TOPIC);
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    loop {
        tokio::select! {
            update = local_updates.recv() => {
                if let Some(data) = update {
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
                        eprintln!("Publish error: {:?}", e);
                    }
                } else {
                    break;
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(DittoNetworkBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(DittoNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: _,
                    message_id: _,
                    message,
                })) => {
                    let _ = remote_updates.send(message.data).await;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

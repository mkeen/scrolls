use pallas::network::{miniprotocols::handshake, multiplexer};
use tonic::{Response, Streaming};
use tonic::transport::{Channel, Endpoint};
use utxorpc::proto::sync::v1::chain_sync_service_client::{ChainSyncServiceClient};
use utxorpc::proto::sync::v1::{FollowTipRequest, FollowTipResponse};
use futures::executor;
use log::log;

pub struct Transport {
    pub channel6: ChainSyncServiceClient<Channel>,
}

impl Transport {
    pub fn setup(address: &str) -> Result<Self, crate::Error> {
        let channel6 = executor::block_on(ChainSyncServiceClient::connect(format!("grpc://{}", address))).unwrap();

        Ok(Self {
            channel6,
        })
    }
}

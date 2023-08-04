use gasket::messaging::OutputPort;
use serde::Deserialize;

use crate::{bootstrap, crosscut, model, storage};

#[cfg(target_family = "unix")]
pub mod n2c;

pub mod n2n;
pub mod utils;
pub mod grpc;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    N2N(n2n::Config),
    GRPC(grpc::Config),
    #[cfg(target_family = "unix")]
    N2C(n2c::Config),
}

impl Config {
    pub fn bootstrapper(
        self,
        chain: &crosscut::ChainWellKnownInfo,
        intersect: &crosscut::IntersectConfig,
        finalize: &Option<crosscut::FinalizeConfig>,
        policy: &crosscut::policies::RuntimePolicy,
    ) -> Bootstrapper {
        match self {
            Config::GRPC(c) => Bootstrapper::GRPC(c.bootstrapper(chain, intersect, finalize, policy)),
            Config::N2N(c) => Bootstrapper::N2N(c.bootstrapper(chain, intersect, finalize, policy)),
            Config::N2C(c) => Bootstrapper::N2C(c.bootstrapper(chain, intersect, finalize, policy)),
        }
    }
}

pub enum Bootstrapper {
    N2N(n2n::Bootstrapper),
    N2C(n2c::Bootstrapper),
    GRPC(grpc::Bootstrapper),
}

impl Bootstrapper {
    pub fn borrow_output_port(&mut self) -> &'_ mut OutputPort<model::RawBlockPayload> {
        match self {
            Bootstrapper::N2N(p) => p.borrow_output_port(),
            Bootstrapper::N2C(p) => p.borrow_output_port(),
            Bootstrapper::GRPC(p) => p.borrow_output_port(),
        }
    }

    pub fn spawn_stages(self, pipeline: &mut bootstrap::Pipeline, cursor: storage::Cursor) {
        match self {
            Bootstrapper::N2N(p) => p.spawn_stages(pipeline, cursor),
            Bootstrapper::N2C(p) => p.spawn_stages(pipeline, cursor),
            Bootstrapper::GRPC(p) => p.spawn_stages(pipeline, cursor),
        }
    }
}

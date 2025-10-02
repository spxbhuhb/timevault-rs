use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::raft::AppConfig;
use crate::typ;
use crate::TvrNodeId;

#[derive(Clone)]
pub struct Network {
    client: reqwest::Client,
}

impl Network {
    pub fn new() -> Self {
        // Reuse connections aggressively to avoid ephemeral port exhaustion under load.
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .build()
            .expect("build reqwest client");
        Self { client }
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<AppConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: TvrNodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection { client: self.client.clone(), target, target_node: node.clone() }
    }
}

pub struct NetworkConnection {
    client: reqwest::Client,
    target: TvrNodeId,
    target_node: BasicNode,
}

impl NetworkConnection {
    async fn send_rpc<Req, Resp, Err>(&self, uri: &str, req: Req) -> Result<Resp, openraft::error::RPCError<TvrNodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let url = format!("http://{}/{}", self.target_node.addr, uri);
        tracing::debug!("send_rpc to url: {}", url);

        let resp = self.client.post(url).json(&req).send().await.map_err(|e| {
            if e.is_connect() {
                return openraft::error::RPCError::Unreachable(Unreachable::new(&e));
            }
            openraft::error::RPCError::Network(NetworkError::new(&e))
        })?;

        tracing::debug!("client.post() is sent");

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<AppConfig> for NetworkConnection {
    async fn append_entries(&mut self, req: AppendEntriesRequest<AppConfig>, _option: RPCOption) -> Result<AppendEntriesResponse<TvrNodeId>, typ::RPCError> {
        self.send_rpc("raft-append", req).await
    }

    async fn install_snapshot(&mut self, req: InstallSnapshotRequest<AppConfig>, _option: RPCOption) -> Result<InstallSnapshotResponse<TvrNodeId>, typ::RPCError<InstallSnapshotError>> {
        self.send_rpc("raft-snapshot", req).await
    }

    async fn vote(&mut self, req: VoteRequest<TvrNodeId>, _option: RPCOption) -> Result<VoteResponse<TvrNodeId>, typ::RPCError> {
        self.send_rpc("raft-vote", req).await
    }
}

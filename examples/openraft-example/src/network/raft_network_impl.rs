use openraft::BasicNode;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RemoteError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ExampleConfig;
use crate::TvrNodeId;
use crate::typ;

pub struct Network {}

impl Network {
    pub async fn send_rpc<Req, Resp, Err>(&self, target: TvrNodeId, target_node: &BasicNode, uri: &str, req: Req) -> Result<Resp, openraft::error::RPCError<TvrNodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = &target_node.addr;

        let url = format!("http://{}/{}", addr, uri);
        tracing::debug!("send_rpc to url: {}", url);

        let client = reqwest::Client::new();
        tracing::debug!("client is created for: {}", url);

        let resp = client.post(url).json(&req).send().await.map_err(|e| {
            // If the error is a connection error, we return `Unreachable` so that connection isn't retried
            // immediately.
            if e.is_connect() {
                return openraft::error::RPCError::Unreachable(Unreachable::new(&e));
            }
            openraft::error::RPCError::Network(NetworkError::new(&e))
        })?;

        tracing::debug!("client.post() is sent");

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
impl RaftNetworkFactory<ExampleConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: TvrNodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: Network {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    owner: Network,
    target: TvrNodeId,
    target_node: BasicNode,
}

impl RaftNetwork<ExampleConfig> for NetworkConnection {
    async fn append_entries(&mut self, req: AppendEntriesRequest<ExampleConfig>, _option: RPCOption) -> Result<AppendEntriesResponse<TvrNodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-append", req).await
    }

    async fn install_snapshot(&mut self, req: InstallSnapshotRequest<ExampleConfig>, _option: RPCOption) -> Result<InstallSnapshotResponse<TvrNodeId>, typ::RPCError<InstallSnapshotError>> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-snapshot", req).await
    }

    async fn vote(&mut self, req: VoteRequest<TvrNodeId>, _option: RPCOption) -> Result<VoteResponse<TvrNodeId>, typ::RPCError> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-vote", req).await
    }
}

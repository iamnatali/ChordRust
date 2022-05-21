use stateright::actor::{*, register::*};
use std::borrow::Cow; // COW == clone-on-write
use std::net::{SocketAddrV4, Ipv4Addr};
use std::collections::BTreeMap;

type CircleId = u64; // to big int ???

struct NodeActor { bootstrap_to_id: Option<Id>, myId: CircleId, 
    predecessor: Option<NodeInfo>, //потом убрать
    fingerTable: BTreeMap<CircleId, NodeInfo> //потом убрать
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct NodeInfo {refId: Id, circleId: CircleId}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum NodeMessage{
    FindPredecessor(
        CircleId, //id
        Id, //asker
        Option<Id>, //successorAsker
    ),
    FoundPredecessor(
        CircleId, //queryId
        NodeInfo, //predecessor
        NodeInfo,//predecessorSuccessor
        Option<Id>//asker
    )
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NodeState{
    predecessor: Option<NodeInfo>,
    behavior: &'static str,
    fingerTable: BTreeMap<CircleId, NodeInfo>
}

fn fingerStart(n: CircleId, k: u16, m:u16) -> CircleId {
    let nonmod = n + u64::from(
        (2u16).pow(
            (k-1).into()
        )
    );
    // let res: <u64 as std::ops::Rem<_>>::Output = nonmod % ((2u16).pow(m.into()).into());
    // <res as CircleId>
    nonmod.rem_euclid((2u16).pow(m.into()).into())
}

fn belongsClockwise01(id: CircleId, intervalStart: CircleId, intervalEnd: CircleId, m: u16) -> bool{
    let largest = 2u64.pow(m.into());
    let intervalStartM = intervalStart.rem_euclid(largest);
    let intervalEndM = intervalEnd.rem_euclid(largest);
    let idM = id.rem_euclid(largest);
    if intervalStartM < intervalEndM {
        intervalStartM < idM && idM <= intervalEndM
    }
    else{
        intervalStartM < idM || idM <= intervalEndM
    }
}

fn belongsClockwise00(id: CircleId, intervalStart: CircleId, intervalEnd: CircleId, m: u16) -> bool{
    let largest = 2u64.pow(m.into());
    let intervalStartM = intervalStart.rem_euclid(largest);
    let intervalEndM = intervalEnd.rem_euclid(largest);
    let idM = id.rem_euclid(largest);
    if intervalStartM < intervalEndM {
        intervalStartM < idM && idM < intervalEndM
    }
    else{
        intervalStartM < idM || idM < intervalEndM
    }
}


impl NodeActor{
    fn closestPrecedingFinger(&self, id: CircleId, m: u16, ft: &BTreeMap<CircleId, NodeInfo>) -> NodeInfo{
        let mut result: Option<NodeInfo> = None;
        for i in (0..100).rev() {
            let ftid = self.myId + 2u64.pow(i-1);
            match ft.get(&ftid) {
                Some(nodeInfo) => {
                    if belongsClockwise00(nodeInfo.circleId, self.myId, id, 3){
                        result = Some(*nodeInfo);
                        break;
                    }
                }
                None => {}
            }
        }
        let id = self.myId + 2u64.pow((m-1).into());
        match result {
            Some(res) => {res}
            None => {match ft.get(&id) {
                Some(res) => {*res}
                None =>  {NodeInfo{refId: Id::from(1), circleId: 1}} //этот код должен быть недостижимым
            }}
        }
    } 
}

//m=3
impl Actor for NodeActor {
    type Msg = NodeMessage;
    type State = NodeState;

    fn on_start(&self, _id: Id, _o: &mut Out<Self>) -> Self::State {
        // if let Some(peer_id) = self.bootstrap_to_id {
        //     _o.send(peer_id, NodeMessage::FindPredecessor(fingerStart(self.myId, 1, 3), _id, None));
        //     NodeState{
        //         predecessor : None,
        //         behavior : &"initializing",
        //         fingerTable : BTreeMap::new()
        //     }
        // } else {
        //     NodeState{
        //         predecessor : Some(NodeInfo{refId :_id, circleId : self.myId}),
        //         behavior : &"initializing",
        //         fingerTable : BTreeMap::new()
        //     }
        // }
        //peer_id = 0
        if let Some(peer_id) = self.bootstrap_to_id {
            _o.send(peer_id, NodeMessage::FindPredecessor(6, _id, None)); 
        }
        NodeState{
            predecessor :self.predecessor,
            behavior : &"internalReceive",
            fingerTable : self.fingerTable.clone()
        }
    }

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>,
              src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        match state.behavior {
            "initializing" => match msg{
                NodeMessage::FoundPredecessor(_, predecessor, predecessor_successor, _) => {
                    let mut state = state.to_mut();
                    state.predecessor = Some(predecessor);
                    state.behavior = "internalReceive";
                    state.fingerTable .insert(fingerStart(self.myId, 1, 3), predecessor_successor);
                }
                _ => {}
            }
            "internalReceive" => match msg {
                NodeMessage::FindPredecessor(id, asker, successor_asker) => {
                    match state.fingerTable.get(&fingerStart(self.myId, 1, 3)) {
                        Some(value) => 
                        if belongsClockwise00(id, self.myId, value.circleId, 3){
                            o.send(asker, NodeMessage::FoundPredecessor(id, NodeInfo{refId:_id, circleId: self.myId}, *value, successor_asker))
                        } else {
                            let info = self.closestPrecedingFinger(id, 3, &state.fingerTable);
                            o.send(info.refId, msg)
                        }
                        None => {
                            let info = self.closestPrecedingFinger(id, 3, &state.fingerTable);
                            o.send(info.refId, msg)
                        }
                    }
                }
                _ => {}
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use stateright::{*, semantics::*, semantics::register::*};
    use ActorModelAction::Deliver;
    use RegisterMsg::{Get, GetOk, Put, PutOk};

    #[test]
    fn could_find_predecessor() {
        let checker = ActorModel::new(
                (),
                ()
            )
            .actor(NodeActor { bootstrap_to_id: Some(Id::from(0)), myId: 0, 
                predecessor: None, //потом убрать
                fingerTable: BTreeMap::from([
                    (1, NodeInfo{refId: Id::from(1), circleId: 1}),
                    (2, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (4, NodeInfo{refId: Id::from(0), circleId: 0})
                    ])//потом убрать
            })
            .actor(NodeActor { bootstrap_to_id: None, myId: 1, 
                predecessor: None, //потом убрать
                fingerTable: BTreeMap::from([
                    (2, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (3, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (5, NodeInfo{refId: Id::from(0), circleId: 0})
                    ])//потом убрать
            })
            .actor(NodeActor { bootstrap_to_id: None, myId: 3, 
                predecessor: None, //потом убрать
                fingerTable: BTreeMap::from([
                    (4, NodeInfo{refId: Id::from(0), circleId: 0}),
                    (5, NodeInfo{refId: Id::from(0), circleId: 0}),
                    (7, NodeInfo{refId: Id::from(0), circleId: 0})
                    ])//потом убрать
            })
            // .actor(RegisterActor::Server(ServerActor))
            // .actor(RegisterActor::Client { put_count: 2, server_count: 1 })
            // .property(Expectation::Always, "linearizable", |_, state| {
            //     state.history.serialized_history().is_some()
            // })
            // .property(Expectation::Sometimes, "get succeeds", |_, state| {
            //     state.network.iter_deliverable()
            //         .any(|e| matches!(e.msg, RegisterMsg::GetOk(_, _)))
            // })
            .property(Expectation::Sometimes, "find succeeds", |_, state| {
                state.network.iter_deliverable()
                    .any(|e| matches!(e.msg, NodeMessage::FoundPredecessor(6, NodeInfo{refId: _, circleId: 3}, NodeInfo{refId: _, circleId: 0}, _)))
            })
            // .record_msg_in(RegisterMsg::record_returns)
            // .record_msg_out(RegisterMsg::record_invocations)
            .checker().spawn_dfs().join();
        checker.assert_properties(); // TRY IT: Uncomment this line, and the test will fail.
        // checker.assert_discovery("linearizable", vec![
        //     Deliver { src: Id::from(0), dst: Id::from(0), msg: NodeMessage::FindPredecessor(6, Id::from(0), None)},

        // ]);
        // checker.assert_discovery("linearizable", vec![
        //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(1, 'A') },
        //     Deliver { src: Id::from(0), dst: Id::from(1), msg: PutOk(1) },
        //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(2, 'Z') },
        //     Deliver { src: Id::from(0), dst: Id::from(1), msg: PutOk(2) },
        //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(1, 'A') },
        //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Get(3) },
        //     Deliver { src: Id::from(0), dst: Id::from(1), msg: GetOk(3, 'A') },
        // ]);
    }
}

fn main() {
    // env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    // spawn(
    //     serde_json::to_vec,
    //     |bytes| serde_json::from_slice(bytes),
    //     vec![
    //         (SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3000), ServerActor)
    //     ]).unwrap();
}

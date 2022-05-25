use stateright::actor::{*, register::*};
use std::borrow::Cow; // COW == clone-on-write
use std::net::{SocketAddrV4, Ipv4Addr};
use std::collections::BTreeMap;
use rand::Rng;

type CircleId = u64; // to big int ???

struct InitializingParams{
    firstMessage: Option<(Id, NodeMessage)>,
    predecessor: Option<NodeInfo>, //потом убрать
    fingerTable: BTreeMap<CircleId, NodeInfo> //потом убрать
}

struct NodeActor { 
    myId: CircleId,
    bootstrap_to_id: Option<Id>,
    init: Option<InitializingParams>
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct NodeInfo {refId: Id, circleId: CircleId}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum NodeMessage{
    FindPredecessor(
        CircleId, //id
        Id, //asker
        Option<Id>, //successorAsker
        Option<CircleId> //additional info
    ),
    FoundPredecessor(
        CircleId, //queryId
        NodeInfo, //predecessor
        NodeInfo,//predecessorSuccessor
        Option<Id>,//asker
        Option<CircleId> //additional info
    ),

    Stabilize(u16),
    Notify(NodeInfo),
    GetPredecessor,
    GotPredecessor(Option<NodeInfo>),

    FixFingers,

    FindSuccessor(CircleId, Id, Option<CircleId>),//_, _, additional info 
    Successor(NodeInfo, NodeInfo, Option<CircleId>), //result, predecessor, additional info 
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NodeState{
    predecessor: Option<NodeInfo>,
    behavior: &'static str,
    fingerTable: BTreeMap<CircleId, NodeInfo>
}

fn fingerStart(n: CircleId, k: u16, m:u16) -> CircleId {
    // let nonmod = n + u64::from(
    //     (2u16).pow(
    //         (k-1).into()
    //     )
    // );
    // let res: <u64 as std::ops::Rem<_>>::Output = nonmod % ((2u16).pow(m.into()).into());
    // <res as CircleId>
    //nonmod.rem_euclid((2u16).pow(m.into()).into());
    n
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
            let ftid = fingerStart(self.myId, i, 3); //self.myId + 2u64.pow(i-1);
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
        let id = fingerStart(self.myId, m, 3);//self.myId + 2u64.pow((m-1).into());
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
        if let Some(init_params) = &self.init {
            if let Some(first_message) = &init_params.firstMessage {
                _o.send(first_message.0, first_message.1);
            }
            NodeState{
                predecessor : init_params.predecessor,
                behavior : &"internalReceive",
                fingerTable : init_params.fingerTable.clone()
            }
        }
        else if let Some(peer_id) = self.bootstrap_to_id {
            _o.send(peer_id, NodeMessage::FindPredecessor(fingerStart(self.myId, 1, 3), _id, None, None));
            NodeState{
                predecessor : None,
                behavior : &"initializing",
                fingerTable : BTreeMap::new()
            }
        } else {
            let mut ft = BTreeMap::new();
            for i in 0 .. 3 {
                let ind = fingerStart(self.myId, i + 1, 3);
                //let index = self.myId + 2u64.pow(i);
                ft.insert(ind, NodeInfo{refId: _id, circleId: self.myId});
            }            
            _o.set_timer(std::time::Duration::new(5, 0) .. std::time::Duration::new(10, 0)); 
            //model_timeout() ???
            NodeState{
                predecessor : Some(NodeInfo{refId :_id, circleId : self.myId}),
                behavior : &"internalReceive",
                fingerTable : ft
            }
        }
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut Cow<'_, Self::State>,
        o: &mut Out<Self>
    ){
        o.send(id, NodeMessage::Stabilize(1));
        o.send(id, NodeMessage::FixFingers);
        o.set_timer(std::time::Duration::new(5, 0) .. std::time::Duration::new(10, 0));
    }

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>,
              src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        match state.behavior {
            "initializing" => match msg{
                NodeMessage::FoundPredecessor(_, predecessor, predecessor_successor, _, _) => {
                    let mut state = state.to_mut();
                    state.predecessor = Some(predecessor);
                    state.behavior = "internalReceive";
                    state.fingerTable .insert(fingerStart(self.myId, 1, 3), predecessor_successor);
                }
                _ => {}
            }
            "internalReceive" => match msg {
                //find part
                NodeMessage::FindSuccessor(circleId, asker, add_info) => {
                    o.send(_id, NodeMessage::FindPredecessor(circleId, _id,  Some(asker), add_info));
                } 
                NodeMessage::FindPredecessor(id, asker, successor_asker, add_info) => {
                    match state.fingerTable.get(&fingerStart(self.myId, 1, 3)) {
                        Some(value) => 
                        if belongsClockwise00(id, self.myId, value.circleId, 3){
                            o.send(asker, NodeMessage::FoundPredecessor(id, NodeInfo{refId:_id, circleId: self.myId}, *value, successor_asker, add_info))
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
                NodeMessage::FoundPredecessor(_, predecessor, predecessorSuccessor, asker, add_info) => {
                    if let Some(ask) = asker {
                        o.send(ask, NodeMessage::Successor(predecessorSuccessor, predecessor, add_info));
                    }
                }
                //stabilize part
                NodeMessage::Stabilize(k) => {
                    if k <= 3 {
                        let successor_opt = state.fingerTable.get(&fingerStart(self.myId, k, 3));
                        if let Some(successor) = successor_opt {
                            o.send(successor.refId, NodeMessage::GetPredecessor);
                        }
                    }
                }
                NodeMessage::GetPredecessor => {
                    o.send(src, NodeMessage::GotPredecessor(state.predecessor))
                }
                NodeMessage::GotPredecessor(succ_predcessor) => {
                    let im_state = state.clone();
                    let successor_opt = im_state.fingerTable.get(&fingerStart(self.myId, 1, 3));
                    if let Some(successor) = successor_opt {
                        if let Some(x) = succ_predcessor {
                            if belongsClockwise00(x.circleId, self.myId, successor.circleId, 3) {
                                let mut_state = state.to_mut();
                                mut_state.fingerTable.insert(fingerStart(self.myId, 1, 3), x);
                            }
                        }
                        o.send(successor.refId, NodeMessage::Notify(NodeInfo{refId: _id, circleId: self.myId}));
                    }
                }
                NodeMessage::Notify(nsh) => {
                    let im_state = state.clone();
                    if let Some(predecessor) = im_state.predecessor {
                        if belongsClockwise00(nsh.circleId, predecessor.circleId, self.myId, 3) {
                            let mut_state = state.to_mut();
                            mut_state.predecessor = Some(nsh);
                        }
                    }
                }
                //fix part
                NodeMessage::FixFingers => {
                    let mut rng = rand::thread_rng();
                    let i = rng.gen_range(0..3) + 1;
                    let ft_key = fingerStart(self.myId, i, 3);
                    o.send(_id, NodeMessage::FindSuccessor(ft_key, _id, Some(ft_key)));
                }
                NodeMessage::Successor(successor, _, Some(ft_key)) => {
                    let mut_state = state.to_mut();
                    mut_state.fingerTable.insert(ft_key, successor);   
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
                init: Some(InitializingParams{
                    firstMessage: Some((Id::from(0), NodeMessage::FindPredecessor(6, Id::from(0), None, None))),
                    predecessor: None, //потом убрать
                    fingerTable: BTreeMap::from([
                    (1, NodeInfo{refId: Id::from(1), circleId: 1}),
                    (2, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (4, NodeInfo{refId: Id::from(0), circleId: 0})
                    ])//потом убрать
                })
            })
            .actor(NodeActor { bootstrap_to_id: None, myId: 1,
                init: Some(InitializingParams{
                    firstMessage: None,
                    predecessor: None, //потом убрать
                    fingerTable: BTreeMap::from([
                    (2, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (3, NodeInfo{refId: Id::from(2), circleId: 3}),
                    (5, NodeInfo{refId: Id::from(0), circleId: 0})
                    ])//потом убрать
                }) 
            })
            .actor(NodeActor { bootstrap_to_id: None, myId: 3,
                init: Some(InitializingParams{
                    firstMessage: None,
                    predecessor: None, //потом убрать
                    fingerTable: BTreeMap::from([
                        (4, NodeInfo{refId: Id::from(0), circleId: 0}),
                        (5, NodeInfo{refId: Id::from(0), circleId: 0}),
                        (7, NodeInfo{refId: Id::from(0), circleId: 0})
                        ])})//потом убрать
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
            // .property(Expectation::Sometimes, "find succeeds", |_, state| {
            //     state.network.iter_deliverable()
            //         .any(|e| matches!(e.msg, NodeMessage::FoundPredecessor(_, _, _, _, _)/*NodeMessage::FoundPredecessor(6, NodeInfo{refId: _, circleId: 3}, NodeInfo{refId: _, circleId: 0}, _, _)*/))
            // })
            // .record_msg_in(RegisterMsg::record_returns)
            // .record_msg_out(RegisterMsg::record_invocations)
            .checker().spawn_dfs().join();
        //checker.assert_properties(); // TRY IT: Uncomment this line, and the test will fail.
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

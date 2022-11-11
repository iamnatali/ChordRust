use stateright::actor::{*, register::*};
use std::borrow::Cow; // COW == clone-on-write
use std::net::{SocketAddrV4, Ipv4Addr};
use std::collections::BTreeMap;
use rand::Rng;

//Id - тип по которому можно обращаться
type CircleId = u16; //123435, 32432434 (зашит хэш ip)
type FingerId = u16; //1, 2, 3 1<=id<=m

struct InitializingParams{
    predecessor: NodeInfo,
    finger_table: BTreeMap<FingerId, NodeInfo>
}

struct NodeActor {
    bootstrap: Option<(Id, NodeMessage)>,
    circle_id: CircleId,
    init: Option<InitializingParams>
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct NodeInfo {id: Id, circle_id: CircleId}

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

    FindSuccessor(CircleId, Id, Option<CircleId>),//_, _, additional info 
    Successor(NodeInfo, NodeInfo, Option<CircleId>), //result, predecessor, additional info 
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NodeState{
    predecessor: NodeInfo,
    behavior: &'static str,
    finger_table: BTreeMap<FingerId, NodeInfo>
}

fn finger_start(current_node_id: CircleId, finger_number: u16, m:u16) -> u16 {
    (current_node_id + (2u16).pow((finger_number-1).into())).rem_euclid((2u16).pow(m.into()).into())
}

//first node >= finger_start(current_node_id, finger_number, m)
// fn finger_node(current_node_id: CircleId, finger_number: u16, m:u16, ft: &BTreeMap<FingerId, NodeInfo>){
//     let id_start = finger_start(current_node_id, finger_number, m);
//     successor(id_start);
// }

fn belongs_clockwise01(id: CircleId, interval_start: CircleId, interval_end: CircleId, m: u16) -> bool{
    let largest = 2u16.pow(m.into());
    let interval_start_m = interval_start.rem_euclid(largest);
    let interval_end_m = interval_end.rem_euclid(largest);
    let id_m = id.rem_euclid(largest);
    if interval_start_m < interval_end_m {
        interval_start_m < id_m && id_m <= interval_end_m
    }
    else{
        interval_start_m < id_m || id_m <= interval_end_m
    }
}

fn belongs_clockwise00(id: CircleId, interval_start: CircleId, interval_end: CircleId, m: u16) -> bool{
    let largest = 2u16.pow(m.into());
    let interval_start_m = interval_start.rem_euclid(largest);
    let interval_end_m = interval_end.rem_euclid(largest);
    let id_m = id.rem_euclid(largest);
    if interval_start_m < interval_end_m {
        interval_start_m < id_m && id_m < interval_end_m
    }
    else{
        interval_start_m < id_m || id_m < interval_end_m
    }
}


impl NodeActor{
    fn closest_preceding_finger(&self, self_id: Id, id: CircleId, m: u16, ft: &BTreeMap<CircleId, NodeInfo>) -> NodeInfo{
        let mut result: Option<NodeInfo> = None;
        for i in (0..m).rev() {
            match ft.get(&i) {
                Some(node_info) => {
                    if belongs_clockwise00(node_info.circle_id, self.circle_id, id, m){
                        result = Some(*node_info);
                        break;
                    }
                }
                None => {}
            }
        }
        match result {
            Some(res) => {res}
            None => NodeInfo{id: self_id, circle_id: self.circle_id}
        }
    } 
}

impl Actor for NodeActor {
    type Msg = NodeMessage;
    type State = Option<NodeState>;

    fn on_start(&self, _id: Id, _o: &mut Out<Self>) -> Self::State {
        if let Some(init_params) = &self.init {
            if let Some((to, mes)) = &self.bootstrap{
                _o.send(*to, *mes);
            }
            Some(NodeState{
                predecessor : init_params.predecessor,
                behavior : &"internalReceive",
                finger_table : init_params.finger_table.clone()
            })
        } else {None}
    }

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>,
              src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        let mut_state = state.to_mut();
        if let Some(current_state) = mut_state {
            match current_state.behavior {
                "internalReceive" => match msg {
                    //find part
                    NodeMessage::FindSuccessor(circle_id, asker, add_info) => {
                        o.send(_id, NodeMessage::FindPredecessor(circle_id, _id,  Some(asker), add_info));
                    }
                    NodeMessage::FindPredecessor(id, asker, successor_asker, add_info) => {
                        if id == self.circle_id {
                            o.send(asker, NodeMessage::FoundPredecessor(id, current_state.predecessor, NodeInfo{id: _id, circle_id:self.circle_id}, successor_asker, add_info))
                        } else{
                            match current_state.finger_table.get(&1u16) {
                                Some(n_successor) => 
                                //was 00
                                if belongs_clockwise01(id, self.circle_id, n_successor.circle_id, 3){
                                    o.send(asker, NodeMessage::FoundPredecessor(id, NodeInfo{id: _id, circle_id:self.circle_id}, *n_successor, successor_asker, add_info))
                                } else {
                                    let info = self.closest_preceding_finger(_id, id, 3, &current_state.finger_table);
                                    o.send(info.id, msg)
                                }
                                None => {
                                    let n: Option<NodeState> = None; 
                                    *mut_state = n;
                                } 
                            }
                        }
                    }
                    NodeMessage::FoundPredecessor(_, predecessor, predecessor_successor, asker, add_info) => {
                        if let Some(ask) = asker {
                            o.send(ask, NodeMessage::Successor(predecessor_successor, predecessor, add_info));
                        }
                    }
                    _ => {}
                }
                _ => {}
            }
        } else {}
    }
}

// #[cfg(test)]
// mod tests {
//     #[test]
//     fn it_works() {
//         let result = 2 + 2;
//         assert_eq!(result, 4);
//     }
// }

#[cfg(test)]
mod test {
    use super::*;
    use stateright::{*, semantics::*, semantics::register::*};
    use ActorModelAction::Deliver;
    use RegisterMsg::{Get, GetOk, Put, PutOk};

    #[test]
    fn could_find_predecessor() {
        let mut a0 = BTreeMap::new();
        a0.insert(1, NodeInfo{id: Id::from(1), circle_id:1});
        a0.insert(2, NodeInfo{id: Id::from(2), circle_id:3});
        a0.insert(3, NodeInfo{id: Id::from(0), circle_id:0});
        let mut a1 = BTreeMap::new();
        a1.insert(1, NodeInfo{id: Id::from(2), circle_id:3});
        a1.insert(2, NodeInfo{id: Id::from(2), circle_id:3});
        a1.insert(3, NodeInfo{id: Id::from(0), circle_id:0});
        let mut a3 = BTreeMap::new();
        a3.insert(1, NodeInfo{id: Id::from(0), circle_id:0});
        a3.insert(2, NodeInfo{id: Id::from(0), circle_id:0});
        a3.insert(3, NodeInfo{id: Id::from(0), circle_id:0});
        let checker = ActorModel::new(
                (),
                ()
            )
            .actor(NodeActor { bootstrap: Some((Id::from(0), NodeMessage::FindPredecessor(0, Id::from(0), None, None))), circle_id: 0, 
                init: Some(InitializingParams{predecessor: NodeInfo{id: Id::from(2), circle_id:3},
                finger_table: a0
            })
            })
            .actor(NodeActor { bootstrap: None, circle_id: 1,
                init: Some(InitializingParams{predecessor: NodeInfo{id: Id::from(0), circle_id:0},
                finger_table: a1
            })
            })
            .actor(NodeActor { bootstrap: None, circle_id: 3,
                init: Some(InitializingParams{predecessor: NodeInfo{id: Id::from(1), circle_id:1},
                finger_table: a3
            })
            })
            // .property(Expectation::Always, "linearizable", |_, state| {
            //     state.history.serialized_history().is_some()
            // })
            .property(Expectation::Sometimes, "find succeeds", |_, state| {
                let c = state.network.len();
                print!("wow {}", c);
                state.network.iter_deliverable()
                    .any(|e| matches!(e.msg, NodeMessage::FoundPredecessor(_, _, _, _, _)/*NodeMessage::FoundPredecessor(6, NodeInfo{refId: _, circleId: 3}, NodeInfo{refId: _, circleId: 0}, _, _)*/))
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


// #[cfg(test)]
// mod test {
//     use super::*;
//     use stateright::{*, semantics::*, semantics::register::*};
//     use ActorModelAction::Deliver;
//     use RegisterMsg::{Get, GetOk, Put, PutOk};

//     #[test]
//     fn could_find_predecessor() {
//         let checker = ActorModel::new(
//                 (),
//                 ()
//             )
//             .actor(NodeActor { bootstrap_to_id: Some(Id::from(0)), myId: 0, 
//                 init: None
//             })
//             .actor(NodeActor { bootstrap_to_id: None, myId: 1,
//                 init: None
//             })
//             .actor(NodeActor { bootstrap_to_id: None, myId: 3,
//                 init: None
//             })
//             // .actor(RegisterActor::Server(ServerActor))
//             // .actor(RegisterActor::Client { put_count: 2, server_count: 1 })
//             // .property(Expectation::Always, "linearizable", |_, state| {
//             //     state.history.serialized_history().is_some()
//             // })
//             // .property(Expectation::Sometimes, "get succeeds", |_, state| {
//             //     state.network.iter_deliverable()
//             //         .any(|e| matches!(e.msg, RegisterMsg::GetOk(_, _)))
//             // })
//             // .property(Expectation::Sometimes, "find succeeds", |_, state| {
//             //     state.network.iter_deliverable()
//             //         .any(|e| matches!(e.msg, NodeMessage::FoundPredecessor(_, _, _, _, _)/*NodeMessage::FoundPredecessor(6, NodeInfo{refId: _, circleId: 3}, NodeInfo{refId: _, circleId: 0}, _, _)*/))
//             // })
//             // .record_msg_in(RegisterMsg::record_returns)
//             // .record_msg_out(RegisterMsg::record_invocations)
//             .checker().spawn_dfs().join();
//         //checker.assert_properties(); // TRY IT: Uncomment this line, and the test will fail.
//         // checker.assert_discovery("linearizable", vec![
//         //     Deliver { src: Id::from(0), dst: Id::from(0), msg: NodeMessage::FindPredecessor(6, Id::from(0), None)},

//         // ]);
//         // checker.assert_discovery("linearizable", vec![
//         //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(1, 'A') },
//         //     Deliver { src: Id::from(0), dst: Id::from(1), msg: PutOk(1) },
//         //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(2, 'Z') },
//         //     Deliver { src: Id::from(0), dst: Id::from(1), msg: PutOk(2) },
//         //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Put(1, 'A') },
//         //     Deliver { src: Id::from(1), dst: Id::from(0), msg: Get(3) },
//         //     Deliver { src: Id::from(0), dst: Id::from(1), msg: GetOk(3, 'A') },
//         // ]);
//     }
// }

fn main() {
    // env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    // spawn(
    //     serde_json::to_vec,
    //     |bytes| serde_json::from_slice(bytes),
    //     vec![
    //         (SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3000), ServerActor)
    //     ]).unwrap();
}

use stateright::actor::{*, register::*};
use std::borrow::Cow;
use std::net::{SocketAddrV4, Ipv4Addr};
use std::collections::BTreeMap;
use rand::Rng;
use stateright::actor::register::{
    RegisterActor, RegisterMsg, RegisterMsg::*};
use std::sync::Arc;

//Id - тип по которому можно обращаться
type CircleId = u16; //123435, 32432434 (зашит хэш ip)
type FingerId = u16; //1, 2, 3 1<=id<=m
type RequestId = u64; //для registerActor
type Value = char; //для registerActor

enum InitializingParams{
    ToJoin,
    Joined{predecessor: NodeInfo, finger_table: BTreeMap<FingerId, NodeInfo>},
    ToDie{predecessor: NodeInfo, finger_table: BTreeMap<FingerId, NodeInfo>}
}

struct NodeActor {
    circle_id: CircleId,
    init: InitializingParams
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct NodeInfo {id: Id, circle_id: CircleId}

type PredecessorAsker = Id;
type SuccessorAsker = Id;
type RegisterAsker = Id;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
enum NodeMessage{
    Stabilize,
    GetPredecessor,
    Predecessor(Option<NodeInfo>),
    Notify(NodeInfo),

    FixFingers,

    StartJoin(
        Id, //known_node
        Option<(RegisterAsker, RequestId)> //register_asker
    ), 

    FindPredecessor(
        CircleId, //queried_id
        PredecessorAsker, //asker
        Option<SuccessorAsker>, //successor_asker
        Option<(RegisterAsker, RequestId)>, //register_asker
        Option<FingerId> //additional info
    ),
    FoundPredecessor(
        CircleId, //queried_id
        NodeInfo, //predecessor
        NodeInfo,//predecessor_successor
        Option<SuccessorAsker>,//successor_asker
        Option<(RegisterAsker, RequestId)>, //register_asker
        Option<FingerId> //additional info
    ),

    FindSuccessor(CircleId, Option<SuccessorAsker>, Option<(RegisterAsker, RequestId)>, Option<FingerId>),//queried_id, asker, register_node, additional info 
    FoundSuccessor(NodeInfo, NodeInfo, Option<FingerId>), //result, predecessor, additional info

    AliveCheck,
    IsAlive
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Behavior{
    StartJoin,
    StartJoinFoundPredecessor,
    InternalReceive,
    FixFingersFoundSuccessor,
    StabilizePredecessor,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NodeState{
    predecessor: Option<NodeInfo>,
    behavior: Behavior,
    finger_table: BTreeMap<FingerId, NodeInfo>,
}

fn finger_start(current_node_id: CircleId, finger_number: u16, m:u16) -> u16 {
    (current_node_id + (2u16).pow((finger_number-1).into())).rem_euclid((2u16).pow(m.into()).into())
}

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
    type Msg = RegisterMsg<RequestId, Value, NodeMessage>;
    type State = Option<NodeState>;

    fn on_start(&self, _id: Id, _o: &mut Out<Self>) -> Self::State {
        match &self.init {
            InitializingParams::ToJoin => {
                None
            }
            InitializingParams::Joined{predecessor: init_predecessor, finger_table: init_finger_table}|
            InitializingParams::ToDie{predecessor: init_predecessor, finger_table: init_finger_table}
            => {
                _o.set_timer(model_timeout());
                //_o.set_timer(std::time::Duration::new(5, 0) .. std::time::Duration::new(10, 0)); 
                Some(NodeState{
                    predecessor : Some(*init_predecessor),
                    behavior : Behavior::InternalReceive,
                    finger_table : (*init_finger_table).clone(),
                })
            }
        }
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut Cow<'_, Self::State>,
        o: &mut Out<Self>
    ){
        o.send(id, Internal(NodeMessage::Stabilize));
        o.send(id, Internal(NodeMessage::FixFingers));
        o.set_timer(model_timeout());
        //o.set_timer(std::time::Duration::new(5, 0) .. std::time::Duration::new(10, 0));
    }

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>,
              src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        let mut_state = state.to_mut();
        if let Some(current_state) = mut_state.as_mut() {
            match current_state.behavior {
                Behavior::StartJoin => match msg {
                    Internal(NodeMessage::StartJoin(id, register_asker)) => 
                        {
                            current_state.behavior = Behavior::StartJoinFoundPredecessor;
                            o.send(id, Internal(
                                NodeMessage::FindPredecessor(self.circle_id, _id, None, register_asker, None)
                            ))
                        }
                    _ => {}
                }
                Behavior::StartJoinFoundPredecessor => {
                    match msg {
                        Internal(NodeMessage::FoundPredecessor(_, predecessor, predecessor_successor, _, register_node, _)) =>
                        {
                            current_state.predecessor = Some(predecessor);
                            current_state.finger_table.insert(1, predecessor_successor);
                            current_state.behavior = Behavior::InternalReceive;
                            o.set_timer(model_timeout()); 
                            //o.set_timer(std::time::Duration::new(5, 0) .. std::time::Duration::new(10, 0)); 
                            if let Some(r_asker) = register_node {
                                o.send(r_asker.0, PutOk(r_asker.1));
                            }
                        }
                        _ => {}
                    }
                }
                Behavior::FixFingersFoundSuccessor => {
                    match msg {
                        Internal(NodeMessage::FoundSuccessor(successor, _, Some(i))) =>{
                            current_state.behavior = Behavior::InternalReceive;
                            current_state.finger_table.insert(i, successor);
                        }
                        _ => {} 
                    }
                }
                Behavior::StabilizePredecessor => {
                    match msg {
                        Internal(NodeMessage::Predecessor(predecessor_opt)) => {
                            let successor: NodeInfo = current_state.finger_table[&1];
                            if let Some(predecessor) = predecessor_opt{
                                if belongs_clockwise00(predecessor.circle_id, self.circle_id, successor.circle_id, 3){
                                    current_state.finger_table.insert(1, predecessor);
                                    o.send(predecessor.id, 
                                         Internal(NodeMessage::Notify(NodeInfo{id:_id, circle_id:self.circle_id})));
                                } else {
                                    o.send(successor.id, 
                                         Internal(NodeMessage::Notify(NodeInfo{id:_id, circle_id:self.circle_id})));
                                }
                            } else {
                                o.send(successor.id, 
                                     Internal(NodeMessage::Notify(NodeInfo{id:_id, circle_id:self.circle_id})));
                            }
                            current_state.behavior = Behavior::InternalReceive;
                        }
                        _ => {}
                    }
                }
                Behavior::InternalReceive => match msg {
                    Internal(NodeMessage::FixFingers) => {
                        let mut rng = rand::thread_rng();
                        let i = 2;//rng.gen_range(1..3) + 1; //2, 3
                        let start_circle_id = finger_start(self.circle_id, i, 3);
                        current_state.behavior = Behavior::FixFingersFoundSuccessor;
                        o.send(_id,
                            Internal(NodeMessage::FindSuccessor(start_circle_id, Some(_id), None, Some(i))));
                    }

                    Internal(NodeMessage::Stabilize) => {
                        let successor: NodeInfo = current_state.finger_table[&1];
                        current_state.behavior = Behavior::StabilizePredecessor;
                        o.send(successor.id, Internal(NodeMessage::GetPredecessor));
                    }
                    Internal(NodeMessage::GetPredecessor) => {
                        o.send(src, Internal(NodeMessage::Predecessor(current_state.predecessor)));
                    }
                    Internal(NodeMessage::Notify(maybe_predecessor)) => {
                        if let Some(pred) = current_state.predecessor{
                            if belongs_clockwise00(maybe_predecessor.circle_id, pred.circle_id, self.circle_id, 3){
                                current_state.predecessor = Some(maybe_predecessor);
                            }
                        } else {
                            current_state.predecessor = Some(maybe_predecessor);
                        }
                    }

                    //сделать так, чтобы значение действительно хранилось
                    Get(request_id) => {
                        o.send(src, GetOk(request_id, 'A'));
                    }
                    Put(request_id, value) => {
                        match self.init {
                            InitializingParams::ToDie { predecessor:_, finger_table:_ } => {
                                *mut_state = None;
                                o.cancel_timer();
                                o.send(src, PutOk(request_id));
                            }
                            _ => {o.send(src, PutOk(request_id));}
                        }
                        //let n_value = (request_id as u16).rem_euclid(2u16.pow(3));
                        //o.send(_id, Internal(NodeMessage::FindSuccessor(5, None, Some((src, request_id)), None)));
                    }
                    Internal(NodeMessage::FindSuccessor(circle_id, asker, register_node, add_info)) => {
                        o.send(_id, Internal(NodeMessage::FindPredecessor(circle_id, _id,  asker, register_node, add_info)));
                    }
                    Internal(NodeMessage::FindPredecessor(id, asker, successor_asker, register_node, add_info)) => {
                        match current_state.finger_table.get(&1u16) {
                            Some(n_successor) => 
                                if belongs_clockwise01(id, self.circle_id, n_successor.circle_id, 3){
                                    o.send(asker, Internal(NodeMessage::FoundPredecessor(id, NodeInfo{id: _id, circle_id:self.circle_id}, *n_successor, successor_asker, register_node, add_info)))
                                } else {
                                    let info = self.closest_preceding_finger(_id, id, 3, &current_state.finger_table);
                                    o.send(info.id, msg)
                                }
                            None => {
                                    *mut_state = None;
                            } 
                        }
                    }
                    Internal(NodeMessage::FoundPredecessor(_, predecessor, predecessor_successor, successor_asker, register_node, add_info)) => {
                        if let Some(asker) = successor_asker {
                            o.send(asker, Internal(NodeMessage::FoundSuccessor(predecessor_successor, predecessor, add_info)));
                        }
                        if let Some(r_asker) = register_node {
                            o.send(r_asker.0, PutOk(r_asker.1));
                        }
                    }
                    _ => {}
                }
                _ => {}
            }
        } else {
            match msg {
                Put(request_id, _) => {
                    *mut_state = Some(NodeState{
                        predecessor : None,
                        behavior : Behavior::StartJoin,
                        finger_table : BTreeMap::new(),
                    });
                    o.send(_id, Internal(NodeMessage::StartJoin(Id::from(0), Some((src, request_id)))));
                }
                _ => {}
            }
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
    fn check_chord() {
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
        let model = ActorModel::new(
                (),
                ()
            )
            .actor(RegisterActor::Server(NodeActor {circle_id: 0, 
                init: InitializingParams::Joined{predecessor: NodeInfo{id: Id::from(2), circle_id:3},
                finger_table: a0
            }
            }))
            .actor(RegisterActor::Server(NodeActor { circle_id: 1,
                init: InitializingParams::Joined{predecessor: NodeInfo{id: Id::from(0), circle_id:0},
                finger_table: a1
            }
            }))
            .actor(RegisterActor::Server(NodeActor { circle_id: 3,
                init: InitializingParams::ToDie{predecessor: NodeInfo{id: Id::from(1), circle_id:1},
                finger_table: a3
            }
            }))
            .actor(RegisterActor::Server(NodeActor { circle_id: 6,
                init: InitializingParams::ToJoin,
            }  
            ))
            .actors((0..1)
                    .map(|_| RegisterActor::Client {
                        put_count: 3,
                        server_count: 3,
                    }))
            //проверить, что все ft eventually указывают на верные ноды
            .property(Expectation::Eventually, "is eventually Ideal", |model, state| {
                let actors_circle_ids = &model.actors.iter().flat_map(|a| match a {
                    RegisterActor::Server(NodeActor{circle_id: id, init: _}) => {vec![id]}
                    _ => {vec![]}
                }).collect::<Vec<&u16>>();
                let actors_nonempty_states_indexes = &state.actor_states.iter().enumerate().flat_map(|(i, state)| match &**state{
                    RegisterActorState::Server(Some(_)) => {vec![i]}
                    _ => {vec![]} 
                }).collect::<Vec<usize>>();
                print!("wow {}\n", actors_nonempty_states_indexes.len());
                let actors_vec = actors_circle_ids
                .iter()
                .enumerate()
                .filter(|(i, _)| actors_nonempty_states_indexes.contains(i))
                .map(|(_, id)| **id).collect::<Vec<u16>>();
                let actor_states = &state.actor_states;
                let mut i = 0;
                let mut result = true;
                for state in actor_states.iter() {
                    match &**state {
                        RegisterActorState::Server(Some(NodeState{predecessor:p, behavior:_, finger_table:ft})) => 
                        {
                            if let Some(pred) = p {
                                print!("i {}\n", i);
                                let predecessor = pred.circle_id;
                                print!("pred {}\n", predecessor);
                                if i==0 {
                                    result = result && predecessor == actors_vec[actors_vec.len()-1];
                                    print!("res {}\n", result);
                                    print!("actor {}\n", actors_vec[actors_vec.len()-1]);
                                } else {
                                    result = result && predecessor == actors_vec[i-1];
                                    print!("res {}\n", result);
                                    print!("actor {}\n", actors_vec[i-1]);
                                }
                            } else {result = false;}
                            let finger_list = Vec::from_iter(ft.values());
                            if finger_list.len()>0{
                                let successor = finger_list[0].circle_id;
                                if i==(actors_vec.len()-1) {
                                    result = result && successor == actors_vec[0];
                                } else {
                                    result = result && successor == actors_vec[i+1];
                                }
                            } else {result = false;}
                            // let mut int_i = 1;
                            // for entry in finger_list.iter(){
                            //     let start = finger_start(actors_vec[i], int_i, 3);
                            //     let real_succ = actors_vec.iter().find(|&&id| id>= start);
                            // }
                        }
                        _ => {}
                    }
                    i +=1;
                }
                print!("evevn res {}\n", result);
                false
            })//;
            //проверить, что на все запросы правильно ответили
            // .property(Expectation::Always, "finds success", |model, state| {
            //     state.network.iter_deliverable()
            //         .any(|e| matches!(e.msg, Internal(NodeMessage::FoundPredecessor(_, _, _, _, _, _))))
            // })
            .property(Expectation::Sometimes, "joins", |model, state| {
                state.network.iter_deliverable()
                    .any(|e| matches!(e.msg, Internal(NodeMessage::StartJoin(_, _))))
            });
        //let _ = model.checker().serve("localhost:3000").assert_properties();
        model.checker()
        //.target_state_count(10)
        .spawn_bfs()
        .report(&mut std::io::stdout().lock())
        .join()
        .assert_properties();
    }
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let mut a3 = BTreeMap::new();
        a3.insert(1, NodeInfo{id: Id::from(0), circle_id:0});
        a3.insert(2, NodeInfo{id: Id::from(0), circle_id:0});
        a3.insert(3, NodeInfo{id: Id::from(0), circle_id:0});
    spawn(
        serde_json::to_vec,
        |bytes| serde_json::from_slice(bytes),
        vec![
            (SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3000), (NodeActor { circle_id: 3,
                init: InitializingParams::Joined{predecessor: NodeInfo{id: Id::from(1), circle_id:1},
                finger_table: a3
            }
            }))
        ]).unwrap();
}

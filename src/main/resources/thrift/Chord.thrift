namespace java br.com.arthurcruzdev.jchord
/*
* Chord NodeInfo structure definition
*/
struct NodeInfo{
    1:i64 id,
    2:string ip,
    3:i32 port
}
/*
* Chord Node structure definition
*/
//struct Node{
//    1:i64 id,
//    2:NodeInfo nodeInfo,
//    3:list<NodeInfo> fingerTable,
//    4:NodeInfo predecessor
//}
//Chord Protocol Exceptions
exception UnableToJoinChordException{
    1:i32 errNum = 0,
    2:string errMsg = "An error occurred and the node was unable to join the Chord."
}
exception UnableToFindSuccessorException{
    1:i32 errNum = 1,
    2:string errMsg = "An error occurred and the node was unable to find the key's successor."
}
exception UnableToFindPredecessorException{
    1:i32 errNum = 2,
    2:string errMsg = "An error occurred and the node was unable to find the key's predecessor."
}
exception UnableToFindClosestPrecedingFingerException{
    1:i32 errNum = 3,
    2:string errMsg = "An error occurred and the node was unable to find the closest preceding finger."
}
exception UnableToTransferKeysException{
    1:i32 errNum = 4,
    2:string errMsg = "An error occurred and the node was unable to transfer it's keys."
}
//exception UnableToStabilizeException{
//    1:i32 errNum = 5,
//    2:string errMsg = "An error occurred while executing stabilization protocol."
//}
exception UnableToNotifyException{
    1:i32 errNum = 6,
    2:string errMsg = "The node was unable send a notification to other node."
}
//exception UnableToFixFingerException{
//    1:i32 errNum = 7,
//    2:string errMsg = "An error occurred while refreshing the finger table."
//}
//exception UnableToSendSelfException{
//    1:i32 errNum = 8,
//    2:string errMsg = "The node was unable to send its data."
//}
//exception UnableToSetPredecessorException{
//    1:i32 errNum = 9,
//    2:string errMsg = "The node was unable to set its predecessor."
//}
/*
* Chord Protocol Operations
*/
service Chord{
    void join(1:NodeInfo n) throws(1:UnableToJoinChordException ex),
    NodeInfo findSuccessor(1:i64 id) throws(1:UnableToFindSuccessorException ex),
    NodeInfo findPredecessor(1:i64 id) throws(1:UnableToFindPredecessorException ex),
    NodeInfo closestPrecedingFinger(1:i64 id) throws(1:UnableToFindClosestPrecedingFingerException ex),
    list<binary> transferKeys(1:NodeInfo n) throws(1:UnableToTransferKeysException ex),
    void notify(1:NodeInfo n) throws(1:UnableToNotifyException ex),
}
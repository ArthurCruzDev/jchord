package br.com.arthurcruzdev.jchord.handler;

import br.com.arthurcruzdev.jchord.thrift.*;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

public class ChordHandler implements Chord.Iface{

    @Override
    public void join(NodeInfo n) throws UnableToJoinChordException, TException {

    }

    @Override
    public NodeInfo findSuccessor(long id) throws UnableToFindSuccessorException, TException {
        return null;
    }

    @Override
    public NodeInfo findPredecessor(long id) throws UnableToFindPredecessorException, TException {
        return null;
    }

    @Override
    public NodeInfo closestPrecedingFinger(long id) throws UnableToFindClosestPrecedingFingerException, TException {
        return null;
    }

    @Override
    public List<ByteBuffer> transferKeys(NodeInfo n) throws UnableToTransferKeysException, TException {
        return null;
    }

    @Override
    public void notify(NodeInfo n) throws UnableToNotifyException, TException {

    }
}

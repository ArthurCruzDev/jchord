package br.com.arthurcruzdev;

import br.com.arthurcruzdev.thrift.*;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

public class ChordHandler implements Chord.Iface{
    @Override
    public void join(Node n) throws UnableToJoinChordException, TException {

    }

    @Override
    public Node findSuccessor(long id) throws UnableToFindSuccessorException, TException {
        return null;
    }

    @Override
    public Node findPredecessor(long id) throws UnableToFindPredecessorException, TException {
        return null;
    }

    @Override
    public Node closestPrecedingFinger(long id) throws UnableToFindClosestPrecedingFingerException, TException {
        return null;
    }

    @Override
    public List<ByteBuffer> transferKeys(Node n) throws UnableToTransferKeysException, TException {
        return null;
    }

    @Override
    public void notify(Node n) throws UnableToNotifyException, TException {

    }
}

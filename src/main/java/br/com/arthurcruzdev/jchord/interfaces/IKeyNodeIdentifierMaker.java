package br.com.arthurcruzdev.jchord.interfaces;

import br.com.arthurcruzdev.jchord.thrift.NodeInfo;

import java.security.NoSuchAlgorithmException;

public interface IKeyNodeIdentifierMaker {
    public static final int NUM_BITS = 63;
    public long getIdentifier(NodeInfo nodeInfo) throws NoSuchAlgorithmException;
    public long getIdentifier(String key) throws NoSuchAlgorithmException;

}

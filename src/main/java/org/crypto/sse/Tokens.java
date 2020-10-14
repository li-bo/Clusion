package org.crypto.sse;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mjq on 2020/9/8.
 */
public class Tokens implements Serializable {

    public byte[][] tokenMMGlobal;
    public byte[] tokenDIC;
    public List<byte[][]> tokenMMLocal = new ArrayList<byte[][]>();

    public Tokens(List<String> subSearch, List<byte[]> listOfkeys) throws UnsupportedEncodingException {
        this.tokenMMGlobal = CassandraLsm.token(listOfkeys.get(0), subSearch.get(0));
        this.tokenDIC = CryptoPrimitives.generateCmac(listOfkeys.get(1), 3 + subSearch.get(0));
        for (int i = 1; i < subSearch.size(); i++) {
            tokenMMLocal.add(
                    CassandraLsm.token(CryptoPrimitives.generateCmac(listOfkeys.get(0), subSearch.get(0)), subSearch.get(i)+"1"));
        }
    }

    public byte[][] getTokenMMGlobal() {
        return tokenMMGlobal;
    }

    public void setTokenMMGlobal(byte[][] tokenMMGlobal) {
        this.tokenMMGlobal = tokenMMGlobal;
    }

    public byte[] getTokenDIC() {
        return tokenDIC;
    }

    public void setTokenDIC(byte[] tokenDIC) {
        this.tokenDIC = tokenDIC;
    }

    public List<byte[][]> getTokenMMLocal() {
        return tokenMMLocal;
    }

    public void setTokenMMLocal(List<byte[][]> tokenMMLocal) {
        this.tokenMMLocal = tokenMMLocal;
    }
}

package org.crypto.sse;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by mjq on 2020/9/8.
 */
public class IEXCassandra implements Serializable {
    CassandraLsm globalMM = null;
    CassandraLsm[] localMultiMap = null;;
    Multimap<String, Integer> dictionaryForMM = null;
    public static long numberPairs = 0;
    static double filterParameter = 0;

    public IEXCassandra(CassandraLsm globalMM, CassandraLsm[] localMultiMap, Multimap<String, Integer> dictionaryForMM) {
        this.globalMM = globalMM;
        this.localMultiMap = localMultiMap;
        this.dictionaryForMM = dictionaryForMM;
    }
    public CassandraLsm getGlobalMM(){
        return getGlobalMM();
    }
    public CassandraLsm[] getLocalMultiMap() {
        return localMultiMap;
    }
    public Multimap<String, Integer> getDictionaryForMM() {
        return dictionaryForMM;
    }
    public void setDictionaryForMM(Multimap<String, Integer> dictionaryForMM) {
        this.dictionaryForMM = dictionaryForMM;
    }

    // ***********************************************************************************************//

    ///////////////////// Key Generation /////////////////////////////

    // ***********************************************************************************************//
    public static List<byte[]> keyGen(int keySize, String password, String filePathString, int icount)
            throws InvalidKeySpecException, NoSuchAlgorithmException, NoSuchProviderException {
        List<byte[]> listOfkeys = new ArrayList<byte[]>();
        // Generation of two keys for Secure inverted index
        listOfkeys.add(TSet.keyGen(keySize, password + "secureIndex", filePathString, icount));
        listOfkeys.add(TSet.keyGen(keySize, password + "dictionary", filePathString, icount));

        // Generation of one key for encryption
        //ZMF跟布尔查询有关了 先不要下面这个操作貌似也行
        //listOfkeys.add(ZMF.keyGenSM(keySize, password + "encryption", filePathString, icount));

        return listOfkeys;
    }

    // ***********************************************************************************************//

    ///////////////////// Setup /////////////////////////////

    // ***********************************************************************************************//
    public static IEXCassandra setup(DocumentRepository dr, CassandraLsm clsm, List<byte[]> keys, Multimap<String, String> lookup1, Multimap<String, String> lookup2,
                                     int blockSize, int arrayBlock, int dataSize) throws InterruptedException, ExecutionException, IOException, NoSuchProviderException, InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
        // Instantiation of the object that contains Global MM, Local MMs and
        // the dictionary

        CassandraLsm[] localMultiMap = new CassandraLsm[lookup1.keySet().size()];
        Multimap<String, Integer> dictionaryForMM = ArrayListMultimap.create();

        int counter = 0;

        ///////////////////// Computing Filtering Factor and exact needed data
        ///////////////////// size/////////////////////////////
        //histogram不知是干嘛用的
        HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
        Printer.debugln("Number of documents " + lookup2.keySet().size());
//        for (String keyword : lookup1.keySet()) {
//            if (histogram.get(lookup1.get(keyword).size()) != null) {
//                int tmp = histogram.get(lookup1.get(keyword).size());
//                histogram.put(lookup1.get(keyword).size(), tmp + 1);
//            } else {
//                histogram.put(lookup1.get(keyword).size(), 1);
//            }
//
//            if (dataSize < lookup1.get(keyword).size()) {
//                dataSize = lookup1.get(keyword).size();
//            }
//
//        }
        // Construction of the global multi-map
        Printer.debugln("\nBeginning of Global MM creation \n");

        long startTime1 = System.nanoTime();
        IEXCassandra disj2 = new IEXCassandra(clsm, localMultiMap, dictionaryForMM);
        long endTime1 = System.nanoTime();
        //numberPairs变量也不知道是干嘛是
//        numberPairs = numberPairs + lookup1.size();

        // Construction of the local multi-map
        Printer.debugln("Start of Local Multi-Map construction");

        long startTime = System.nanoTime();

        for (String keyword : lookup1.keySet()) {
            // Filter setting optional. For a setup without any filtering set
            // filterParameter to 0
            if (((double) lookup1.get(keyword).size() / TextExtractPar.maxTupleSize > filterParameter)) {
//                Printer.debugln("Keyword in LMM " + keyword);

                // First computing V_w. Determine Doc identifiers
                Set<String> VW = new TreeSet<String>();
                for (String idDoc : lookup1.get(keyword)) {
                    VW.addAll(lookup2.get(idDoc));
                }

                Multimap<String, String> secondaryLookup = ArrayListMultimap.create();
                // here we are only interested in documents in the intersection
                // between "keyword" and "word"

                Collection<String> l2 = new ArrayList<String>(lookup1.get(keyword));
                for (String word : VW) {
                    // Filter setting optional. For a setup without any
                    // filtering set filterParameter to 0
                    if (word!=keyword && ((double) lookup1.get(word).size() / TextExtractPar.maxTupleSize > filterParameter)) {
                        Collection<String> l1 = new ArrayList<String>(lookup1.get(word));

                        //只保留l1中在l2中出现的部分
                        //System.out.println(word+ "**"+keyword+"   "+l1 + "kaishi" + l2);
                        l1.retainAll(l2);
                        //System.out.println(l1 + "zheli");
                        //得到word和keyword的交集l1
                        //加1主要是想同之前的globalMM的结果中的相同word做区分 这里的word是指有交集的word（localMM中的）
                        //System.out.println(secondaryLookup.get(word + counter).isEmpty());
                        if(secondaryLookup.get(word+"1").isEmpty()){//其实这一步都可以不用要的吧！！
                            for(String str:l1){
                                secondaryLookup.put(word+"1",str);
                            }
                        }
                            //secondaryLookup.putAll(word + keyword, l1);

                    }
                }
//                for(String kk:secondaryLookup.keySet())
//                    System.out.println(kk+"***"+secondaryLookup.get(kk));
                // End of VW construction
                disj2.getLocalMultiMap()[counter] = CassandraLsm.constructlsm(dr,
                        CryptoPrimitives.generateCmac(keys.get(0), keyword), secondaryLookup, blockSize, arrayBlock, dataSize);


                byte[] key3 = CryptoPrimitives.generateCmac(keys.get(1), 3 + keyword);
                numberPairs = numberPairs + secondaryLookup.size();
                dictionaryForMM.put(new String(key3), counter);
            }
            counter++;
        }
        disj2.setDictionaryForMM(dictionaryForMM);
        return disj2;
    }

    // ***********************************************************************************************//

    ///////////////////// Search Token Generation /////////////////////////////

    // ***********************************************************************************************//

    public static List<Tokens> token(List<byte[]> listOfkeys, List<String> search)
            throws UnsupportedEncodingException {
        List<Tokens> token = new ArrayList<Tokens>();

        for (int i = 0; i < search.size(); i++) {

            List<String> subSearch = new ArrayList<String>();
            // Create a temporary list that carry keywords in *order*
            for (int j = i; j < search.size(); j++) {
                subSearch.add(search.get(j));
            }

            token.add(new Tokens(subSearch, listOfkeys));
        }
        return token;
    }

    // ***********************************************************************************************//

    ///////////////////// Query /////////////////////////////

    // ***********************************************************************************************//
    //求的是一组关键字的或
    public static Set<String> query(List<String> searchDis, DocumentRepository dr, List<Tokens> token, IEXCassandra disj)
            throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
            NoSuchProviderException, NoSuchPaddingException, IOException {

        //这里的listcont为查询的一组关键字中的w1对应dictionary1的值
        int listcount1 = CassandraLsm.getdictionary1().get(new String(searchDis.get(0)));
        int listcount2 = CassandraLsm.getdictionary1().get(new String(searchDis.get(0) + "1"));

        //System.out.println(listcount1+"**"+listcount2);

        Set<String> finalResult = new TreeSet<String>();
        for (int i = 0; i < token.size(); i++) {
            Set<String> result = new HashSet<String>();
            for (int c = 1; c <= listcount1; c++) {
                result.addAll(CassandraLsm.query(dr, token.get(i).getTokenMMGlobal(), c));
            }
//            System.out.println("jieguo   "+result+" i  "+i+" token  "+token.size());

            //最后一个关键字不用求交，求前q-1个关键字的交就ok
            if (i < (token.size()-1) && !(result.size() == 0)) {
                List<Integer> temp = new ArrayList<Integer>(
                        disj.getDictionaryForMM().get(new String(token.get(i).getTokenDIC())));

                if (!(temp.size() == 0)) {
                    //int pos = temp.get(0); //确实没啥用的样子
                    for (int j = 0; j < token.get(i).getTokenMMLocal().size(); j++) {

                        Set<String> temporary = new HashSet<String>();
                        List<String> tempoList = new ArrayList<>();
                        for (int c = 0; c < listcount2; c++) {
                            //tempoList.addAll(CassandraLsm.query(dr, token.get(i).getTokenMMGlobal(), c));
                            tempoList.addAll(CassandraLsm.query(dr, token.get(i).getTokenMMLocal().get(j), c));

                        }
                        if (!(tempoList == null)) {
                            temporary = new HashSet<String>(tempoList);
                        }
                        //排除
                        result = Sets.difference(result, temporary);
                        if (result.isEmpty()) {
                            break;
                        }
                    }
                }
            }
            finalResult.addAll(result);
        }
        return finalResult;
    }

    public static Set<String> boolSearch(String[][] bool, DocumentRepository dr, List<byte[]> listSK, IEXCassandra disj) throws Exception{
        List<String> searchDis = new ArrayList<String>();
        for (int i = 0; i < bool[0].length; i++) {
            searchDis.add(bool[0][i]);
        }
        Set<String> tmp = new HashSet<String>();
        //集合本身就是不会重复的 后边可以改改集合 看如果不是集合 得到的结果有没有重复的值
        tmp.addAll(IEXCassandra.query(searchDis, dr, IEXCassandra.token(listSK, searchDis), disj));
        if(tmp.isEmpty()){
            System.out.println("布尔查询没有意义了，或者在这里输出空，后边再弄吧");
        }
        //接着是布尔查询

        for (int i = 1; i < bool.length; i++) {
            Set<String> finalResult = new HashSet<String>();
            //下面相当于是德尔塔2到L的部分了 求与德尔塔1中各关键字的交 做筛选删除
            for (int k = 0; k < bool[0].length; k++) {
                List<String> searchTMP = new ArrayList<String>();
                searchTMP.add(bool[0][k]);
                //int c = CassandraLsm.dictionary2.get(bool[0][k]+"1");
                for (int r = 0; r < bool[i].length; r++) {
                    searchTMP.add(bool[i][r]);
                }
                //求searchTMP中第一个关键字和剩余关键字的交 直接localMM全部查询还不行 是需要的才查

                ////我要用tokenTMP来做筛选删除 求交的结果集 生成了所需的所有token
                List<Tokens> tokenTMP = IEXCassandra.token(listSK, searchTMP);
                //不知这要干嘛
//                    Set<String> result = new HashSet<String>(IEXCassandra.query(, , tokenTMP.get(0).getTokenMMGlobal(),
//                            disj));
                //tmp是德尔塔1的一组或查询结果
                if (!(tmp.size() == 0)) {
                    //这里能不能用int来替代呢？？
                    List<Integer> temp = new ArrayList<Integer>(
                            disj.getDictionaryForMM().get(new String(tokenTMP.get(0).getTokenDIC())));
                    //
                    //System.out.println("~~~+++++"+CassandraLsm.query(dr, tokenTMP.get(0).getTokenMMLocal().get(0), 1));

                    if (!(temp.size() == 0)) {
                        int pos = temp.get(0);
                        //disj.getLocalMultiMap()[pos];
                        //这里的lcount1 和lcount2怎么个求法？对应是
                        int c2 = CassandraLsm.dictionary1.get(new String(bool[0][k] + "1"));
                        List<String> tempoList = new ArrayList<>();
                        //System.out.println("c2  "+c2);
                        //searchTMP
                        //System.out.println(searchTMP.size()+"~~~~"+tokenTMP.get(0).getTokenMMLocal().size());
                        for(int r = 0; r < searchTMP.size()-1; r++) {
                            for (int c = 1; c <= c2; c++) {
                                //这里边的参数是啥
                                //System.out.println(c + "  +++++++  " + CassandraLsm.query(dr, tokenTMP.get(0).getTokenMMLocal().get(0), c));
                                tempoList.addAll(CassandraLsm.query(dr, tokenTMP.get(0).getTokenMMLocal().get(0), c));
                                //System.out.println(tempoList.size()+"+++++"+CassandraLsm.query(dr, tokenTMP.get(0).getTokenMMLocal().get(0), c));
                            }
                            finalResult.addAll(tempoList);
                        }
                    }
                }
            }
            tmp.retainAll(finalResult);
        }

        return tmp;
    }
}

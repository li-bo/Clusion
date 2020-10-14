package org.crypto.sse;

import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Multimap;
import javafx.util.Pair;

import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mjq on 2020/8/16.
 */
public class CassandraLsm {

    // define the number of character that a file identifier can have
    public static int sizeOfFileIdentifer = 40;
    public static SecureRandom random = new SecureRandom();

    //维系的词典 dictionary1用来记录关键字对应的计数器值c
    public static Map<String, Integer> dictionary1 = new HashMap<>();
    //dictionary2用来记录l对应当前存储的块数t
    public static Map<byte[], Integer> dictionary2 = new HashMap<>();
    //final Multimap<String, byte[]> dictionary = ArrayListMultimap.create();

    // static byte[][] array = null;
    public static Map<String, Integer> getdictionary1() {
        return dictionary1;
    }

    public CassandraLsm(Map<String, Integer> dictionary1, Map<byte[], Integer> dictionary2) {
        this .dictionary1 = dictionary1;
        this.dictionary2 = dictionary2;
        //System.out.println("shibasssss"+dictionary1.get("car"));
        //this.array = array;
    }
    // ***********************************************************************************************//

    ///////////////////// Key Generation /////////////////////////////

    // ***********************************************************************************************//

    public static byte[] keyGen(int keySize, String password, String filePathString, int icount)
            throws InvalidKeySpecException, NoSuchAlgorithmException, NoSuchProviderException {
        File f = new File(filePathString);
        byte[] salt = null;
        //读文件，icount是迭代次数
        if (f.exists() && !f.isDirectory()) {

            salt = CryptoPrimitives.readAlternateImpl(filePathString);
        } else {
            salt = CryptoPrimitives.randomBytes(8);
            CryptoPrimitives.write(salt, "saltInvIX", "salt");

        }

        byte[] key = CryptoPrimitives.keyGenSetM(password, salt, icount, keySize);
        return key;

    }

    public static CassandraLsm constructlsm(DocumentRepository dr, final byte[] key, final Multimap<String, String> lookup, final int blockSize,
                                             final int arrayBlock, final int dataSize) throws InterruptedException, ExecutionException, IOException, NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException {
        final Map<String, Integer> dictionary1 = new HashMap<>();
        final Map<byte[], Integer> dictionary2 = new HashMap<>();

        random.setSeed(CryptoPrimitives.randomSeed(16));

        List<String> listOfKeyword = new ArrayList<String>(lookup.keySet());
        int threads = 0;
        if (Runtime.getRuntime().availableProcessors() > listOfKeyword.size()) {
            threads = listOfKeyword.size();
        } else {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ExecutorService service = Executors.newFixedThreadPool(threads);
        ArrayList<String[]> inputs = new ArrayList<String[]>(threads);

        //分线程处理 分给线程要处理的keyword？
        for (int i = 0; i < threads; i++) {
            String[] tmp;
            if (i == threads - 1) {
                tmp = new String[listOfKeyword.size() / threads + listOfKeyword.size() % threads];
                for (int j = 0; j < listOfKeyword.size() / threads + listOfKeyword.size() % threads; j++) {
                    tmp[j] = listOfKeyword.get((listOfKeyword.size() / threads) * i + j);
                }
            } else {
                tmp = new String[listOfKeyword.size() / threads];
                for (int j = 0; j < listOfKeyword.size() / threads; j++) {

                    tmp[j] = listOfKeyword.get((listOfKeyword.size() / threads) * i + j);
                }
            }
            inputs.add(i, tmp);
        }

//        Printer.debugln("End of Partitionning  \n");

        //这里的加入多线程的方法
        for (final String[] input : inputs) {
            Pair<Map<String, Integer>, Map<byte[], Integer>> output = setup(dr, key, input, lookup, blockSize, arrayBlock, dataSize);

            Map<String, Integer> keys1 = output.getKey();
            Map<byte[], Integer> keys2 = output.getValue();
            Set<String> ss = keys1.keySet();
            for (String k : ss) {
                //if (!dictionary1.containsKey(k) || dictionary1.get(k) < keys1.get(k)) {
                    dictionary1.put(k, keys1.get(k));
               // }
            }
            Set<byte[]> bb = keys2.keySet();
            for (byte[] k : bb) {
                //if (!dictionary2.containsKey(k) || dictionary2.get(k) < keys2.get(k)) {
                    dictionary2.put(k, keys2.get(k));
                    //System.out.print("kkk2 "+dictionary2.get(k));
               // }
            }
        }

        return new CassandraLsm(dictionary1, dictionary2);
    }


//啊！ 忘记什么多线程吧 太上头了
    //multimap用在多线程总是会有ConcurrentModificationException报错！！！！
//        List<Future<Pair<Multimap<String, Integer>, Multimap<byte[], Integer>>>> futures = new ArrayList<Future<Pair<Multimap<String, Integer>, Multimap<byte[], Integer>>>>();
//
//        for (final String[] input : inputs) {
//            Callable<Pair<Multimap<String, Integer>, Multimap<byte[], Integer>>> callable = new Callable<Pair<Multimap<String, Integer>, Multimap<byte[], Integer>>>() {
//                //只要有空闲线程，call就会被调用
//                public Pair<Multimap<String, Integer>, Multimap<byte[], Integer>> call() throws Exception {
//
//                    Pair<Multimap<String, Integer>, Multimap<byte[], Integer>> output = setup(dr, key, input, lookup, blockSize, arrayBlock, dataSize);
//                    return output;
//                }
//            };
//            futures.add(service.submit(callable));
//        }
//
//        service.shutdown();
//
//        for (Future<Pair<Multimap<String, Integer>, Multimap<byte[], Integer>>> future : futures) {
//            Multimap<String, Integer> keys1 = future.get().getKey();
//            Multimap<byte[], Integer> keys2 = future.get().getValue();
////            Set<String> keys1 = future.get().getKey().keySet();
////            Set<byte[]> keys2 = future.get().getValue().keySet();
//            for(String k: keys1.keySet()){
//                //if(!dictionary1.containsKey(k) || dictionary1.get(k)<keys1.get(k)){
//                    dictionary1.putAll(k, keys1.get(k));
//                //}
//            }
//            for(byte[] k: keys2.keySet()){
//                //if(!dictionary2.containsKey(k) || dictionary2.get(k)<keys2.get(k)){
//                    dictionary2.putAll(k, keys2.get(k));
//                    System.out.print("kkk2 "+dictionary2.get(k));
//               // }
//            }
//        }



    // ***********************************************************************************************//

    ///////////////////// Setup /////////////////////////////

    // ***********************************************************************************************//

    public static Pair<Map<String, Integer>, Map<byte[], Integer>> setup(DocumentRepository dr, byte[] key, String[] listOfKeyword, Multimap<String, String> lookup,
                                                 int blockSize, int arrayBlock, int dataSize) throws InvalidKeyException, InvalidAlgorithmParameterException,
            NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
        long startTime = System.nanoTime();

        byte[] iv = new byte[16];

        for (String word : listOfKeyword) {
            // generate the tag
            byte[] key1 = CryptoPrimitives.generateCmac(key, 1 + word);
            byte[] key2 = CryptoPrimitives.generateCmac(key, 2 + word);

            int T = lookup.get(word).size();
            //counter用于记录关键字对应的计数器值
            int counter = 1;
            byte[] value;
            byte[] l;
            while (T != 0) {
//                if (T < blockSize) {
//                    if(dictionary1.get(word).size()==0){
//                        dictionary1.put(word,1);
//                        // System.out.println("++++"+word+"第一种情况");
//                        l = CryptoPrimitives.generateCmac(key1, Integer.toString(counter));
//                        List<String> tmpList = new ArrayList<String>(lookup.get(word));
//                        int sizeList = tmpList.size();
//                        //填充
//                        for (int s = 0; s < (blockSize - sizeList); s++) {
//                            tmpList.add("XX");
//                        }
//
//                        random.nextBytes(iv);
//                        value = CryptoPrimitives.encryptAES_CTR_String(key2, iv,
//                                tmpList.toString(),  blockSize * sizeOfFileIdentifer);
//                        ByteBuffer buf1 = ByteBuffer.wrap(l);
//                        ByteBuffer buf2 = ByteBuffer.wrap(value);
//                        String str1 = Bytes.toHexString(buf1);
//                        String str2 = Bytes.toHexString(buf2);
//                        //enc_arrayBlock 需要考虑下怎么存 方便直接取出用
//                        Document document = new Document(str1,1,str2);
//                        //需要将document插入cassandra的操作
//                        System.out.println("第一种情况"+word+"***"+document.getEnc_id());
//                        dr.insertdocument(document);
//                        dictionary2.put(l,1);
//                    }
//                    else {
//                        List<String> listArrayIndex1 = new ArrayList<String>();
//                        l = CryptoPrimitives.generateCmac(key1, Integer.toString(counter));
//                        List<String> tmpList = new ArrayList<String>(lookup.get(word));
//                        //主要不同就在这句
//                        tmpList.subList(arrayBlock*blockSize*(counter-1),tmpList.size());
//                        int sizeList = tmpList.size();
//                        for (int s = 0; s < (blockSize - sizeList); s++) {
//                            tmpList.add("XX");
//                        }
//                        //tmpList不应该在这用上 应该加上之前整个的吗 ？也不是 之前的自有之前的l作为索引
//                        value = CryptoPrimitives.encryptAES_CTR_String(key2, iv,
//                                tmpList.toString(),  blockSize * sizeOfFileIdentifer);
//
//                        dictionary1.put(word,counter);
//                        random.nextBytes(iv);
//
//                        byte[] v = CryptoPrimitives.encryptAES_CTR_String(key2, iv,
//                                "0"+listArrayIndex1.toString(), arrayBlock*sizeOfFileIdentifer);
//
//                    }
//
//                    break;
////貌似第一种情况可以放入第二种情况中来处理
                //} else if (T < arrayBlock * blockSize) {
                if (T < arrayBlock * blockSize) {
                    if (!dictionary1.containsKey(word)) {
                        dictionary1.put(word, 1);
                        l = CryptoPrimitives.generateCmac(key1, Integer.toString(1));
                    } else {
                        dictionary1.put(word, counter);
                        l = CryptoPrimitives.generateCmac(key1, Integer.toString(counter));
                    }

                    ByteBuffer buf1 = ByteBuffer.wrap(l);
                    String str1 = Bytes.toHexString(buf1);
                    //block为块的大小 t为可以分的块数
                    //System.out.println("++++"+word+"第二种情况");
                    int t = (int) Math.ceil((float) T / blockSize);
                    //最后一个block块需要填充
                    //同一个word时，counter会被覆盖吗

                    List<String> tmpList = new ArrayList<String>(lookup.get(word));
                    List<String> tmpList1 = new ArrayList<String>();
                    int f = tmpList.size();
                    //第三种情况下的处理
                    if (dictionary1.containsKey(word)) {
                        tmpList = tmpList.subList(arrayBlock * blockSize * (counter - 1), f);
//                      System.out.println(word+"**"+tmpList.size());
                    }

                    for (int i = 0; i < t; i++) {
                        if (i != t - 1) {
                            tmpList1 = tmpList.subList(i * blockSize, (i + 1) * blockSize);
                        } else {
                            //需要填充
                            int sizeList = tmpList.size();
                            tmpList1 = tmpList.subList(i * blockSize, tmpList.size());
                            //如果需要进行填充
                            for (int s = 0; s < ((i + 1) * blockSize - sizeList); s++) {
                                tmpList1.add("XX");
                            }
                        }

                        random.nextBytes(iv);
                        value = CryptoPrimitives.encryptAES_CTR_String(key2, iv,
                                tmpList1.toString(), blockSize * sizeOfFileIdentifer);


                        ByteBuffer buf2 = ByteBuffer.wrap(value);
                        String str2 = Bytes.toHexString(buf2);
                        //enc_arrayBlock 需要考虑下怎么存 方便直接取出用
//                        System.out.println("第二种情况也可能是第三种" + word);
                        Document document = new Document(str1, i, str2);
                        //需要将document插入cassandra的操作
                        dr.insertdocument(document);
                    }
                    dictionary2.put(l, t);//t为下一个可以插入的位置
                   // System.out.println(l + "  "+ t);
                    break;
                } else {
                    dictionary1.put(word, counter);
                    //完整的填充 c+1后继续循环

                    //dictionary1.put(new String(l), arrayBlock);
                    l = CryptoPrimitives.generateCmac(key1, Integer.toString(counter));
                    ByteBuffer buf1 = ByteBuffer.wrap(l);
                    String str1 = Bytes.toHexString(buf1);

                    List<String> tmpList = new ArrayList<String>(lookup.get(word));
                    List<String> tmpList1 = new ArrayList<String>();
                    //第三种情况下的非第一次处理
                    if (dictionary1.containsKey(word)) {
                        tmpList = tmpList.subList(arrayBlock * blockSize * (counter - 1), tmpList.size());
                    }
                    //System.out.println(tmpList.size() == T);
                    for (int i = 0; i < arrayBlock; i++) {
                        System.out.println(tmpList.size());
                        tmpList1 = tmpList.subList(i * blockSize, (i + 1) * blockSize);
                        //System.out.println(blockSize+"***"+tmpList.size());
                        value = CryptoPrimitives.encryptAES_CTR_String(key2, iv,
                                tmpList1.toString(), arrayBlock * blockSize * sizeOfFileIdentifer);
                        ByteBuffer buf2 = ByteBuffer.wrap(value);
                        String str2 = Bytes.toHexString(buf2);
//                        System.out.println("第三种情况" + word);
                        Document document = new Document(str1, i, str2);
                        dr.insertdocument(document);
                    }
                    dictionary2.put(l, arrayBlock);
                    System.out.println(l + "  "+ arrayBlock);
                    T = T - arrayBlock * blockSize;
                    counter = counter + 1;
                }

            }
            //System.out.println("yanzheng"+dictionary1.get(word));
        }
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;
        //Printer.debugln("Time for one (w, id) "+totalTime/lookup.size());
        Pair<Map<String, Integer>, Map<byte[], Integer>> pair = new Pair<>(dictionary1, dictionary2);
        return pair;
    }
    ///////////////////// Search Token generation /////////////////////
    ///////////////////// /////////////////////////////

    // ***********************************************************************************************//
    public static byte[][] token(byte[] key, String word) throws UnsupportedEncodingException {

        byte[][] keys = new byte[2][];
        keys[0] = CryptoPrimitives.generateCmac(key, 1 + word);
        keys[1] = CryptoPrimitives.generateCmac(key, 2 + word);

        return keys;
    }
    // ***********************************************************************************************//

    ///////////////////// Query Alg /////////////////////////////

    // ***********************************************************************************************//
    public static List<String> query(DocumentRepository dr, byte[][] keys, int count)
            throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
            NoSuchProviderException, NoSuchPaddingException, IOException {
        //counter 需要传入
        byte[] l = CryptoPrimitives.generateCmac(keys[0], Integer.toString(count));
        //System.out.print(new String(l));
        ByteBuffer buf = ByteBuffer.wrap(l);
        String str = Bytes.toHexString(buf);
        List<String> tempListStr = dr.selectByWord(str);
        //System.out.println(tempListStr.size());
        List<byte[]> tempList = new ArrayList<>();
        for(String s : tempListStr){
            ByteBuffer buffer = Bytes.fromHexString(s);
            tempList.add(Bytes.getArray(buffer));
        }
        //System.out.println(tempList.size());
        List<String> resultAll = new ArrayList<String>();
        if (!(tempList.size() == 0)) {
            for(byte[] t :tempList) {
                //System.out.println(tempList.get(0));
                String temp = (new String(CryptoPrimitives.decryptAES_CTR_String(t, keys[1])))
                        .split("\t\t\t")[0];
                //System.out.println("***" + temp);
                temp = temp.replaceAll("\\s", "");
                temp = temp.replaceAll(",XX", "");
                temp = temp.replace('[', ',');
                temp = temp.replace("]", "");

                String[] result = temp.split(",");
                //System.out.println(result);
                List<String> resultFinal = new ArrayList<String>(Arrays.asList(result));
                resultFinal.remove(0);
                //System.out.println("*****" + resultFinal);

                //System.out.println(resultFinal.size());
                resultAll.addAll(resultFinal);
            }
            return resultAll;
        }
//        if (!(tempList.size() == 0)) {
//            //System.out.println(tempList.get(0));
//            String temp = (new String(CryptoPrimitives.decryptAES_CTR_String(tempList.get(0), keys[1])))
//                    .split("\t\t\t")[0];
//            //System.out.println("***" + temp);
//            temp = temp.replaceAll("\\s", "");
//            temp = temp.replaceAll(",XX", "");
//            temp = temp.replace('[', ',');
//            temp = temp.replace("]", "");
//
//            String[] result = temp.split(",");
//            //System.out.println(result);
//            List<String> resultFinal = new ArrayList<String>(Arrays.asList(result));
//            resultFinal.remove(0);
//            //System.out.println("*****"+resultFinal);
//
//            //System.out.println(resultFinal.size());
//            return resultFinal;
//        }

        //为空的情况下的返回
        return new ArrayList<String>();
    }


}

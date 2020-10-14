package org.crypto.sse;

import com.datastax.driver.core.Session;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by mjq on 2020/8/17.
 */
public class TestLocalCassandraLsm {
    public void boolSearch(){

    }
    public static void main(String[] args) throws Exception {
        //连接Cassandra数据库
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        Session session = client.getSession();

        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        String keyspaceName = "library";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(keyspaceName);

        DocumentRepository dr = new DocumentRepository(session);
        dr.deleteTable("document");
        dr.createTable();


        Printer.addPrinter(new Printer(Printer.LEVEL.EXTRA));

        BufferedReader keyRead = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Enter your password :");

        String pass = keyRead.readLine();
        //list中总共有三个元素，two keys for Secure inverted index\one key for encryption
        List<byte[]> listSK = IEXCassandra.keyGen(256, pass, "salt/salt", 100000);

        System.out.println("Enter the relative path name of the folder that contains the files to make searchable");

        String pathName = keyRead.readLine();

        //all files from a directory（pathName）
//        ArrayList<File> listOfFile = new ArrayList<File>();

        //TextProc.listf(pathName, listOfFile);同下面的方法重复？
        //创建lookup表
        TextProc.TextProc(false, pathName);

        // The two parameters depend on the size of the dataset. Change
        // accordingly to have better search performance
        int Block = 2;
        int arrayBlock = 5;
        int dataSize = 1000000;

        // Construction of the global multi-map
        System.out.println("\nBeginning of Encrypted Multi-map creation \n");
        System.out.println("Number of keywords " + TextExtractPar.lp1.keySet().size());
        System.out.println("Number of pairs " + TextExtractPar.lp1.keys().size());

        //start
        long startTime = System.nanoTime();

        Multimap<String, String> myMultimap = ArrayListMultimap.create();

        CassandraLsm clsm = CassandraLsm.constructlsm(dr, listSK.get(0), TextExtractPar.lp1, Block, arrayBlock, dataSize);

        //constructlsm
        long endTime = System.nanoTime();

        //time elapsed
        long output = endTime - startTime;
        System.out.println("Elapsed time in seconds: " + output / 1000000000);

        //      单关键字查询

        while (true) {
            System.out.println("Enter the keyword to search for:");
            String keyword = keyRead.readLine();

            byte[][] token = CassandraLsm.token(listSK.get(0), keyword);
            if (!CassandraLsm.getdictionary1().containsKey(keyword)) {
                System.out.println("not exit!");
                continue;
            }

            int t = CassandraLsm.getdictionary1().get(keyword);
//            List<Integer> l = new ArrayList<>(CassandraLsm.getdictionary1().get(keyword));
            for (int i = 1; i <= t; i++) {
                System.out.println("Final Result: " + CassandraLsm.query(dr, token, i));
            }
            //暂时先这样，后边可以改为用户可控制
            break;
        }


        Multimap<String, Integer> dictionaryForMM = null;
        CassandraLsm[] localMultiMap = null;
//        有三个数据结构需要进行setup
        IEXCassandra disj = IEXCassandra.setup(dr, clsm, listSK, TextExtractPar.lp1, TextExtractPar.lp2, Block, arrayBlock, dataSize);
//        这里需要先确定第一个关键字的count

//
//        //disconjunctive查询+布尔查询的实现

        while (true) {
            System.out.println("How many disjunctions? ");
            int numDisjunctions = Integer.parseInt(keyRead.readLine());

//            // Storing the CNF form
            String[][] bool = new String[numDisjunctions][];
            for (int i = 0; i < numDisjunctions; i++) {
                System.out.println("Enter the keywords of the disjunctions ");
                bool[i] = keyRead.readLine().split(" ");
            }

            Set<String> tmp = new HashSet<String>();
            tmp.addAll(IEXCassandra.boolSearch(bool,dr,listSK,disj));
            System.out.println("The Result of Bool Search: "+tmp);

            //暂时先这样，后边可以改为用户可控制
            //break;
        }
    }
}

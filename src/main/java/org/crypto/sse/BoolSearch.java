package org.crypto.sse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by mjq on 2020/10/8.
 */
public class BoolSearch {
    //貌似不用单独开一个类的，毕竟我只是封装一个功能函数

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

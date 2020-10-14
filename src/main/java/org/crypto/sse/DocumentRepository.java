package org.crypto.sse;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mjq on 2020/8/14.
 */
public class DocumentRepository {
    private static final String TABLE_NAME = "document";

    private Session session;

    public DocumentRepository(Session session){
        this.session = session;
    }

    //创建列族
    /**
     * Creates the books table.
     */
    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME)
                .append("(")
                .append("enc_word  text, ")
                .append("enc_arrayBlock int, ")
                .append("enc_id text,")
                .append("PRIMARY KEY (enc_word, enc_arrayBlock));");
                //分区键和集群键


        final String query = sb.toString();
        session.execute(query);
        System.out.println("Created successfully!");
    }
    /**
     * Insert a row in the table books.
     *
     * @param document
     */
    public void insertdocument(Document document) {
        //这里的引号问题特别讨厌 还有 字节数组没法识别是什么鬼
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME)
                .append("(Enc_word,enc_arrayBlock,enc_id) ")
                .append("VALUES ('")
                .append(document.getEnc_word())
                .append("', ")
                .append(document.getEnc_arrayBlock())
                .append(", '")
                .append(document.getEnc_id()).append("');");
        final String insert = sb.toString();
//        System.out.println(insert);
//        char[] t = query.toCharArray();
//        for(int i =0;i<query.length();i++){
//            if (t[i] == '@')
//                System.out.println(i);
//        }
//        System.out.println("insert successfully!");
        session.execute(insert);
    }

    /**
     * Select all books from books
     *
     * @return
     */
    //这里关于数据类型有些问题不明白
    public List<Document> selectAll() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Document> documents = new ArrayList<Document>();
        //后续会改 不应该这样返回
        for (Row r : rs) {
            Document document =
                    new Document(r.getString("enc_word"),r.getInt("enc_arrayBlock"), r.getString("enc_id"));
            documents.add(document);
        }
        System.out.println("查询成功！");
        return documents;
    }
    /**
     * Select document by enc_word.
     *
     * @return
     */
    public List<String> selectByWord(String enc_word) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME).append(" WHERE enc_word = '").append(enc_word).append("';");

        final String query = sb.toString();

        ResultSet rs = session.execute(query);

        //List<Document> documents = new ArrayList<Document>();
        List<String> result= new ArrayList<String>();
        for (Row r : rs) {
            Document s = new Document(r.getString("enc_word" ), r.getInt("enc_arrayBlock"), r.getString("enc_id"));
            //documents.add(s);
            result.add(s.getEnc_id());
        }

        return result;
    }

    /**
     * Delete table.
     *
     * @param tableName the name of the table to delete.
     */
    public void deleteTable(String tableName) {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();
        session.execute(query);
        System.out.println("deleted successfully!");
    }
}

package org.crypto.sse;

/**
 * Created by mjq on 2020/8/14.
 */
public class Document {
    //Enc(id) Enc(word) 字节数组
//    private ByteBuffer enc_id;
//    private ByteBuffer enc_word;
//    Document(){
//    }
//    public Document(ByteBuffer enc_id, ByteBuffer enc_word){
//        this.enc_id = enc_id;
//        this.enc_word = enc_word;
//    }
//
//    public ByteBuffer getEnc_id(){
//        return enc_id;
//    }
//
//    public void setEnc_id(ByteBuffer enc_id){
//        this.enc_id = enc_id;
//    }
//
//    public ByteBuffer getEnc_word(){
//        return enc_word;
//    }
//
//    public void setEnc_word(ByteBuffer enc_id) {
//        this.enc_id = enc_word;
//    }

//    private byte[] enc_id;
//    private byte[] enc_word;
//    Document(){
//    }
//    public Document(byte[] enc_id, byte[] enc_word){
//        this.enc_id = enc_id;
//        this.enc_word = enc_word;
//    }
//
//    public byte[] getEnc_id(){
//        return enc_id;
//    }
//
//    public void setEnc_id(byte[] enc_id){
//        this.enc_id = enc_id;
//    }
//
//    public byte[] getEnc_word(){
//        return enc_word;
//    }
//
//    public void setEnc_word(ByteBuffer enc_id) {
//        this.enc_id = enc_word;
//    }


    private String enc_id;
    private int enc_arrayBlock;
    private String enc_word;
    Document(){
    }
    public Document(String enc_word, int enc_arrayBlock, String enc_id){
        this.enc_id = enc_id;
        this.enc_arrayBlock = enc_arrayBlock;
        this.enc_word = enc_word;
    }

    public String getEnc_id(){
        return enc_id;
    }

    public void setEnc_id(String enc_id){
        this.enc_id = enc_id;
    }

    public int getEnc_arrayBlock(){
        return enc_arrayBlock;
    }

    public String getEnc_word(){
        return enc_word;
    }

    public void setEnc_word(String enc_id){
        this.enc_id = enc_word;
    }


//    private  long enc_id;
//    private  long enc_word;
//    Document(){
//    }
//    public Document(long enc_word, long enc_id){
//        this.enc_id = enc_id;
//        this.enc_word = enc_word;
//    }
//
//    public long getEnc_id(){
//        return enc_id;
//    }
//
//    public void setEnc_id(long enc_id){
//        this.enc_id = enc_id;
//    }
//
//    public long getEnc_word(){
//        return enc_word;
//    }

}

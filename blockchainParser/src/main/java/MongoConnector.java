import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Sergio Serusi & Stefano Lande on 09/04/2017.
 */

public class MongoConnector {
    private MongoClient mongoClient;
    private MongoCollection<Document> transactionCollection;
    private boolean connected = false;

    /*
    * This method connect this application with Mongo
     */
    public void connect() {
        if (!connected) {
            MongoCredential credential = MongoCredential.createCredential(Settings.MONGO_USER, Settings.MONGO_DB_NAME, Settings.MONGO_PWD);
            //mongoClient = new MongoClient(new ServerAddress(Settings.MONGO_SERVER_IP, Settings.MONGO_SERVER_PORT), Arrays.asList(credential));
            mongoClient = new MongoClient(Settings.MONGO_SERVER_IP, Settings.MONGO_SERVER_PORT);

            MongoDatabase db = mongoClient.getDatabase(Settings.MONGO_DB_NAME);
            transactionCollection = db.getCollection(Settings.MONGO_COLLECTION_NAME);
            connected = true;
        }
    }

    /*
    * This method disconnect this applicazion with Mongo
     */
    public void disconnect() {
        if (connected) {
            mongoClient.close();
            connected = false;
            System.out.println("Disconnect to Mongo");
        }
    }

    /*
    * This method add the transaction "T" to the mongo DB
     */
    public void addTransaction(Transaction t, Block block, int height) {
        if (!connected) {
            throw new RuntimeException("Mongo not connected");
        }

        Document txDoc = new Document("txid", t.getHashAsString());
        //txDoc.append("locktime", t.getLockTime());

        List<Document> vin = new ArrayList<>();

        try {

            for (TransactionInput tIn : t.getInputs()) {
                Document in = new Document();


                if (!tIn.isCoinBase()) {
                    String txid = tIn.getOutpoint().getHash().toString();
                    //in.append("vout", tIn.getOutpoint().getIndex());

                    //in.append("txid", txid);
                    //in.append("scriptSig", tIn.getScriptSig().toString());

                    //try to extract the address from the scriptSig - valid only for pay-to-pubkey-hash
                    try {
                        List<ScriptChunk> chuncks = tIn.getScriptSig().getChunks();

                        byte[] keyBytes = chuncks.get(1).data;
                        ECKey key = ECKey.fromPublicOnly(keyBytes);
                        in.append("address", key.toAddress(MainNetParams.get()).toString());

                    } catch (Exception e) {
                        in.append("address", "");
                    }

                } else {
                    //in.append("coinbase", true);
                }
                //in.append("sequence", tIn.getSequenceNumber());


                vin.add(in);
            }

            txDoc.append("vin", vin);

            List<Document> vout = new ArrayList<>();

            for (TransactionOutput tOut : t.getOutputs()) {

                Document out = new Document();
                //out.append("value", tOut.getValue().getValue());
                //out.append("n", tOut.getIndex());
                //out.append("scriptPubKey", tOut.getScriptPubKey().toString());
                //out.append("type", tOut.getScriptPubKey().getScriptType().toString());

                List<String> keys = new ArrayList<>();

                //try to parse the destination address from the scriptPubKey
                if (tOut.getScriptPubKey().isSentToMultiSig()) {
                    for (ECKey key : tOut.getScriptPubKey().getPubKeys()) {
                        keys.add(key.getPrivateKeyAsWiF(MainNetParams.get()));
                    }
                } else if (tOut.getScriptPubKey().getScriptType() == Script.ScriptType.P2PKH) {
                    keys.add(tOut.getAddressFromP2PKHScript(MainNetParams.get()).toString());
                } else if (tOut.getScriptPubKey().getScriptType() == Script.ScriptType.PUB_KEY) {
                    ECKey key = ECKey.fromPublicOnly(tOut.getScriptPubKey().getPubKey());
                    keys.add(key.toAddress(MainNetParams.get()).toString());
                }

                if (tOut.getScriptPubKey().isOpReturn()){
                    //out.append("isOpReturn", true);
                    try{
                        //out.append("opReturnData", tOut.getScriptPubKey().getChunks().get(0).data);
                    } catch (Exception e){}
                }

                out.append("addresses", keys);
                vout.add(out);

            }
            txDoc.append("vout", vout);


            //txDoc.append("blockhash", block.getHashAsString());
            //txDoc.append("blockheight", height);
            txDoc.append("time", block.getTimeSeconds());


        } catch (Exception e) {
        }

        transactionCollection.insertOne(txDoc);

    }

    /*
     * This method find the last block stored in the database and returns its Locktime
     * @return The time (a long value) of the last block stored in the DB
     */
    protected long getLastBlockTime() {
        //Take the last transaction from the DB
        Document cursor = transactionCollection.find().sort(new BasicDBObject("time", -1)).first();
        if(cursor==null)return 0;
        return ((long) cursor.get("time"));
    }

}
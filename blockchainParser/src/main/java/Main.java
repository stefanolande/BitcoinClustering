/**
 * Created by Sergio Serusi & Stefano Lande on 09/04/2017.
 */

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.File;
import java.util.*;

public class Main {
    private static MongoConnector mongo;

    public static void main(String[] args) {
        mongo = new MongoConnector();

        //Connection to the DB mongo
        mongo.connect();
        long startTime = System.currentTimeMillis();

        //Call method to update the DB
        updateDB(mongo);

        //Disconnect to the DB mongo
        mongo.disconnect();
        System.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) / 1000 + " secondi");
    }

    /*
     * This method insert the new transaction that are in the blockchain in the mongo DB
     *
    */
    private static void updateDB(MongoConnector mongo){
        //Initalize bitcoinJ

        BriefLogFormatter.init();
        NetworkParameters networkParameters = new MainNetParams();
        Context.getOrCreate(MainNetParams.get());

        //read the blockchain files from the disk
        List<File> blockChainFiles = new LinkedList<File>();
        for (int i = 0; true; i++) {
            File file = new File(Settings.BLOCKCHAIN_PATH + String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists()) {
                System.out.println("file not exist");
                break;
            }
            blockChainFiles.add(file);
        }

        long lastBlockTime = mongo.getLastBlockTime();//Ultima Transazione presente nel DB

        BlockFileLoader bfl = new BlockFileLoader(networkParameters, blockChainFiles);

        // Iterate over the blocks in the blockchain.
        int height = 1;
        for (Block block : bfl) {
            height++;

            if(height % 1000 == 0){
                System.out.println("Current block: " + height);
            }

            //Se il tempo del blocco corrente è maggiore del tempo dell'utimo blocco presente nel DB, allora iniziamo ad aggiornare il database
            if (block.getTimeSeconds() > lastBlockTime) {
                //System.out.println("Add Transactions to mongo!");
                for (Transaction t : block.getTransactions()) {
                    mongo.addTransaction(t, block, height);
                }
            }
        }
    }

}

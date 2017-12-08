/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.tx;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nelser05
 */

/*
Get the new transaction number.
While the CheckpointThread is in progress, keep trying to acquire the transaction lock.
Create an instance of the recovery manager.
Create an instance of the concurrency manager.
Add the new transaction to the list of all current transactions (don't forget to remove it from the list on commit or rollback).
If this is a 10th transaction, start the following process:
Set inProgress flag to true
Start new new CheckpointThread ( (new Thread(new CheckpointThread())).start() )
Done.
*/
public class CheckpointThread implements Runnable {
  private static final long MAXTIME = 10000;
  private static Object checkpointLock = new Object();
  private boolean inProgress = false;
  @Override
  public void run() {
    while (Transaction.getCurrentTransactionsList().size() != 0) {
      synchronized (Transaction.getLock()){
        try {
        checkpointLock.wait();
        // Not sure why I get an error here either, but it's part of the algorithm
        //BufferMgr.flushAll();

        
      } catch (InterruptedException ex) {
          Logger.getLogger(CheckpointThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
  }
}
}

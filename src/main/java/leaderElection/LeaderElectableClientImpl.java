package leaderElection;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;

/**
 * Implementation of Zookepper Client that simply outputs its state after any leader election.
 * 
 * @author Vivek
 */
public class LeaderElectableClientImpl extends LeaderElectableClient{

	public LeaderElectableClientImpl() throws KeeperException, IOException, InterruptedException {
		super();
	}

	@Override
	void performLeaderWork() {

		if(getCachedIsLeader()){
			System.out.println("I'm the Zoo leader, thus performing the leader work...");
		}
		else{
			System.out.println("Currently, Not a Zoo leader...");
		}

	}

	public static void main(String args[]) throws KeeperException, IOException, InterruptedException {
		LeaderElectableClient client = new LeaderElectableClientImpl();
		client.run();
	}

}
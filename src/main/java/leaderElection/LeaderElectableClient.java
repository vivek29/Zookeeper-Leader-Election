package leaderElection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * An abstract ZooKeeper client that implements the leader election algorithm.
 *
 * @author Vivek
 *
 */
public abstract class LeaderElectableClient implements Watcher, ZooZNodeDeletionMonitorListener {

	private static final String DEFAULT_ELECTION_ZNODE_PATH = "/election";
	private static final String DEFAULT_SERVER_HOSTS = "localhost:2181";
	// private static final String DEFAULT_SERVER_HOSTS = "localhost:2181,localhost:2182,localhost:2183";

	private static final int DEFAULT_TIME_OUT_MS = 3000;

	private ZooKeeper ZooKeeper = null;
	private boolean isLeader = false;

	// Monitors existence of a parameter zNode
	ZooZNodeDeletionMonitor zNodeDeletionMonitor = null;

	// The path to the election GUID znode
	private String electionGUIDZNodePath = null;

	// @return String containing path to persistent election znode
	private static final String getElectionZNodePath() { 
		return DEFAULT_ELECTION_ZNODE_PATH; 
	}

	// @return String containing "host:port" pairs
	private static final String getHosts() { 
		return DEFAULT_SERVER_HOSTS; 
	}

	// @return integer containing time out interval in milliseconds
	private static int getTimeOutMs() { 
		return DEFAULT_TIME_OUT_MS; 
	}

	// @return handle to ZooKeeper API
	private ZooKeeper getZooKeeper() {		
		assert null != ZooKeeper;
		return ZooKeeper;
	}

	// @return true if this is the current leader, false otherwise
	protected boolean getCachedIsLeader() { 
		return isLeader; 
	}

	// @return handle to the deletion monitor
	private ZooZNodeDeletionMonitor getZNodeDeletionMonitor() {
		assert null != zNodeDeletionMonitor;
		return zNodeDeletionMonitor;
	}

	// @return the path of this client's election GUID
	protected final String getElectionGUIDZNodePath() {
		assert null != electionGUIDZNodePath;
		return electionGUIDZNodePath;
	}

	// Utility function to convert a GUID to a full path
	private String formatElectionGUIDZNodePath( String zNodeGUID ) {
		return getElectionZNodePath() + "/" + zNodeGUID;
	}


	// @return the path of the leader's election GUID
	private String getLeaderElectionGUIDZNodePath( List<String> optionalGUIDs ) throws KeeperException, InterruptedException {
		List<String> guids = ((optionalGUIDs == null) || optionalGUIDs.isEmpty()) ?
				getZooKeeper().getChildren( getElectionZNodePath(), false) : optionalGUIDs;
				if ( !guids.isEmpty() ) {
					String leaderGUID = formatElectionGUIDZNodePath( Collections.min( guids ) );
					//	System.out.println( "LeaderElectableClient::getLeaderElectionGUIDZNodePath:: " + leaderGUID );
					return leaderGUID;
				}
				else {
					// System.out.println( "LeaderElectableClient::getLeaderElectionGUIDZNodePath:: no GUIDS exist!" );
					return null;
				}
	}


	// @return largest guid znode that is less than our guid (unless we are the leader, then return leader znode path)
	private String getZNodePathToWatch( List<String> optionalGUIDs ) throws KeeperException, InterruptedException {

		// Early out if we are the leader
		if ( getCachedIsLeader() ) {
			//	System.out.println( "LeaderElectableClient::getZNodePathToWatch:: (" + getElectionGUIDZNodePath() + ") -> " + getElectionGUIDZNodePath() );
			return getElectionGUIDZNodePath();
		}

		List<String> guids = ((optionalGUIDs == null) || optionalGUIDs.isEmpty()) ?
				getZooKeeper().getChildren( getElectionZNodePath(), false) : optionalGUIDs;

				if ( !guids.isEmpty() ) {
					// Initialize to first path less than our znode
					String zNodePathToWatch = null;
					int itrGUID = 0;
					for ( ; itrGUID < guids.size(); ++itrGUID ) {
						String guid = formatElectionGUIDZNodePath( guids.get( itrGUID ) );
						if ( guid.compareTo( getElectionGUIDZNodePath() ) < 0 ) {
							zNodePathToWatch = guid;
							break;
						}
					}

					// There should be at least one znode less than us
					assert null != zNodePathToWatch;

					// Find largest znode that's less than our znode
					for ( ; itrGUID < guids.size(); ++itrGUID ) {
						String guid = formatElectionGUIDZNodePath( guids.get( itrGUID ) );
						if ( 
								( guid.compareTo( zNodePathToWatch ) > 0 )
								&& ( guid.compareTo( getElectionGUIDZNodePath() ) < 0 )
								) {
							zNodePathToWatch = guid;
						}
					}
					//	System.out.println( "LeaderElectableClient::getZNodePathToWatch:: (" + getElectionGUIDZNodePath() + ") -> " + zNodePathToWatch );
					return zNodePathToWatch;
				}
				else {
					// System.out.println( "LeaderElectableClient::getZNodePathToWatch:: no GUIDS exist!");
					return null;
				}
	}

	// Constructor
	protected LeaderElectableClient() throws KeeperException, IOException, InterruptedException {

		// Initialize the ZooKeeper api
		ZooKeeper = new ZooKeeper( getHosts(), getTimeOutMs(), this );
		// Attempt to create the election znode parent
		conditionalCreateElectionZNode();
		// Create the election GUID
		createElectionGUIDZNode();
	}

	// Attempts to create an election znode if it doesn't already exist
	private void conditionalCreateElectionZNode() throws KeeperException, InterruptedException {
		if ( null == getZooKeeper().exists( getElectionZNodePath(), false) ) {
			try {
				final String path = getZooKeeper().create( getElectionZNodePath(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
				System.out.println( "LeaderElectableClient::conditionalCreateElectionZNode:: created with path:" + path );
			}
			catch( KeeperException.NodeExistsException ne ) {
				System.out.println( "LeaderElectableClient::conditionalCreateElectionZNode:: failed (NodeExistsException)" );
			}
		}
		else {
			System.out.println( "LeaderElectableClient::conditionalCreateElectionZNode:: already created." );
		}
	}

	// Creates a sequential znode
	private void createElectionGUIDZNode() throws KeeperException, InterruptedException {
		// Create an ephemeral_sequential file
		electionGUIDZNodePath = getZooKeeper().create( getElectionZNodePath() + "/guid-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
		System.out.println( "LeaderElectableClient::createElectionGUIDZNode:: created with path:" +  electionGUIDZNodePath );
	}

	// Elects a leader and caches the results for this client
	private void electAndCacheLeader() throws KeeperException, InterruptedException {
		isLeader = getElectionGUIDZNodePath().equals( getLeaderElectionGUIDZNodePath( null ) );
	}

	// Sets a deletion monitor on the next lowest election GUID (if this is the leader, then listen to itself)
	private void resetZNodeDeletionMonitor() throws KeeperException, InterruptedException {
		zNodeDeletionMonitor = new ZooZNodeDeletionMonitor( getZooKeeper(), getZNodePathToWatch( null ), this );
	}

	// Watcher callback
	public void process(WatchedEvent event) {
		// znode monitor can be null if we haven't initialized it yet
		if ( null != zNodeDeletionMonitor ) {
			// Forward to znode monitor
			getZNodeDeletionMonitor().process( event );
		}
	}

	// Callback when monitored znode is deleted
	public void onZNodeDeleted() {
		try {
			determineAndPerformRole();
		}
		catch (Exception e) {
			// do nothing
		}
	}

	// Callback received when znode monitor dies
	public void onZooKeeperSessionClosed() {
		synchronized (this) {
			notifyAll();
		}
	}

	// Holds leader election, performs work based on results, and watches on a GUID
	private void determineAndPerformRole() throws KeeperException, InterruptedException {
		electAndCacheLeader();
		// Do work based on whether or not we are the leader
		performLeaderWork();
		// Set a deletion monitor if we are not the leader
		resetZNodeDeletionMonitor();
	}

	// Wait until monitor is dead
	public void run() throws KeeperException, IOException, InterruptedException {

		// Perform initial work based on whether we are the leader or not
		determineAndPerformRole();

		try {
			synchronized (this) {
				while (!getZNodeDeletionMonitor().getIsZooKeeperSessionClosed()) {
					wait();
				}
			}
		}
		catch (InterruptedException e) {}
	}

	// Override this function to determine what work should be done, if leader
	abstract void performLeaderWork();

}
package leaderElection;

/**
 * Other classes use the DataMonitor by implementing this method
 * @author Vivek
 *
 */
public interface ZooZNodeDeletionMonitorListener {

	/**
	 * The existence status of the node has changed.
	 */
	void onZNodeDeleted();

	/**
	 * The ZooKeeper session is no longer valid.
	 */
	void onZooKeeperSessionClosed();
}
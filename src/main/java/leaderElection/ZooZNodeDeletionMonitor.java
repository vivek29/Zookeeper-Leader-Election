package leaderElection;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * A monitor that watches for znode deletion and informs its listener when this occurs
 * 
 * See this for more: http://zookeeper.apache.org/doc/current/javaExample.html#ch_Introduction
 * @author Vivek
 *
 */
public class ZooZNodeDeletionMonitor implements Watcher, StatCallback {

	ZooKeeper hZooKeeper = null;

	// The ZNode that we're monitoring
	String zNodePath = null;

	// True if dead, false otherwise
	boolean isZooKeeperSessionClosed = false;

	// The listener object interested in changes to our monitored znode
	ZooZNodeDeletionMonitorListener listener = null;

	/**
	 * @return handle to ZooKeeper API
	 */
	private ZooKeeper getZooKeeper() {
		assert null != hZooKeeper;
		return hZooKeeper;
	}

	/**
	 * @return Path to zNode that we're monitoring for existence
	 */
	private final String getZNodePath() {
		return zNodePath;
	}

	/**
	 * @return True if dead, False otherwise
	 */
	public boolean getIsZooKeeperSessionClosed() {
		return isZooKeeperSessionClosed;
	}

	/**
	 * @return Listener to callback when data changes
	 */
	private ZooZNodeDeletionMonitorListener getListener() {
		assert null != listener;
		return listener;
	}

	/**
	 * Constructor
	 */
	public ZooZNodeDeletionMonitor(ZooKeeper hZooKeeper, String zNodePath, ZooZNodeDeletionMonitorListener listener) {
		this.hZooKeeper = hZooKeeper;
		this.zNodePath = zNodePath;
		this.listener = listener;
		checkZNodeExistsAsync();
	}

	/**
	 * Utility function to close monitor and inform listener on session close
	 */
	private void onZooKeeperSessionClosed() {
		isZooKeeperSessionClosed = true;
		getListener().onZooKeeperSessionClosed();
	}

	/**
	 * Utility function to check on znode existence
	 */
	private void checkZNodeExistsAsync() {
		getZooKeeper().exists( getZNodePath(), true, this, null) ;
	}

	/**
	 * Watcher callback
	 */
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
			// the state of the connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// Do nothing!
				// watches are automatically re-registered with 
				// server and any watches triggered while the client was 
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				onZooKeeperSessionClosed();
				break;
			default:
				break;
			}
		} else {
			if (path != null && path.equals(getZNodePath())) {
				// Something has changed on the node.
				checkZNodeExistsAsync();
			}
		}
	}

	/**
	 * AsyncCallback.StatCallback
	 */
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		if ( rc == Code.NONODE.intValue()) {
			getListener().onZNodeDeleted();
		}
		else if ( rc == Code.OK.intValue()) {
		}
		else if ( 
				( rc == Code.SESSIONEXPIRED.intValue())
				|| ( rc == Code.NOAUTH.intValue())
				) {
			onZooKeeperSessionClosed();
		}
		else {
			// Retry errors
			checkZNodeExistsAsync();
		}		
	}
}
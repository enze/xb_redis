<?php
/**
 * redis sentinel monitor
 * key分配算法采用hash一致性算法
 *
 * 2015.3.13 13:58
 * @author enze.wei <[enzewei@gmail.com]>
 */

class SentinelMonitor {

	private $_servers = array();
	private $_current = array();

	private $_stream = null;
	private $_socket = null;

	/*
	 * 根据key进行hash
	 */
	private $_key = null;

	public function __construct() {
	}

	/**
	 * retry 3次后依然没有成功连接sentinel的时候，直接剔除该sentinel
	 * 
	 * @return void
	 */
	private function _connect() {
		$i = 0;
		loop : {
			$this->_current = HashServer::hash($this->_key, $this->_servers);
			$this->_stream = RedisStream::instance($this->_current['retry']);
		}
		try {
			$this->_stream->create($this->_current['host'], $this->_current['port'], 'tcp');
		} catch (ResourceException $e) {
			$this->removeServer($this->_current);
			$serverTotal = count($this->_servers);
			if ($i < $serverTotal) {
				$i++;
				goto loop;
			}
			if (1 > $serverTotal) {
				throw new ResourceException('-9100001');
			}
		}
	}

	private function _getMaster() {
		$this->_stream->request(Command::getMasters());
		$masters = $this->_stream->response();
		$masterName = array();
		foreach ($masters as $key => $value) {
			$masterName[$key] = $value[1];
		}
		return HashServer::hash($this->_key, $masterName);
	}

	private function _getSlaveByMaster($masterName) {
		$this->_stream->request(Command::getSlavesByMaster($masterName));
		$slaves = $this->_stream->response();
		/*
		 * 如果只有master，没有slave
		 */
		if (true === empty($slaves)) {
			$masters = $this->_getHostByMaster($masterName);
			return join(':', $masters);
		}
		$slaveName = array();
		foreach ($slaves as $key => $value) {
			//for ($i = 0 ; $i < $value[27]; $i++) {
				$slaveName[] = $value[1];
			//}
		}
		return HashServer::hash($this->_key, $slaveName);
	}

	private function _getHostByMaster($masterName) {
		$this->_stream->request(Command::getMasterAddrByName($masterName));
		$master = $this->_stream->response();
		return $master;
	}

	public function addServer($host, $port, $auth = false, $retry = 0, $dbNum = 0) {
		$this->_servers[] = array('host' => $host, 'port' => $port, 'auth' => $auth, 'retry' => $retry, 'db' => $dbNum);
	}

	public function addServers($servers = array()) {
		if (true === is_array($servers)) {
			foreach ($servers as $val) {
				for ($i = 0; $i < $val[2]; $i++) {
					$this->_servers[] = array('host' => $val[0], 'port' => $val[1], 'auth' => $val[3], 'retry' => $val[4], 'db' => $val[5]);
				}
			}
		}
	}

	public function removeServer($server) {
		$keys = array_keys($this->_servers, $server);
		foreach ($keys as $value) {
			unset($this->_servers[$value]);
		}
	}

	public function getMasterServer($masterName = '') {
		try {
			if (true === empty($masterName)) {
				$this->_connect();
				$masterName = $this->_getMaster();
				$master = $this->_getHostByMaster($masterName);
				array_push($master, $this->_current['auth']);
				array_push($master, $this->_current['db']);
				return $master;
			}
		} catch (ResourceException $e) {
			throw new RedisException ($e->getMessage(), $e->getCode());
		}
	}

	public function getSlaveServer($masterName = '') {
		try {
			if (true === empty($masterName)) {
				$this->_connect();
				$masterName = $this->_getMaster();
				$slave = $this->_getSlaveByMaster($masterName);
				$slave = explode(':', $slave);
				array_push($slave, $this->_current['auth']);
				array_push($slave, $this->_current['db']);
				return $slave;
			}
		} catch (ResourceException $e) {
			throw new RedisException ($e->getMessage(), $e->getCode());
		}
	}

	public function __set($var, $val) {
		$var = '_' . $var;
		if (true === property_exists($this, $var)) {
			$this->$var = $val;
		}
	}

	public function __get($var) {
		$var = '_' . $var;
		if (true === property_exists($this, $var)) {
			return $this->$var;
		}
		return null;
	}
}